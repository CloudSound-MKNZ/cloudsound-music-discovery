"""Music discovery service main application.

Provides automatic music discovery from YouTube and Bandcamp based on
concert descriptions and event information.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import structlog

from cloudsound_shared.health import router as health_router
from cloudsound_shared.metrics import get_metrics
from cloudsound_shared.middleware.error_handler import (
    http_exception_handler,
    validation_exception_handler,
    general_exception_handler,
)
from cloudsound_shared.middleware.correlation import CorrelationIDMiddleware
from cloudsound_shared.logging import configure_logging, get_logger
from cloudsound_shared.config.settings import app_settings

from .metrics import init_metrics
from .clients.youtube_client import YouTubeClient
from .clients.bandcamp_client import BandcampClient
from .services.link_extractor import LinkExtractor, ExtractedLink, LinkType
from .services.storage_service import StorageService
from .services.downloader import MusicDownloader
from .producers.rabbitmq_producer import DownloadTaskProducer
from .producers.kafka_producer import MusicEventProducer
from .consumers.kafka_consumer import EventConsumer
from .consumers.rabbitmq_consumer import DownloadWorker

# Configure logging
configure_logging(log_level=app_settings.log_level, log_format=app_settings.log_format)
logger = get_logger(__name__)

# Global service instances
youtube_client: Optional[YouTubeClient] = None
bandcamp_client: Optional[BandcampClient] = None
link_extractor: Optional[LinkExtractor] = None
storage_service: Optional[StorageService] = None
downloader: Optional[MusicDownloader] = None
download_producer: Optional[DownloadTaskProducer] = None
event_producer: Optional[MusicEventProducer] = None
event_consumer: Optional[EventConsumer] = None
download_worker: Optional[DownloadWorker] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global youtube_client, bandcamp_client, link_extractor, storage_service
    global downloader, download_producer, event_producer, event_consumer, download_worker
    
    # Startup
    logger.info("music_discovery_service_starting", version=app_settings.app_version)
    
    # Initialize metrics
    init_metrics(app_settings.app_version)
    
    # Initialize clients
    youtube_client = YouTubeClient(use_mock=app_settings.use_mock_apis)
    bandcamp_client = BandcampClient(use_mock=app_settings.use_mock_apis)
    link_extractor = LinkExtractor(
        youtube_client=youtube_client,
        bandcamp_client=bandcamp_client,
    )
    
    # Initialize storage
    storage_service = StorageService(quota_limit_gb=10.0)
    try:
        await storage_service.initialize()
    except Exception as e:
        logger.warning("storage_service_init_failed", error=str(e))
    
    # Initialize downloader
    downloader = MusicDownloader(
        storage_service=storage_service,
        youtube_client=youtube_client,
        bandcamp_client=bandcamp_client,
    )
    
    # Initialize producers
    try:
        download_producer = DownloadTaskProducer()
        download_producer.connect()
    except Exception as e:
        logger.warning("rabbitmq_producer_init_failed", error=str(e))
        download_producer = None
    
    try:
        event_producer = MusicEventProducer()
        event_producer.connect()
    except Exception as e:
        logger.warning("kafka_producer_init_failed", error=str(e))
        event_producer = None
    
    # Initialize and start consumers (in background)
    try:
        event_consumer = EventConsumer(
            download_producer=download_producer,
            link_extractor=link_extractor,
        )
        event_consumer.start_async()
    except Exception as e:
        logger.warning("kafka_consumer_init_failed", error=str(e))
        event_consumer = None
    
    try:
        download_worker = DownloadWorker(
            downloader=downloader,
            event_producer=event_producer,
        )
        download_worker.start_async()
    except Exception as e:
        logger.warning("rabbitmq_worker_init_failed", error=str(e))
        download_worker = None
    
    logger.info("music_discovery_service_started", version=app_settings.app_version)
    
    yield
    
    # Shutdown
    logger.info("music_discovery_service_shutting_down")
    
    # Stop consumers
    if event_consumer:
        event_consumer.stop()
    if download_worker:
        download_worker.stop()
    
    # Close producers
    if download_producer:
        download_producer.close()
    if event_producer:
        event_producer.close()
    
    # Close clients
    if youtube_client:
        await youtube_client.close()
    if bandcamp_client:
        await bandcamp_client.close()
    
    logger.info("music_discovery_service_shutdown")


# Create FastAPI app
app = FastAPI(
    title="CloudSound Music Discovery Service",
    version=app_settings.app_version,
    description="Automatic music discovery from YouTube and Bandcamp",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Correlation ID middleware
app.add_middleware(CorrelationIDMiddleware)

# Exception handlers
app.add_exception_handler(StarletteHTTPException, http_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(Exception, general_exception_handler)

# Include health router
app.include_router(health_router)


# Request/Response models
class ExtractLinksRequest(BaseModel):
    """Request to extract links from text."""
    text: str
    include_youtube: bool = True
    include_bandcamp: bool = True


class ExtractLinksResponse(BaseModel):
    """Response with extracted links."""
    links: List[Dict[str, Any]]
    count: int


class QueueDownloadRequest(BaseModel):
    """Request to queue a download."""
    url: str
    context: Optional[Dict[str, Any]] = None


class QueueDownloadResponse(BaseModel):
    """Response with queued task info."""
    task_id: str
    url: str
    link_type: str


class StorageStatsResponse(BaseModel):
    """Storage statistics response."""
    quota_limit_gb: float
    used_gb: float
    available_gb: float
    usage_percent: float
    total_files: int


# API endpoints
@app.get("/metrics")
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(content=get_metrics(), media_type="text/plain")


@app.post(
    f"{app_settings.api_prefix}/discover/extract-links",
    response_model=ExtractLinksResponse,
)
async def extract_links(request: ExtractLinksRequest) -> ExtractLinksResponse:
    """Extract music links from text.
    
    Finds YouTube and Bandcamp URLs in the provided text.
    """
    include_types = set()
    if request.include_youtube:
        include_types.add(LinkType.YOUTUBE)
    if request.include_bandcamp:
        include_types.add(LinkType.BANDCAMP)
    
    links = link_extractor.extract_links(request.text, include_types)
    
    return ExtractLinksResponse(
        links=[
            {
                "url": link.url,
                "link_type": link.link_type.value,
                "video_id": link.video_id,
                "artist": link.artist,
                "slug": link.slug,
                "content_type": link.content_type,
            }
            for link in links
        ],
        count=len(links),
    )


@app.post(
    f"{app_settings.api_prefix}/discover/queue-download",
    response_model=QueueDownloadResponse,
)
async def queue_download(request: QueueDownloadRequest) -> QueueDownloadResponse:
    """Queue a URL for download.
    
    The download will be processed asynchronously by the download worker.
    """
    if not download_producer:
        raise StarletteHTTPException(
            status_code=503,
            detail="Download queue not available",
        )
    
    # Determine link type
    link_type = LinkExtractor.get_link_type(request.url)
    
    # Extract additional info
    video_id = None
    artist = None
    slug = None
    content_type = None
    
    if link_type == LinkType.YOUTUBE:
        video_id = YouTubeClient.extract_video_id(request.url)
    elif link_type == LinkType.BANDCAMP:
        parsed = BandcampClient.parse_url(request.url)
        if parsed:
            artist = parsed.get("artist")
            slug = parsed.get("slug")
            content_type = parsed.get("type")
    
    # Queue the download
    task_id = download_producer.queue_download(
        url=request.url,
        link_type=link_type.value,
        video_id=video_id,
        artist=artist,
        slug=slug,
        content_type=content_type,
        context=request.context,
    )
    
    return QueueDownloadResponse(
        task_id=task_id,
        url=request.url,
        link_type=link_type.value,
    )


@app.get(
    f"{app_settings.api_prefix}/discover/storage/stats",
    response_model=StorageStatsResponse,
)
async def get_storage_stats() -> StorageStatsResponse:
    """Get storage usage statistics."""
    if not storage_service:
        raise StarletteHTTPException(
            status_code=503,
            detail="Storage service not available",
        )
    
    stats = await storage_service.get_stats()
    
    return StorageStatsResponse(**stats)


@app.get(f"{app_settings.api_prefix}/discover/queue/stats")
async def get_queue_stats() -> Dict[str, Any]:
    """Get download queue statistics."""
    if not download_producer:
        return {"error": "Queue not available"}
    
    return download_producer.get_queue_stats()


@app.get(f"{app_settings.api_prefix}/discover/clients/status")
async def get_clients_status() -> Dict[str, Any]:
    """Get status of external API clients."""
    status = {}
    
    if youtube_client:
        status["youtube"] = youtube_client.get_circuit_breaker_stats()
    
    if bandcamp_client:
        status["bandcamp"] = bandcamp_client.get_circuit_breaker_stats()
    
    return status


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)

