"""RabbitMQ consumer for processing download tasks.

Consumes download tasks from the queue and processes them
using the music downloader service.
"""
import asyncio
import json
from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import threading
import structlog

from cloudsound_shared.rabbitmq import RabbitMQClient
from cloudsound_shared.config.settings import app_settings

from ..services.downloader import MusicDownloader, DownloadResult, DownloadStatus
from ..services.link_extractor import ExtractedLink, LinkType
from ..producers.kafka_producer import MusicEventProducer

logger = structlog.get_logger(__name__)

# Queue name
DOWNLOAD_QUEUE = "music.downloads"


@dataclass
class DownloadTask:
    """Download task from queue."""
    task_id: str
    url: str
    link_type: str
    video_id: Optional[str] = None
    artist: Optional[str] = None
    slug: Optional[str] = None
    content_type: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DownloadTask":
        """Create from dictionary."""
        return cls(
            task_id=data.get("task_id", ""),
            url=data.get("url", ""),
            link_type=data.get("link_type", "unknown"),
            video_id=data.get("video_id"),
            artist=data.get("artist"),
            slug=data.get("slug"),
            content_type=data.get("content_type"),
            context=data.get("context"),
            created_at=data.get("created_at"),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
        )
    
    def to_extracted_link(self) -> ExtractedLink:
        """Convert to ExtractedLink."""
        try:
            link_type = LinkType(self.link_type)
        except ValueError:
            link_type = LinkType.UNKNOWN
        
        return ExtractedLink(
            url=self.url,
            link_type=link_type,
            video_id=self.video_id,
            artist=self.artist,
            slug=self.slug,
            content_type=self.content_type,
        )


class DownloadWorker:
    """RabbitMQ worker for processing download tasks.
    
    Consumes download tasks from the queue and processes them
    using the music downloader. Publishes results to Kafka.
    
    Usage:
        worker = DownloadWorker(
            downloader=music_downloader,
            event_producer=kafka_producer,
        )
        
        # Start processing (blocking)
        worker.start()
    """
    
    def __init__(
        self,
        downloader: Optional[MusicDownloader] = None,
        event_producer: Optional[MusicEventProducer] = None,
        queue_name: str = DOWNLOAD_QUEUE,
        prefetch_count: int = 1,
    ):
        """Initialize download worker.
        
        Args:
            downloader: Music downloader service
            event_producer: Kafka producer for events
            queue_name: RabbitMQ queue name
            prefetch_count: Number of messages to prefetch
        """
        self.downloader = downloader or MusicDownloader()
        self.event_producer = event_producer
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        
        self._client: Optional[RabbitMQClient] = None
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        
        logger.info(
            "download_worker_initialized",
            queue_name=queue_name,
            prefetch_count=prefetch_count,
        )
    
    def _create_client(self) -> RabbitMQClient:
        """Create RabbitMQ client."""
        return RabbitMQClient(
            host=app_settings.rabbitmq_host,
            port=app_settings.rabbitmq_port,
            user=app_settings.rabbitmq_user,
            password=app_settings.rabbitmq_password,
            vhost=app_settings.rabbitmq_vhost,
        )
    
    def start(self) -> None:
        """Start processing download tasks (blocking).
        
        This runs in the current thread and blocks until stopped.
        """
        logger.info("download_worker_starting", queue_name=self.queue_name)
        
        self._client = self._create_client()
        self._client.connect()
        self._client.declare_queue(self.queue_name)
        
        # Set prefetch
        self._client.channel.basic_qos(prefetch_count=self.prefetch_count)
        
        # Create event loop for async operations
        self._event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._event_loop)
        
        self._running = True
        
        try:
            self._client.consume(
                self.queue_name,
                callback=self._handle_message,
                auto_ack=False,
            )
        except Exception as e:
            logger.error("download_worker_error", error=str(e))
        finally:
            if self._event_loop:
                self._event_loop.close()
            self._client.close()
            logger.info("download_worker_stopped")
    
    def start_async(self) -> None:
        """Start processing in a background thread."""
        if self._worker_thread and self._worker_thread.is_alive():
            logger.warning("download_worker_already_running")
            return
        
        self._worker_thread = threading.Thread(
            target=self.start,
            daemon=True,
            name="download-worker",
        )
        self._worker_thread.start()
        logger.info("download_worker_started_async")
    
    def stop(self) -> None:
        """Stop processing tasks."""
        self._running = False
        
        if self._client and self._client.connection:
            # Cancel consuming
            if self._client.channel:
                self._client.channel.stop_consuming()
        
        if self._worker_thread:
            self._worker_thread.join(timeout=10)
        
        logger.info("download_worker_stop_requested")
    
    def _handle_message(self, message: Dict[str, Any]) -> None:
        """Handle a download task message.
        
        Args:
            message: Task message from queue
        """
        try:
            task = DownloadTask.from_dict(message)
            
            logger.info(
                "download_worker_task_received",
                task_id=task.task_id,
                url=task.url,
                link_type=task.link_type,
            )
            
            # Process download in event loop
            result = self._event_loop.run_until_complete(
                self._process_download(task)
            )
            
            # Publish result
            if self.event_producer:
                self._publish_result(task, result)
            
            logger.info(
                "download_worker_task_completed",
                task_id=task.task_id,
                status=result.status.value,
            )
            
        except Exception as e:
            logger.error(
                "download_worker_task_error",
                error=str(e),
            )
    
    async def _process_download(self, task: DownloadTask) -> DownloadResult:
        """Process a download task.
        
        Args:
            task: Download task to process
            
        Returns:
            Download result
        """
        link = task.to_extracted_link()
        
        result = await self.downloader.download(link)
        
        # Handle retries
        if result.status == DownloadStatus.FAILED:
            if task.retry_count < task.max_retries:
                logger.warning(
                    "download_worker_will_retry",
                    task_id=task.task_id,
                    retry_count=task.retry_count + 1,
                    max_retries=task.max_retries,
                )
                # In a real implementation, we would requeue with incremented retry_count
        
        return result
    
    def _publish_result(self, task: DownloadTask, result: DownloadResult) -> None:
        """Publish download result to Kafka.
        
        Args:
            task: Original task
            result: Download result
        """
        if not self.event_producer:
            return
        
        try:
            if result.status == DownloadStatus.COMPLETED:
                self.event_producer.publish_download_completed(
                    url=result.url,
                    file_path=result.file_path or "",
                    title=result.title,
                    artist=result.artist,
                    duration=result.duration,
                    file_size=result.file_size,
                    context=task.context,
                )
            else:
                self.event_producer.publish_download_failed(
                    url=result.url,
                    error=result.error or "Unknown error",
                    status=result.status.value,
                    context=task.context,
                )
        except Exception as e:
            logger.error(
                "download_worker_publish_error",
                task_id=task.task_id,
                error=str(e),
            )
    
    async def process_task_async(self, task: DownloadTask) -> DownloadResult:
        """Process a task asynchronously (for testing/direct calls).
        
        Args:
            task: Download task
            
        Returns:
            Download result
        """
        return await self._process_download(task)

