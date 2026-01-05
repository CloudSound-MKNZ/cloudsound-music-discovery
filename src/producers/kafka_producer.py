"""Kafka producer for music discovery events.

Publishes events about downloaded music, failures, and other
music discovery activities.
"""
import json
from typing import Optional, Dict, Any
from datetime import datetime
import structlog

from cloudsound_shared.kafka import KafkaProducerClient
from cloudsound_shared.config.settings import app_settings

logger = structlog.get_logger(__name__)

# Topics
MUSIC_DOWNLOADED_TOPIC = "music.downloaded"
MUSIC_DOWNLOAD_FAILED_TOPIC = "music.download.failed"
MUSIC_DISCOVERED_TOPIC = "music.discovered"


class MusicEventProducer:
    """Kafka producer for music discovery events.
    
    Publishes events when music is discovered, downloaded, or fails.
    
    Usage:
        producer = MusicEventProducer()
        producer.connect()
        
        producer.publish_download_completed(
            url="https://youtube.com/watch?v=abc123",
            file_path="tracks/artist/song.mp3",
            title="Song Name",
            artist="Artist Name",
        )
    """
    
    def __init__(
        self,
        client: Optional[KafkaProducerClient] = None,
    ):
        """Initialize music event producer.
        
        Args:
            client: Optional pre-configured Kafka client
        """
        self._client = client
        self._connected = False
        
        logger.info("music_event_producer_initialized")
    
    def connect(self) -> None:
        """Connect to Kafka."""
        if self._connected:
            return
        
        if not self._client:
            self._client = KafkaProducerClient(
                bootstrap_servers=app_settings.kafka_bootstrap_servers,
            )
        
        self._client.connect()
        self._connected = True
        
        logger.info("music_event_producer_connected")
    
    def close(self) -> None:
        """Close connection."""
        if self._client:
            self._client.close()
            self._connected = False
            logger.info("music_event_producer_closed")
    
    def publish_download_completed(
        self,
        url: str,
        file_path: str,
        title: Optional[str] = None,
        artist: Optional[str] = None,
        album: Optional[str] = None,
        duration: int = 0,
        file_size: int = 0,
        source: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Publish music downloaded event.
        
        Args:
            url: Original source URL
            file_path: Path in storage
            title: Track title
            artist: Artist name
            album: Album name (if applicable)
            duration: Duration in seconds
            file_size: File size in bytes
            source: Source platform (youtube, bandcamp)
            context: Additional context (concert_id, etc.)
        """
        if not self._connected:
            self.connect()
        
        event = {
            "event_type": "music.downloaded",
            "url": url,
            "file_path": file_path,
            "title": title,
            "artist": artist,
            "album": album,
            "duration": duration,
            "file_size": file_size,
            "source": source,
            "context": context or {},
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        self._client.send(
            MUSIC_DOWNLOADED_TOPIC,
            value=event,
            key=url,
        )
        
        logger.info(
            "music_downloaded_event_published",
            url=url,
            file_path=file_path,
            title=title,
        )
    
    def publish_download_failed(
        self,
        url: str,
        error: str,
        status: str = "failed",
        retry_count: int = 0,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Publish music download failed event.
        
        Args:
            url: Source URL that failed
            error: Error message
            status: Status (failed, quota_exceeded, etc.)
            retry_count: Number of retries attempted
            context: Additional context
        """
        if not self._connected:
            self.connect()
        
        event = {
            "event_type": "music.download.failed",
            "url": url,
            "error": error,
            "status": status,
            "retry_count": retry_count,
            "context": context or {},
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        self._client.send(
            MUSIC_DOWNLOAD_FAILED_TOPIC,
            value=event,
            key=url,
        )
        
        logger.warning(
            "music_download_failed_event_published",
            url=url,
            error=error,
            status=status,
        )
    
    def publish_music_discovered(
        self,
        urls: list[str],
        source: str,
        source_id: str,
        source_name: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Publish music discovered event.
        
        Args:
            urls: List of discovered URLs
            source: Source type (concert, facebook_event)
            source_id: ID of the source entity
            source_name: Name of the source
            context: Additional context
        """
        if not self._connected:
            self.connect()
        
        event = {
            "event_type": "music.discovered",
            "urls": urls,
            "url_count": len(urls),
            "source": source,
            "source_id": source_id,
            "source_name": source_name,
            "context": context or {},
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        self._client.send(
            MUSIC_DISCOVERED_TOPIC,
            value=event,
            key=source_id,
        )
        
        logger.info(
            "music_discovered_event_published",
            source=source,
            source_id=source_id,
            url_count=len(urls),
        )
    
    def flush(self) -> None:
        """Flush pending messages."""
        if self._client and self._client.producer:
            self._client.producer.flush()

