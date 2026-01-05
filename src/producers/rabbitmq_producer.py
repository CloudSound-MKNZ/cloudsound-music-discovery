"""RabbitMQ producer for download tasks.

Queues download tasks for processing by the download worker.
"""
import json
import uuid
from typing import Optional, Dict, Any
from datetime import datetime
import structlog

from cloudsound_shared.rabbitmq import RabbitMQClient
from cloudsound_shared.config.settings import app_settings

logger = structlog.get_logger(__name__)

# Queue name
DOWNLOAD_QUEUE = "music.downloads"


class DownloadTaskProducer:
    """Producer for queuing download tasks.
    
    Publishes download tasks to RabbitMQ for processing by the download worker.
    
    Usage:
        producer = DownloadTaskProducer()
        producer.connect()
        
        producer.queue_download(
            url="https://youtube.com/watch?v=abc123",
            link_type="youtube",
            video_id="abc123",
        )
    """
    
    def __init__(
        self,
        queue_name: str = DOWNLOAD_QUEUE,
        client: Optional[RabbitMQClient] = None,
    ):
        """Initialize download task producer.
        
        Args:
            queue_name: Name of the download queue
            client: Optional pre-configured RabbitMQ client
        """
        self.queue_name = queue_name
        self._client = client
        self._connected = False
        
        logger.info("download_task_producer_initialized", queue_name=queue_name)
    
    def connect(self) -> None:
        """Connect to RabbitMQ and declare queue."""
        if self._connected:
            return
        
        if not self._client:
            self._client = RabbitMQClient(
                host=app_settings.rabbitmq_host,
                port=app_settings.rabbitmq_port,
                user=app_settings.rabbitmq_user,
                password=app_settings.rabbitmq_password,
                vhost=app_settings.rabbitmq_vhost,
            )
        
        self._client.connect()
        self._client.declare_queue(self.queue_name)
        self._connected = True
        
        logger.info("download_task_producer_connected")
    
    def close(self) -> None:
        """Close connection."""
        if self._client:
            self._client.close()
            self._connected = False
            logger.info("download_task_producer_closed")
    
    def queue_download(
        self,
        url: str,
        link_type: str,
        video_id: Optional[str] = None,
        artist: Optional[str] = None,
        slug: Optional[str] = None,
        content_type: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        priority: int = 0,
        max_retries: int = 3,
    ) -> str:
        """Queue a download task.
        
        Args:
            url: URL to download
            link_type: Type of link (youtube, bandcamp, etc.)
            video_id: YouTube video ID (if applicable)
            artist: Artist name/subdomain (if applicable)
            slug: Track/album slug (if applicable)
            content_type: Content type (track/album)
            context: Additional context (source, concert_id, etc.)
            priority: Task priority (higher = more important)
            max_retries: Maximum retry attempts
            
        Returns:
            Task ID
        """
        if not self._connected:
            self.connect()
        
        task_id = str(uuid.uuid4())
        
        message = {
            "task_id": task_id,
            "url": url,
            "link_type": link_type,
            "video_id": video_id,
            "artist": artist,
            "slug": slug,
            "content_type": content_type,
            "context": context or {},
            "priority": priority,
            "max_retries": max_retries,
            "retry_count": 0,
            "created_at": datetime.utcnow().isoformat(),
        }
        
        self._client.publish(self.queue_name, message, persistent=True)
        
        logger.info(
            "download_task_queued",
            task_id=task_id,
            url=url,
            link_type=link_type,
        )
        
        return task_id
    
    def queue_batch(
        self,
        tasks: list[Dict[str, Any]],
    ) -> list[str]:
        """Queue multiple download tasks.
        
        Args:
            tasks: List of task dicts with url, link_type, etc.
            
        Returns:
            List of task IDs
        """
        task_ids = []
        
        for task in tasks:
            task_id = self.queue_download(
                url=task.get("url", ""),
                link_type=task.get("link_type", "unknown"),
                video_id=task.get("video_id"),
                artist=task.get("artist"),
                slug=task.get("slug"),
                content_type=task.get("content_type"),
                context=task.get("context"),
                priority=task.get("priority", 0),
                max_retries=task.get("max_retries", 3),
            )
            task_ids.append(task_id)
        
        logger.info("download_tasks_batch_queued", count=len(task_ids))
        
        return task_ids
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics.
        
        Returns:
            Dict with queue stats
        """
        if not self._connected:
            self.connect()
        
        try:
            # Get queue info
            method_frame = self._client.channel.queue_declare(
                queue=self.queue_name,
                durable=True,
                passive=True,  # Just check, don't create
            )
            
            return {
                "queue_name": self.queue_name,
                "message_count": method_frame.method.message_count,
                "consumer_count": method_frame.method.consumer_count,
            }
        except Exception as e:
            logger.error("download_task_producer_stats_error", error=str(e))
            return {
                "queue_name": self.queue_name,
                "error": str(e),
            }

