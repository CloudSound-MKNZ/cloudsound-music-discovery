"""Kafka consumer for processing concert and event updates.

Listens for events about new concerts or event descriptions
that may contain music links to discover and download.
"""
import asyncio
import json
from typing import Optional, Callable, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime
import threading
import structlog

from cloudsound_shared.kafka import KafkaConsumerClient
from cloudsound_shared.config.settings import app_settings

from ..services.link_extractor import LinkExtractor, ExtractedLink
from ..producers.rabbitmq_producer import DownloadTaskProducer

logger = structlog.get_logger(__name__)

# Topics to consume
CONCERT_CREATED_TOPIC = "concerts.created"
CONCERT_UPDATED_TOPIC = "concerts.updated"
EVENT_CREATED_TOPIC = "facebook.events.raw"


@dataclass
class ConcertEvent:
    """Concert event from Kafka."""
    concert_id: str
    name: str
    description: Optional[str]
    artists: List[str]
    venue: Optional[str]
    date: Optional[datetime]
    event_type: str  # "created" or "updated"
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], event_type: str) -> "ConcertEvent":
        """Create from dictionary."""
        return cls(
            concert_id=str(data.get("id", "")),
            name=data.get("name", ""),
            description=data.get("description"),
            artists=data.get("artists", []),
            venue=data.get("venue"),
            date=data.get("date"),
            event_type=event_type,
        )


class EventConsumer:
    """Kafka consumer for concert and event updates.
    
    Processes events to extract music links and queue them for download.
    
    Usage:
        consumer = EventConsumer(download_producer=producer)
        
        # Start consuming (blocking)
        consumer.start()
        
        # Or process single event
        await consumer.process_event(event_data)
    """
    
    def __init__(
        self,
        download_producer: Optional[DownloadTaskProducer] = None,
        link_extractor: Optional[LinkExtractor] = None,
        topics: Optional[List[str]] = None,
        group_id: str = "music-discovery",
    ):
        """Initialize event consumer.
        
        Args:
            download_producer: Producer for download tasks
            link_extractor: Service for extracting links
            topics: List of topics to consume
            group_id: Consumer group ID
        """
        self.download_producer = download_producer
        self.link_extractor = link_extractor or LinkExtractor()
        self.topics = topics or [
            CONCERT_CREATED_TOPIC,
            CONCERT_UPDATED_TOPIC,
        ]
        self.group_id = group_id
        
        self._consumer: Optional[KafkaConsumerClient] = None
        self._running = False
        self._consumer_thread: Optional[threading.Thread] = None
        
        logger.info(
            "event_consumer_initialized",
            topics=self.topics,
            group_id=group_id,
        )
    
    def _create_consumer(self) -> KafkaConsumerClient:
        """Create Kafka consumer client."""
        return KafkaConsumerClient(
            topics=self.topics,
            group_id=self.group_id,
            bootstrap_servers=app_settings.kafka_bootstrap_servers,
            auto_offset_reset="earliest",
        )
    
    def start(self) -> None:
        """Start consuming messages (blocking).
        
        This runs in the current thread and blocks until stopped.
        """
        logger.info("event_consumer_starting", topics=self.topics)
        
        self._consumer = self._create_consumer()
        self._running = True
        
        try:
            for message in self._consumer.consume():
                if not self._running:
                    break
                
                try:
                    self._handle_message(message)
                except Exception as e:
                    logger.error(
                        "event_consumer_message_error",
                        topic=message.topic,
                        error=str(e),
                    )
        finally:
            self._consumer.close()
            logger.info("event_consumer_stopped")
    
    def start_async(self) -> None:
        """Start consuming in a background thread."""
        if self._consumer_thread and self._consumer_thread.is_alive():
            logger.warning("event_consumer_already_running")
            return
        
        self._consumer_thread = threading.Thread(
            target=self.start,
            daemon=True,
            name="event-consumer",
        )
        self._consumer_thread.start()
        logger.info("event_consumer_started_async")
    
    def stop(self) -> None:
        """Stop consuming messages."""
        self._running = False
        
        if self._consumer_thread:
            self._consumer_thread.join(timeout=5)
        
        logger.info("event_consumer_stop_requested")
    
    def _handle_message(self, message: Any) -> None:
        """Handle a Kafka message.
        
        Args:
            message: Kafka message
        """
        topic = message.topic
        value = message.value
        
        logger.debug(
            "event_consumer_message_received",
            topic=topic,
            key=message.key,
        )
        
        # Determine event type from topic
        if topic == CONCERT_CREATED_TOPIC:
            event_type = "created"
        elif topic == CONCERT_UPDATED_TOPIC:
            event_type = "updated"
        else:
            event_type = "unknown"
        
        # Process based on topic
        if topic in [CONCERT_CREATED_TOPIC, CONCERT_UPDATED_TOPIC]:
            self._process_concert_event(value, event_type)
        elif topic == EVENT_CREATED_TOPIC:
            self._process_facebook_event(value)
    
    def _process_concert_event(self, data: Dict[str, Any], event_type: str) -> None:
        """Process a concert event.
        
        Args:
            data: Concert event data
            event_type: Type of event (created/updated)
        """
        try:
            event = ConcertEvent.from_dict(data, event_type)
            
            logger.info(
                "event_consumer_concert_event",
                concert_id=event.concert_id,
                name=event.name,
                event_type=event_type,
            )
            
            # Extract links from description
            links = []
            if event.description:
                links = self.link_extractor.extract_downloadable_links(
                    event.description
                )
            
            if links:
                logger.info(
                    "event_consumer_links_found",
                    concert_id=event.concert_id,
                    link_count=len(links),
                )
                
                # Queue downloads
                self._queue_downloads(links, {
                    "source": "concert",
                    "concert_id": event.concert_id,
                    "concert_name": event.name,
                })
            else:
                logger.debug(
                    "event_consumer_no_links",
                    concert_id=event.concert_id,
                )
                
        except Exception as e:
            logger.error(
                "event_consumer_concert_error",
                error=str(e),
            )
    
    def _process_facebook_event(self, data: Dict[str, Any]) -> None:
        """Process a Facebook event.
        
        Args:
            data: Facebook event data
        """
        try:
            event_id = data.get("id", "")
            description = data.get("description", "")
            name = data.get("name", "")
            
            logger.info(
                "event_consumer_facebook_event",
                event_id=event_id,
                name=name,
            )
            
            # Extract links from description
            links = []
            if description:
                links = self.link_extractor.extract_downloadable_links(description)
            
            if links:
                logger.info(
                    "event_consumer_links_found",
                    event_id=event_id,
                    link_count=len(links),
                )
                
                # Queue downloads
                self._queue_downloads(links, {
                    "source": "facebook",
                    "event_id": event_id,
                    "event_name": name,
                })
                
        except Exception as e:
            logger.error(
                "event_consumer_facebook_error",
                error=str(e),
            )
    
    def _queue_downloads(
        self,
        links: List[ExtractedLink],
        context: Dict[str, Any],
    ) -> None:
        """Queue links for download.
        
        Args:
            links: Links to download
            context: Additional context for the download
        """
        if not self.download_producer:
            logger.warning("event_consumer_no_producer")
            return
        
        for link in links:
            try:
                self.download_producer.queue_download(
                    url=link.url,
                    link_type=link.link_type.value,
                    video_id=link.video_id,
                    artist=link.artist,
                    slug=link.slug,
                    content_type=link.content_type,
                    context=context,
                )
            except Exception as e:
                logger.error(
                    "event_consumer_queue_error",
                    url=link.url,
                    error=str(e),
                )
    
    async def process_event_async(self, data: Dict[str, Any], topic: str) -> List[ExtractedLink]:
        """Process an event asynchronously (for testing/direct calls).
        
        Args:
            data: Event data
            topic: Topic name
            
        Returns:
            List of extracted links
        """
        # Determine event type
        if topic == CONCERT_CREATED_TOPIC:
            event_type = "created"
        elif topic == CONCERT_UPDATED_TOPIC:
            event_type = "updated"
        else:
            event_type = "unknown"
        
        # Extract text to search
        text = ""
        if topic in [CONCERT_CREATED_TOPIC, CONCERT_UPDATED_TOPIC]:
            event = ConcertEvent.from_dict(data, event_type)
            text = event.description or ""
        elif topic == EVENT_CREATED_TOPIC:
            text = data.get("description", "")
        
        # Extract links
        return self.link_extractor.extract_downloadable_links(text)

