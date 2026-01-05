"""Prometheus metrics for music discovery service.

Tracks downloads, API calls, queue depths, and other key metrics.
"""
from prometheus_client import Counter, Histogram, Gauge, Info
import structlog

logger = structlog.get_logger(__name__)

# Service info
SERVICE_INFO = Info(
    "music_discovery_service",
    "Music discovery service information",
)

# Download metrics
DOWNLOADS_TOTAL = Counter(
    "music_discovery_downloads_total",
    "Total number of download attempts",
    ["source", "status"],
)

DOWNLOAD_DURATION = Histogram(
    "music_discovery_download_duration_seconds",
    "Time spent downloading music",
    ["source"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

DOWNLOAD_SIZE = Histogram(
    "music_discovery_download_size_bytes",
    "Size of downloaded files",
    ["source"],
    buckets=[1024 * 1024 * i for i in [1, 5, 10, 20, 50, 100]],  # 1MB to 100MB
)

# Link extraction metrics
LINKS_EXTRACTED = Counter(
    "music_discovery_links_extracted_total",
    "Total number of links extracted",
    ["source_type", "link_type"],
)

# API client metrics
API_REQUESTS = Counter(
    "music_discovery_api_requests_total",
    "Total API requests to external services",
    ["service", "endpoint", "status"],
)

API_LATENCY = Histogram(
    "music_discovery_api_latency_seconds",
    "Latency of external API calls",
    ["service", "endpoint"],
    buckets=[0.1, 0.25, 0.5, 1, 2.5, 5, 10],
)

# Circuit breaker metrics
CIRCUIT_BREAKER_STATE = Gauge(
    "music_discovery_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=half-open, 2=open)",
    ["name"],
)

CIRCUIT_BREAKER_FAILURES = Counter(
    "music_discovery_circuit_breaker_failures_total",
    "Total circuit breaker failures",
    ["name"],
)

# Queue metrics
QUEUE_DEPTH = Gauge(
    "music_discovery_queue_depth",
    "Current depth of download queue",
    ["queue_name"],
)

QUEUE_MESSAGES_PROCESSED = Counter(
    "music_discovery_queue_messages_processed_total",
    "Total messages processed from queue",
    ["queue_name", "status"],
)

# Storage metrics
STORAGE_USED_BYTES = Gauge(
    "music_discovery_storage_used_bytes",
    "Storage space used in bytes",
)

STORAGE_QUOTA_BYTES = Gauge(
    "music_discovery_storage_quota_bytes",
    "Storage quota limit in bytes",
)

STORAGE_USAGE_PERCENT = Gauge(
    "music_discovery_storage_usage_percent",
    "Storage usage percentage",
)

STORAGE_OPERATIONS = Counter(
    "music_discovery_storage_operations_total",
    "Total storage operations",
    ["operation", "status"],
)

# Kafka metrics
KAFKA_MESSAGES_CONSUMED = Counter(
    "music_discovery_kafka_messages_consumed_total",
    "Total Kafka messages consumed",
    ["topic"],
)

KAFKA_MESSAGES_PRODUCED = Counter(
    "music_discovery_kafka_messages_produced_total",
    "Total Kafka messages produced",
    ["topic"],
)


def init_metrics(version: str = "1.0.0") -> None:
    """Initialize service metrics.
    
    Args:
        version: Service version string
    """
    SERVICE_INFO.info({
        "version": version,
        "service": "music-discovery",
    })
    logger.info("metrics_initialized", version=version)


def record_download(
    source: str,
    status: str,
    duration_seconds: float = 0,
    size_bytes: int = 0,
) -> None:
    """Record a download attempt.
    
    Args:
        source: Source platform (youtube, bandcamp)
        status: Download status (completed, failed, quota_exceeded)
        duration_seconds: Time spent downloading
        size_bytes: Size of downloaded file
    """
    DOWNLOADS_TOTAL.labels(source=source, status=status).inc()
    
    if duration_seconds > 0:
        DOWNLOAD_DURATION.labels(source=source).observe(duration_seconds)
    
    if size_bytes > 0:
        DOWNLOAD_SIZE.labels(source=source).observe(size_bytes)


def record_links_extracted(
    source_type: str,
    link_type: str,
    count: int = 1,
) -> None:
    """Record extracted links.
    
    Args:
        source_type: Source of the text (concert, facebook_event)
        link_type: Type of link (youtube, bandcamp)
        count: Number of links extracted
    """
    LINKS_EXTRACTED.labels(
        source_type=source_type,
        link_type=link_type,
    ).inc(count)


def record_api_request(
    service: str,
    endpoint: str,
    status: str,
    latency_seconds: float = 0,
) -> None:
    """Record an external API request.
    
    Args:
        service: Service name (youtube, bandcamp)
        endpoint: API endpoint called
        status: Response status (success, error, timeout)
        latency_seconds: Request latency
    """
    API_REQUESTS.labels(
        service=service,
        endpoint=endpoint,
        status=status,
    ).inc()
    
    if latency_seconds > 0:
        API_LATENCY.labels(
            service=service,
            endpoint=endpoint,
        ).observe(latency_seconds)


def update_circuit_breaker(name: str, state: str, failure: bool = False) -> None:
    """Update circuit breaker metrics.
    
    Args:
        name: Circuit breaker name
        state: Current state (closed, half_open, open)
        failure: Whether a failure occurred
    """
    state_values = {"closed": 0, "half_open": 1, "open": 2}
    CIRCUIT_BREAKER_STATE.labels(name=name).set(state_values.get(state, 0))
    
    if failure:
        CIRCUIT_BREAKER_FAILURES.labels(name=name).inc()


def update_queue_depth(queue_name: str, depth: int) -> None:
    """Update queue depth gauge.
    
    Args:
        queue_name: Name of the queue
        depth: Current queue depth
    """
    QUEUE_DEPTH.labels(queue_name=queue_name).set(depth)


def record_queue_message(queue_name: str, status: str) -> None:
    """Record a processed queue message.
    
    Args:
        queue_name: Name of the queue
        status: Processing status (success, failed)
    """
    QUEUE_MESSAGES_PROCESSED.labels(
        queue_name=queue_name,
        status=status,
    ).inc()


def update_storage_metrics(
    used_bytes: int,
    quota_bytes: int,
    usage_percent: float,
) -> None:
    """Update storage metrics.
    
    Args:
        used_bytes: Storage space used
        quota_bytes: Storage quota limit
        usage_percent: Usage percentage
    """
    STORAGE_USED_BYTES.set(used_bytes)
    STORAGE_QUOTA_BYTES.set(quota_bytes)
    STORAGE_USAGE_PERCENT.set(usage_percent)


def record_storage_operation(operation: str, status: str) -> None:
    """Record a storage operation.
    
    Args:
        operation: Operation type (upload, download, delete)
        status: Operation status (success, failed)
    """
    STORAGE_OPERATIONS.labels(
        operation=operation,
        status=status,
    ).inc()


def record_kafka_consumed(topic: str) -> None:
    """Record a consumed Kafka message.
    
    Args:
        topic: Topic name
    """
    KAFKA_MESSAGES_CONSUMED.labels(topic=topic).inc()


def record_kafka_produced(topic: str) -> None:
    """Record a produced Kafka message.
    
    Args:
        topic: Topic name
    """
    KAFKA_MESSAGES_PRODUCED.labels(topic=topic).inc()

