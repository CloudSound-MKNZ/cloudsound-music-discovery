"""Message consumers for music discovery."""
from .kafka_consumer import EventConsumer
from .rabbitmq_consumer import DownloadWorker

__all__ = ["EventConsumer", "DownloadWorker"]

