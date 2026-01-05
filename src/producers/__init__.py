"""Message producers for music discovery."""
from .rabbitmq_producer import DownloadTaskProducer
from .kafka_producer import MusicEventProducer

__all__ = ["DownloadTaskProducer", "MusicEventProducer"]

