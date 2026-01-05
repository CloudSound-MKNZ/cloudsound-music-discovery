# Music Discovery Service

Discovers and downloads music from YouTube and Bandcamp APIs.

## Features

- YouTube API integration
- Bandcamp API integration
- Music link extraction
- MP3 download and storage
- Kafka consumer for event processing
- RabbitMQ task queue for downloads

## Development

```bash
cd backend/music-discovery
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn src.main:app --reload --port 8003
```

