"""YouTube API client with circuit breaker protection.

This client handles searching for music on YouTube and extracting
downloadable audio streams. Uses yt-dlp for actual downloads.
"""
import asyncio
import re
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
import aiohttp
import structlog

from ..utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpen
from ..utils.retry import retry_with_backoff

logger = structlog.get_logger(__name__)

# YouTube URL patterns
YOUTUBE_URL_PATTERNS = [
    re.compile(r'(?:https?://)?(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})'),
    re.compile(r'(?:https?://)?(?:www\.)?youtu\.be/([a-zA-Z0-9_-]{11})'),
    re.compile(r'(?:https?://)?(?:www\.)?youtube\.com/embed/([a-zA-Z0-9_-]{11})'),
    re.compile(r'(?:https?://)?(?:www\.)?youtube\.com/v/([a-zA-Z0-9_-]{11})'),
    re.compile(r'(?:https?://)?(?:music\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})'),
]


@dataclass
class YouTubeTrack:
    """Represents a YouTube track/video."""
    video_id: str
    title: str
    channel: str
    duration: int  # Duration in seconds
    thumbnail_url: Optional[str] = None
    view_count: Optional[int] = None
    url: str = ""
    
    def __post_init__(self):
        if not self.url:
            self.url = f"https://www.youtube.com/watch?v={self.video_id}"


@dataclass
class YouTubeSearchResult:
    """Search result from YouTube."""
    tracks: List[YouTubeTrack]
    total_results: int
    next_page_token: Optional[str] = None


class YouTubeClientError(Exception):
    """Base exception for YouTube client errors."""
    pass


class YouTubeQuotaExceeded(YouTubeClientError):
    """Raised when YouTube API quota is exceeded."""
    pass


class YouTubeVideoNotFound(YouTubeClientError):
    """Raised when video is not found."""
    pass


class YouTubeClient:
    """Client for YouTube API and yt-dlp integration.
    
    This client provides:
    - URL validation and video ID extraction
    - Video metadata retrieval
    - Search functionality (mock in development)
    - Circuit breaker protection for API calls
    
    Usage:
        client = YouTubeClient()
        
        # Extract video ID from URL
        video_id = client.extract_video_id("https://youtube.com/watch?v=abc123")
        
        # Get video metadata
        track = await client.get_video_info(video_id)
        
        # Search for tracks
        results = await client.search("artist name", max_results=10)
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        use_mock: bool = True,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 60.0,
    ):
        """Initialize YouTube client.
        
        Args:
            api_key: YouTube Data API key (optional, uses mock if not provided)
            use_mock: Use mock responses instead of real API
            circuit_breaker_threshold: Number of failures before opening circuit
            circuit_breaker_timeout: Seconds before trying to reset circuit
        """
        self.api_key = api_key
        self.use_mock = use_mock or not api_key
        self.base_url = "https://www.googleapis.com/youtube/v3"
        
        self._circuit_breaker = CircuitBreaker(
            name="youtube_api",
            failure_threshold=circuit_breaker_threshold,
            timeout=circuit_breaker_timeout,
            excluded_exceptions=(YouTubeVideoNotFound,),
        )
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        logger.info(
            "youtube_client_initialized",
            use_mock=self.use_mock,
            has_api_key=bool(api_key),
        )
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._session
    
    async def close(self) -> None:
        """Close the client session."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("youtube_client_closed")
    
    @staticmethod
    def extract_video_id(url: str) -> Optional[str]:
        """Extract video ID from YouTube URL.
        
        Args:
            url: YouTube URL (various formats supported)
            
        Returns:
            Video ID if found, None otherwise
        """
        for pattern in YOUTUBE_URL_PATTERNS:
            match = pattern.search(url)
            if match:
                return match.group(1)
        return None
    
    @staticmethod
    def is_youtube_url(url: str) -> bool:
        """Check if URL is a valid YouTube URL.
        
        Args:
            url: URL to check
            
        Returns:
            True if valid YouTube URL
        """
        return YouTubeClient.extract_video_id(url) is not None
    
    def _get_mock_video_info(self, video_id: str) -> YouTubeTrack:
        """Get mock video info for development."""
        return YouTubeTrack(
            video_id=video_id,
            title=f"Mock Song - Video {video_id[:4]}",
            channel="Mock Artist",
            duration=240,  # 4 minutes
            thumbnail_url=f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg",
            view_count=100000,
        )
    
    def _get_mock_search_results(self, query: str, max_results: int) -> YouTubeSearchResult:
        """Get mock search results for development."""
        tracks = []
        for i in range(min(max_results, 5)):
            video_id = f"mock{i:07d}abc"
            tracks.append(YouTubeTrack(
                video_id=video_id,
                title=f"{query} - Track {i + 1}",
                channel=f"Artist for {query}",
                duration=180 + i * 30,
                thumbnail_url=f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg",
                view_count=10000 * (i + 1),
            ))
        
        return YouTubeSearchResult(
            tracks=tracks,
            total_results=len(tracks),
        )
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    async def get_video_info(self, video_id: str) -> YouTubeTrack:
        """Get video information from YouTube.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            YouTubeTrack with video metadata
            
        Raises:
            YouTubeVideoNotFound: If video doesn't exist
            YouTubeClientError: For other API errors
            CircuitBreakerOpen: If circuit breaker is open
        """
        if self.use_mock:
            logger.debug("youtube_get_video_mock", video_id=video_id)
            return self._get_mock_video_info(video_id)
        
        async def _fetch():
            session = await self._get_session()
            
            params = {
                "part": "snippet,contentDetails,statistics",
                "id": video_id,
                "key": self.api_key,
            }
            
            async with session.get(f"{self.base_url}/videos", params=params) as response:
                if response.status == 403:
                    raise YouTubeQuotaExceeded("YouTube API quota exceeded")
                
                response.raise_for_status()
                data = await response.json()
                
                if not data.get("items"):
                    raise YouTubeVideoNotFound(f"Video {video_id} not found")
                
                item = data["items"][0]
                snippet = item["snippet"]
                content_details = item["contentDetails"]
                statistics = item.get("statistics", {})
                
                # Parse duration (ISO 8601 format)
                duration = self._parse_duration(content_details.get("duration", "PT0S"))
                
                return YouTubeTrack(
                    video_id=video_id,
                    title=snippet.get("title", "Unknown"),
                    channel=snippet.get("channelTitle", "Unknown"),
                    duration=duration,
                    thumbnail_url=snippet.get("thumbnails", {}).get("high", {}).get("url"),
                    view_count=int(statistics.get("viewCount", 0)),
                )
        
        return await self._circuit_breaker.call_async(_fetch)
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    async def search(
        self,
        query: str,
        max_results: int = 10,
        page_token: Optional[str] = None,
    ) -> YouTubeSearchResult:
        """Search for videos on YouTube.
        
        Args:
            query: Search query string
            max_results: Maximum number of results (1-50)
            page_token: Token for pagination
            
        Returns:
            YouTubeSearchResult with matching tracks
            
        Raises:
            YouTubeClientError: For API errors
            CircuitBreakerOpen: If circuit breaker is open
        """
        if self.use_mock:
            logger.debug("youtube_search_mock", query=query, max_results=max_results)
            return self._get_mock_search_results(query, max_results)
        
        async def _fetch():
            session = await self._get_session()
            
            params = {
                "part": "snippet",
                "q": query,
                "type": "video",
                "videoCategoryId": "10",  # Music category
                "maxResults": min(max_results, 50),
                "key": self.api_key,
            }
            
            if page_token:
                params["pageToken"] = page_token
            
            async with session.get(f"{self.base_url}/search", params=params) as response:
                if response.status == 403:
                    raise YouTubeQuotaExceeded("YouTube API quota exceeded")
                
                response.raise_for_status()
                data = await response.json()
                
                tracks = []
                for item in data.get("items", []):
                    video_id = item["id"]["videoId"]
                    snippet = item["snippet"]
                    
                    tracks.append(YouTubeTrack(
                        video_id=video_id,
                        title=snippet.get("title", "Unknown"),
                        channel=snippet.get("channelTitle", "Unknown"),
                        duration=0,  # Not available in search results
                        thumbnail_url=snippet.get("thumbnails", {}).get("high", {}).get("url"),
                    ))
                
                return YouTubeSearchResult(
                    tracks=tracks,
                    total_results=data.get("pageInfo", {}).get("totalResults", len(tracks)),
                    next_page_token=data.get("nextPageToken"),
                )
        
        return await self._circuit_breaker.call_async(_fetch)
    
    @staticmethod
    def _parse_duration(duration_str: str) -> int:
        """Parse ISO 8601 duration to seconds.
        
        Args:
            duration_str: Duration string (e.g., "PT4M30S")
            
        Returns:
            Duration in seconds
        """
        pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
        match = pattern.match(duration_str)
        
        if not match:
            return 0
        
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        
        return hours * 3600 + minutes * 60 + seconds
    
    def get_circuit_breaker_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return self._circuit_breaker.get_stats()

