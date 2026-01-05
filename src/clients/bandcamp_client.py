"""Bandcamp API client with circuit breaker protection.

This client handles extracting music metadata from Bandcamp pages.
Since Bandcamp doesn't have a public API, this uses web scraping
techniques to extract track information.
"""
import asyncio
import re
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
import aiohttp
from urllib.parse import urlparse
import structlog

from ..utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpen
from ..utils.retry import retry_with_backoff

logger = structlog.get_logger(__name__)

# Bandcamp URL patterns
BANDCAMP_URL_PATTERNS = [
    # Track: https://artist.bandcamp.com/track/track-name
    re.compile(r'(?:https?://)?([a-zA-Z0-9-]+)\.bandcamp\.com/track/([a-zA-Z0-9-]+)'),
    # Album: https://artist.bandcamp.com/album/album-name
    re.compile(r'(?:https?://)?([a-zA-Z0-9-]+)\.bandcamp\.com/album/([a-zA-Z0-9-]+)'),
    # Custom domain track
    re.compile(r'(?:https?://)?([a-zA-Z0-9.-]+)/track/([a-zA-Z0-9-]+)'),
    # Custom domain album
    re.compile(r'(?:https?://)?([a-zA-Z0-9.-]+)/album/([a-zA-Z0-9-]+)'),
]


@dataclass
class BandcampTrack:
    """Represents a Bandcamp track."""
    track_id: str
    title: str
    artist: str
    album: Optional[str] = None
    duration: int = 0  # Duration in seconds
    url: str = ""
    artwork_url: Optional[str] = None
    is_downloadable: bool = True
    price: Optional[float] = None  # None if free


@dataclass
class BandcampAlbum:
    """Represents a Bandcamp album."""
    album_id: str
    title: str
    artist: str
    url: str
    artwork_url: Optional[str] = None
    tracks: List[BandcampTrack] = None
    release_date: Optional[str] = None
    
    def __post_init__(self):
        if self.tracks is None:
            self.tracks = []


class BandcampClientError(Exception):
    """Base exception for Bandcamp client errors."""
    pass


class BandcampPageNotFound(BandcampClientError):
    """Raised when Bandcamp page is not found."""
    pass


class BandcampParseError(BandcampClientError):
    """Raised when unable to parse Bandcamp page."""
    pass


class BandcampClient:
    """Client for Bandcamp music extraction.
    
    Since Bandcamp doesn't have a public API, this client scrapes
    publicly available pages to extract track metadata.
    
    Usage:
        client = BandcampClient()
        
        # Check if URL is Bandcamp
        if client.is_bandcamp_url(url):
            info = client.parse_url(url)
            
            # Get track info
            track = await client.get_track_info(info['artist'], info['slug'])
    """
    
    def __init__(
        self,
        use_mock: bool = True,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 60.0,
    ):
        """Initialize Bandcamp client.
        
        Args:
            use_mock: Use mock responses instead of real scraping
            circuit_breaker_threshold: Number of failures before opening circuit
            circuit_breaker_timeout: Seconds before trying to reset circuit
        """
        self.use_mock = use_mock
        
        self._circuit_breaker = CircuitBreaker(
            name="bandcamp_api",
            failure_threshold=circuit_breaker_threshold,
            timeout=circuit_breaker_timeout,
            excluded_exceptions=(BandcampPageNotFound,),
        )
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        logger.info("bandcamp_client_initialized", use_mock=self.use_mock)
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
        return self._session
    
    async def close(self) -> None:
        """Close the client session."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("bandcamp_client_closed")
    
    @staticmethod
    def is_bandcamp_url(url: str) -> bool:
        """Check if URL is a Bandcamp URL.
        
        Args:
            url: URL to check
            
        Returns:
            True if valid Bandcamp URL
        """
        for pattern in BANDCAMP_URL_PATTERNS:
            if pattern.match(url):
                return True
        
        # Also check for bandcamp.com in URL
        parsed = urlparse(url)
        return "bandcamp.com" in parsed.netloc
    
    @staticmethod
    def parse_url(url: str) -> Optional[Dict[str, str]]:
        """Parse Bandcamp URL to extract artist and slug.
        
        Args:
            url: Bandcamp URL
            
        Returns:
            Dict with 'artist', 'slug', and 'type' (track/album), or None
        """
        for i, pattern in enumerate(BANDCAMP_URL_PATTERNS):
            match = pattern.match(url)
            if match:
                url_type = "track" if "track" in url else "album"
                return {
                    "artist": match.group(1),
                    "slug": match.group(2),
                    "type": url_type,
                    "url": url,
                }
        return None
    
    def _get_mock_track_info(self, artist: str, track_slug: str) -> BandcampTrack:
        """Get mock track info for development."""
        return BandcampTrack(
            track_id=f"{artist}-{track_slug}",
            title=track_slug.replace("-", " ").title(),
            artist=artist.replace("-", " ").title(),
            duration=210,  # 3:30
            url=f"https://{artist}.bandcamp.com/track/{track_slug}",
            artwork_url=f"https://f4.bcbits.com/img/a0000000000_10.jpg",
            is_downloadable=True,
        )
    
    def _get_mock_album_info(self, artist: str, album_slug: str) -> BandcampAlbum:
        """Get mock album info for development."""
        tracks = []
        for i in range(5):
            tracks.append(BandcampTrack(
                track_id=f"{artist}-{album_slug}-track{i+1}",
                title=f"Track {i + 1}",
                artist=artist.replace("-", " ").title(),
                album=album_slug.replace("-", " ").title(),
                duration=180 + i * 30,
                url=f"https://{artist}.bandcamp.com/track/track-{i+1}",
                is_downloadable=True,
            ))
        
        return BandcampAlbum(
            album_id=f"{artist}-{album_slug}",
            title=album_slug.replace("-", " ").title(),
            artist=artist.replace("-", " ").title(),
            url=f"https://{artist}.bandcamp.com/album/{album_slug}",
            artwork_url=f"https://f4.bcbits.com/img/a0000000000_10.jpg",
            tracks=tracks,
        )
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    async def get_track_info(self, artist: str, track_slug: str) -> BandcampTrack:
        """Get track information from Bandcamp.
        
        Args:
            artist: Artist subdomain or domain
            track_slug: Track URL slug
            
        Returns:
            BandcampTrack with metadata
            
        Raises:
            BandcampPageNotFound: If track doesn't exist
            BandcampParseError: If unable to parse page
            CircuitBreakerOpen: If circuit breaker is open
        """
        if self.use_mock:
            logger.debug("bandcamp_get_track_mock", artist=artist, track_slug=track_slug)
            return self._get_mock_track_info(artist, track_slug)
        
        async def _fetch():
            session = await self._get_session()
            url = f"https://{artist}.bandcamp.com/track/{track_slug}"
            
            async with session.get(url) as response:
                if response.status == 404:
                    raise BandcampPageNotFound(f"Track not found: {url}")
                
                response.raise_for_status()
                html = await response.text()
                
                return self._parse_track_page(html, url)
        
        return await self._circuit_breaker.call_async(_fetch)
    
    @retry_with_backoff(max_retries=3, base_delay=1.0)
    async def get_album_info(self, artist: str, album_slug: str) -> BandcampAlbum:
        """Get album information from Bandcamp.
        
        Args:
            artist: Artist subdomain or domain
            album_slug: Album URL slug
            
        Returns:
            BandcampAlbum with metadata and tracks
            
        Raises:
            BandcampPageNotFound: If album doesn't exist
            BandcampParseError: If unable to parse page
            CircuitBreakerOpen: If circuit breaker is open
        """
        if self.use_mock:
            logger.debug("bandcamp_get_album_mock", artist=artist, album_slug=album_slug)
            return self._get_mock_album_info(artist, album_slug)
        
        async def _fetch():
            session = await self._get_session()
            url = f"https://{artist}.bandcamp.com/album/{album_slug}"
            
            async with session.get(url) as response:
                if response.status == 404:
                    raise BandcampPageNotFound(f"Album not found: {url}")
                
                response.raise_for_status()
                html = await response.text()
                
                return self._parse_album_page(html, url)
        
        return await self._circuit_breaker.call_async(_fetch)
    
    async def get_info_from_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Get track or album info from a Bandcamp URL.
        
        Args:
            url: Bandcamp URL (track or album)
            
        Returns:
            Dict with 'type' ('track' or 'album') and corresponding data
        """
        parsed = self.parse_url(url)
        if not parsed:
            return None
        
        try:
            if parsed["type"] == "track":
                track = await self.get_track_info(parsed["artist"], parsed["slug"])
                return {"type": "track", "data": track}
            else:
                album = await self.get_album_info(parsed["artist"], parsed["slug"])
                return {"type": "album", "data": album}
        except (BandcampPageNotFound, BandcampParseError) as e:
            logger.warning("bandcamp_get_info_failed", url=url, error=str(e))
            return None
    
    def _parse_track_page(self, html: str, url: str) -> BandcampTrack:
        """Parse track page HTML to extract metadata.
        
        This is a simplified parser. Production would use proper HTML parsing.
        """
        # Extract title
        title_match = re.search(r'<h2 class="trackTitle"[^>]*>([^<]+)</h2>', html)
        title = title_match.group(1).strip() if title_match else "Unknown"
        
        # Extract artist
        artist_match = re.search(r'<span itemprop="byArtist"[^>]*>.*?<a[^>]*>([^<]+)</a>', html, re.DOTALL)
        artist = artist_match.group(1).strip() if artist_match else "Unknown"
        
        # Extract duration
        duration_match = re.search(r'"duration":(\d+)', html)
        duration = int(duration_match.group(1)) if duration_match else 0
        
        # Extract artwork
        art_match = re.search(r'<a class="popupImage" href="([^"]+)"', html)
        artwork_url = art_match.group(1) if art_match else None
        
        return BandcampTrack(
            track_id=url,
            title=title,
            artist=artist,
            duration=duration,
            url=url,
            artwork_url=artwork_url,
        )
    
    def _parse_album_page(self, html: str, url: str) -> BandcampAlbum:
        """Parse album page HTML to extract metadata.
        
        This is a simplified parser. Production would use proper HTML parsing.
        """
        # Extract title
        title_match = re.search(r'<h2 class="trackTitle"[^>]*>([^<]+)</h2>', html)
        title = title_match.group(1).strip() if title_match else "Unknown Album"
        
        # Extract artist
        artist_match = re.search(r'<span itemprop="byArtist"[^>]*>.*?<a[^>]*>([^<]+)</a>', html, re.DOTALL)
        artist = artist_match.group(1).strip() if artist_match else "Unknown"
        
        # Extract artwork
        art_match = re.search(r'<a class="popupImage" href="([^"]+)"', html)
        artwork_url = art_match.group(1) if art_match else None
        
        # Extract tracks (simplified)
        tracks = []
        track_pattern = re.compile(
            r'<span class="track-title">([^<]+)</span>.*?<span class="time[^"]*">(\d+:\d+)</span>',
            re.DOTALL
        )
        
        for i, match in enumerate(track_pattern.finditer(html)):
            track_title = match.group(1).strip()
            time_str = match.group(2)
            parts = time_str.split(":")
            duration = int(parts[0]) * 60 + int(parts[1])
            
            tracks.append(BandcampTrack(
                track_id=f"{url}-{i}",
                title=track_title,
                artist=artist,
                album=title,
                duration=duration,
                url=url,
            ))
        
        return BandcampAlbum(
            album_id=url,
            title=title,
            artist=artist,
            url=url,
            artwork_url=artwork_url,
            tracks=tracks,
        )
    
    def get_circuit_breaker_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return self._circuit_breaker.get_stats()

