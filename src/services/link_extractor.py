"""Link extraction service for finding music URLs in text.

Extracts YouTube and Bandcamp links from concert descriptions,
event text, and other sources.
"""
import re
from typing import List, Optional, Set
from dataclasses import dataclass
from enum import Enum
import structlog

from ..clients.youtube_client import YouTubeClient
from ..clients.bandcamp_client import BandcampClient

logger = structlog.get_logger(__name__)


class LinkType(Enum):
    """Types of music links."""
    YOUTUBE = "youtube"
    BANDCAMP = "bandcamp"
    SOUNDCLOUD = "soundcloud"  # Future support
    SPOTIFY = "spotify"  # Future support
    UNKNOWN = "unknown"


@dataclass
class ExtractedLink:
    """Represents an extracted music link."""
    url: str
    link_type: LinkType
    video_id: Optional[str] = None  # For YouTube
    artist: Optional[str] = None  # For Bandcamp
    slug: Optional[str] = None  # Track/album slug
    content_type: Optional[str] = None  # "track" or "album"
    
    def __hash__(self):
        return hash(self.url)
    
    def __eq__(self, other):
        if not isinstance(other, ExtractedLink):
            return False
        return self.url == other.url


# URL patterns for different platforms
URL_PATTERNS = {
    LinkType.YOUTUBE: [
        re.compile(r'https?://(?:www\.)?youtube\.com/watch\?v=[a-zA-Z0-9_-]{11}[^\s]*'),
        re.compile(r'https?://youtu\.be/[a-zA-Z0-9_-]{11}[^\s]*'),
        re.compile(r'https?://(?:www\.)?youtube\.com/embed/[a-zA-Z0-9_-]{11}[^\s]*'),
        re.compile(r'https?://(?:music\.)?youtube\.com/watch\?v=[a-zA-Z0-9_-]{11}[^\s]*'),
    ],
    LinkType.BANDCAMP: [
        re.compile(r'https?://[a-zA-Z0-9-]+\.bandcamp\.com/(?:track|album)/[a-zA-Z0-9-]+[^\s]*'),
    ],
    LinkType.SOUNDCLOUD: [
        re.compile(r'https?://(?:www\.)?soundcloud\.com/[a-zA-Z0-9-]+/[a-zA-Z0-9-]+[^\s]*'),
    ],
    LinkType.SPOTIFY: [
        re.compile(r'https?://open\.spotify\.com/(?:track|album)/[a-zA-Z0-9]+[^\s]*'),
    ],
}


class LinkExtractor:
    """Service for extracting music links from text.
    
    Usage:
        extractor = LinkExtractor()
        
        text = '''
        Check out our band on YouTube: https://youtube.com/watch?v=abc123xyz
        And our Bandcamp: https://ourband.bandcamp.com/album/debut
        '''
        
        links = extractor.extract_links(text)
        for link in links:
            print(f"{link.link_type.value}: {link.url}")
    """
    
    def __init__(
        self,
        youtube_client: Optional[YouTubeClient] = None,
        bandcamp_client: Optional[BandcampClient] = None,
    ):
        """Initialize link extractor.
        
        Args:
            youtube_client: Optional YouTube client for validation
            bandcamp_client: Optional Bandcamp client for validation
        """
        self.youtube_client = youtube_client
        self.bandcamp_client = bandcamp_client
        
        logger.info("link_extractor_initialized")
    
    def extract_links(
        self,
        text: str,
        include_types: Optional[Set[LinkType]] = None,
    ) -> List[ExtractedLink]:
        """Extract all music links from text.
        
        Args:
            text: Text to search for links
            include_types: Optional set of link types to include (default: all)
            
        Returns:
            List of extracted links (deduplicated)
        """
        if not text:
            return []
        
        links: Set[ExtractedLink] = set()
        
        for link_type, patterns in URL_PATTERNS.items():
            # Skip types not in include_types if specified
            if include_types and link_type not in include_types:
                continue
            
            for pattern in patterns:
                for match in pattern.finditer(text):
                    url = match.group(0)
                    # Clean up URL (remove trailing punctuation)
                    url = self._clean_url(url)
                    
                    extracted = self._create_extracted_link(url, link_type)
                    if extracted:
                        links.add(extracted)
        
        result = list(links)
        
        logger.debug(
            "links_extracted",
            text_length=len(text),
            link_count=len(result),
            types=[l.link_type.value for l in result],
        )
        
        return result
    
    def extract_youtube_links(self, text: str) -> List[ExtractedLink]:
        """Extract only YouTube links from text.
        
        Args:
            text: Text to search
            
        Returns:
            List of YouTube links
        """
        return self.extract_links(text, include_types={LinkType.YOUTUBE})
    
    def extract_bandcamp_links(self, text: str) -> List[ExtractedLink]:
        """Extract only Bandcamp links from text.
        
        Args:
            text: Text to search
            
        Returns:
            List of Bandcamp links
        """
        return self.extract_links(text, include_types={LinkType.BANDCAMP})
    
    def extract_downloadable_links(self, text: str) -> List[ExtractedLink]:
        """Extract only downloadable links (YouTube and Bandcamp).
        
        Args:
            text: Text to search
            
        Returns:
            List of downloadable links
        """
        return self.extract_links(
            text,
            include_types={LinkType.YOUTUBE, LinkType.BANDCAMP}
        )
    
    def _clean_url(self, url: str) -> str:
        """Clean up extracted URL by removing trailing punctuation.
        
        Args:
            url: Raw URL from regex match
            
        Returns:
            Cleaned URL
        """
        # Remove common trailing punctuation
        while url and url[-1] in '.,;:!?)"\'>':
            url = url[:-1]
        return url
    
    def _create_extracted_link(self, url: str, link_type: LinkType) -> Optional[ExtractedLink]:
        """Create ExtractedLink with additional metadata.
        
        Args:
            url: URL to process
            link_type: Type of link
            
        Returns:
            ExtractedLink with metadata, or None if invalid
        """
        try:
            if link_type == LinkType.YOUTUBE:
                video_id = YouTubeClient.extract_video_id(url)
                if video_id:
                    return ExtractedLink(
                        url=url,
                        link_type=link_type,
                        video_id=video_id,
                        content_type="track",
                    )
            
            elif link_type == LinkType.BANDCAMP:
                parsed = BandcampClient.parse_url(url)
                if parsed:
                    return ExtractedLink(
                        url=url,
                        link_type=link_type,
                        artist=parsed.get("artist"),
                        slug=parsed.get("slug"),
                        content_type=parsed.get("type"),
                    )
            
            else:
                # For other types, just return basic info
                return ExtractedLink(
                    url=url,
                    link_type=link_type,
                )
                
        except Exception as e:
            logger.warning("link_extraction_failed", url=url, error=str(e))
        
        return None
    
    async def validate_links(
        self,
        links: List[ExtractedLink],
    ) -> List[ExtractedLink]:
        """Validate extracted links by checking if they exist.
        
        Args:
            links: List of links to validate
            
        Returns:
            List of valid links
        """
        valid_links = []
        
        for link in links:
            try:
                if link.link_type == LinkType.YOUTUBE and self.youtube_client:
                    if link.video_id:
                        await self.youtube_client.get_video_info(link.video_id)
                        valid_links.append(link)
                
                elif link.link_type == LinkType.BANDCAMP and self.bandcamp_client:
                    if link.artist and link.slug:
                        if link.content_type == "track":
                            await self.bandcamp_client.get_track_info(
                                link.artist, link.slug
                            )
                        else:
                            await self.bandcamp_client.get_album_info(
                                link.artist, link.slug
                            )
                        valid_links.append(link)
                
                else:
                    # No client to validate, assume valid
                    valid_links.append(link)
                    
            except Exception as e:
                logger.warning(
                    "link_validation_failed",
                    url=link.url,
                    error=str(e),
                )
        
        logger.debug(
            "links_validated",
            total=len(links),
            valid=len(valid_links),
        )
        
        return valid_links
    
    @staticmethod
    def get_link_type(url: str) -> LinkType:
        """Determine the type of a music link.
        
        Args:
            url: URL to check
            
        Returns:
            LinkType enum value
        """
        for link_type, patterns in URL_PATTERNS.items():
            for pattern in patterns:
                if pattern.match(url):
                    return link_type
        return LinkType.UNKNOWN

