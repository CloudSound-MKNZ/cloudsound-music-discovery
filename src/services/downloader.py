"""Music downloader service using yt-dlp.

Downloads audio from YouTube and Bandcamp URLs, converts to MP3,
and handles storage quota management.
"""
import asyncio
import os
import tempfile
import subprocess
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import structlog

from ..clients.youtube_client import YouTubeClient, YouTubeTrack
from ..clients.bandcamp_client import BandcampClient, BandcampTrack
from .link_extractor import ExtractedLink, LinkType

logger = structlog.get_logger(__name__)


class DownloadStatus(Enum):
    """Status of a download."""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    CONVERTING = "converting"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    QUOTA_EXCEEDED = "quota_exceeded"


@dataclass
class DownloadResult:
    """Result of a download operation."""
    url: str
    status: DownloadStatus
    file_path: Optional[str] = None  # Path in storage (e.g., MinIO key)
    title: Optional[str] = None
    artist: Optional[str] = None
    duration: int = 0  # Duration in seconds
    file_size: int = 0  # Size in bytes
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class DownloaderError(Exception):
    """Base exception for downloader errors."""
    pass


class QuotaExceededError(DownloaderError):
    """Raised when storage quota is exceeded."""
    pass


class DownloadFailedError(DownloaderError):
    """Raised when download fails."""
    pass


class MusicDownloader:
    """Service for downloading music from various sources.
    
    Uses yt-dlp for downloading and converting audio from YouTube
    and Bandcamp. Integrates with storage service for quota management.
    
    Usage:
        downloader = MusicDownloader(storage_service=storage)
        
        result = await downloader.download(extracted_link)
        if result.status == DownloadStatus.COMPLETED:
            print(f"Downloaded: {result.title} to {result.file_path}")
    """
    
    def __init__(
        self,
        storage_service: Optional[Any] = None,  # StorageService
        youtube_client: Optional[YouTubeClient] = None,
        bandcamp_client: Optional[BandcampClient] = None,
        temp_dir: Optional[str] = None,
        audio_format: str = "mp3",
        audio_quality: str = "192",  # kbps
        max_file_size_mb: int = 50,  # Maximum file size in MB
    ):
        """Initialize music downloader.
        
        Args:
            storage_service: Optional storage service for uploads
            youtube_client: Optional YouTube client
            bandcamp_client: Optional Bandcamp client
            temp_dir: Temporary directory for downloads
            audio_format: Output audio format
            audio_quality: Audio quality in kbps
            max_file_size_mb: Maximum allowed file size
        """
        self.storage_service = storage_service
        self.youtube_client = youtube_client or YouTubeClient()
        self.bandcamp_client = bandcamp_client or BandcampClient()
        self.temp_dir = temp_dir or tempfile.gettempdir()
        self.audio_format = audio_format
        self.audio_quality = audio_quality
        self.max_file_size = max_file_size_mb * 1024 * 1024  # Convert to bytes
        
        logger.info(
            "music_downloader_initialized",
            temp_dir=self.temp_dir,
            audio_format=audio_format,
            audio_quality=audio_quality,
            max_file_size_mb=max_file_size_mb,
        )
    
    async def download(
        self,
        link: ExtractedLink,
        progress_callback: Optional[Callable[[str, float], None]] = None,
    ) -> DownloadResult:
        """Download music from an extracted link.
        
        Args:
            link: Extracted link to download
            progress_callback: Optional callback(url, progress_percent)
            
        Returns:
            DownloadResult with status and file info
        """
        logger.info("download_started", url=link.url, link_type=link.link_type.value)
        
        try:
            # Check storage quota if available
            if self.storage_service:
                quota = await self.storage_service.get_quota()
                if quota.used >= quota.limit:
                    logger.warning("download_quota_exceeded", url=link.url)
                    return DownloadResult(
                        url=link.url,
                        status=DownloadStatus.QUOTA_EXCEEDED,
                        error="Storage quota exceeded",
                    )
            
            # Get metadata
            metadata = await self._get_metadata(link)
            
            # Download audio
            if progress_callback:
                progress_callback(link.url, 0.1)
            
            temp_file = await self._download_audio(link.url)
            
            if progress_callback:
                progress_callback(link.url, 0.7)
            
            # Check file size
            file_size = os.path.getsize(temp_file)
            if file_size > self.max_file_size:
                os.remove(temp_file)
                return DownloadResult(
                    url=link.url,
                    status=DownloadStatus.FAILED,
                    error=f"File too large: {file_size / 1024 / 1024:.1f}MB > {self.max_file_size / 1024 / 1024}MB",
                )
            
            # Upload to storage if available
            storage_path = None
            if self.storage_service:
                storage_path = await self._upload_to_storage(
                    temp_file, metadata, link
                )
                os.remove(temp_file)  # Clean up temp file
            else:
                storage_path = temp_file  # Keep local file
            
            if progress_callback:
                progress_callback(link.url, 1.0)
            
            logger.info(
                "download_completed",
                url=link.url,
                title=metadata.get("title"),
                file_size=file_size,
                storage_path=storage_path,
            )
            
            return DownloadResult(
                url=link.url,
                status=DownloadStatus.COMPLETED,
                file_path=storage_path,
                title=metadata.get("title"),
                artist=metadata.get("artist"),
                duration=metadata.get("duration", 0),
                file_size=file_size,
                metadata=metadata,
            )
            
        except QuotaExceededError as e:
            return DownloadResult(
                url=link.url,
                status=DownloadStatus.QUOTA_EXCEEDED,
                error=str(e),
            )
        except Exception as e:
            logger.error("download_failed", url=link.url, error=str(e))
            return DownloadResult(
                url=link.url,
                status=DownloadStatus.FAILED,
                error=str(e),
            )
    
    async def _get_metadata(self, link: ExtractedLink) -> Dict[str, Any]:
        """Get metadata for a link.
        
        Args:
            link: Extracted link
            
        Returns:
            Dict with title, artist, duration, etc.
        """
        metadata: Dict[str, Any] = {}
        
        try:
            if link.link_type == LinkType.YOUTUBE and link.video_id:
                track = await self.youtube_client.get_video_info(link.video_id)
                metadata = {
                    "title": track.title,
                    "artist": track.channel,
                    "duration": track.duration,
                    "source": "youtube",
                    "video_id": track.video_id,
                    "thumbnail_url": track.thumbnail_url,
                }
            
            elif link.link_type == LinkType.BANDCAMP and link.artist and link.slug:
                if link.content_type == "track":
                    track = await self.bandcamp_client.get_track_info(
                        link.artist, link.slug
                    )
                    metadata = {
                        "title": track.title,
                        "artist": track.artist,
                        "album": track.album,
                        "duration": track.duration,
                        "source": "bandcamp",
                        "artwork_url": track.artwork_url,
                    }
                else:
                    # For albums, get first track info
                    album = await self.bandcamp_client.get_album_info(
                        link.artist, link.slug
                    )
                    metadata = {
                        "title": album.title,
                        "artist": album.artist,
                        "is_album": True,
                        "track_count": len(album.tracks),
                        "source": "bandcamp",
                        "artwork_url": album.artwork_url,
                    }
            
        except Exception as e:
            logger.warning("metadata_fetch_failed", url=link.url, error=str(e))
            metadata = {"title": "Unknown", "artist": "Unknown"}
        
        return metadata
    
    async def _download_audio(self, url: str) -> str:
        """Download audio using yt-dlp.
        
        Args:
            url: URL to download
            
        Returns:
            Path to downloaded file
            
        Raises:
            DownloadFailedError: If download fails
        """
        # Generate unique filename
        import uuid
        output_template = os.path.join(
            self.temp_dir,
            f"cloudsound_{uuid.uuid4().hex[:8]}.%(ext)s"
        )
        
        # yt-dlp command
        cmd = [
            "yt-dlp",
            "--extract-audio",
            f"--audio-format={self.audio_format}",
            f"--audio-quality={self.audio_quality}K",
            "--no-playlist",
            "--no-warnings",
            "-o", output_template,
            url,
        ]
        
        try:
            # Run download in thread pool to not block async
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=300,  # 5 minute timeout
                )
            )
            
            if result.returncode != 0:
                raise DownloadFailedError(
                    f"yt-dlp failed: {result.stderr or result.stdout}"
                )
            
            # Find the output file
            output_pattern = output_template.replace(".%(ext)s", f".{self.audio_format}")
            if os.path.exists(output_pattern):
                return output_pattern
            
            # Try to find any file matching the pattern
            base_name = output_template.replace(".%(ext)s", "")
            for ext in ["mp3", "m4a", "opus", "webm"]:
                candidate = f"{base_name}.{ext}"
                if os.path.exists(candidate):
                    return candidate
            
            raise DownloadFailedError("Output file not found after download")
            
        except subprocess.TimeoutExpired:
            raise DownloadFailedError("Download timed out after 5 minutes")
        except FileNotFoundError:
            raise DownloadFailedError("yt-dlp not found. Please install it.")
    
    async def _upload_to_storage(
        self,
        file_path: str,
        metadata: Dict[str, Any],
        link: ExtractedLink,
    ) -> str:
        """Upload file to storage service.
        
        Args:
            file_path: Local file path
            metadata: Track metadata
            link: Original extracted link
            
        Returns:
            Storage path/key
        """
        # Generate storage key
        import uuid
        artist = metadata.get("artist", "unknown").replace(" ", "-").lower()[:30]
        title = metadata.get("title", "unknown").replace(" ", "-").lower()[:50]
        unique_id = uuid.uuid4().hex[:8]
        
        storage_key = f"tracks/{artist}/{title}-{unique_id}.{self.audio_format}"
        
        # Upload
        with open(file_path, "rb") as f:
            await self.storage_service.upload_file(
                storage_key,
                f,
                content_type=f"audio/{self.audio_format}",
                metadata={
                    "source_url": link.url,
                    "source_type": link.link_type.value,
                    **{k: str(v) for k, v in metadata.items() if v is not None}
                }
            )
        
        return storage_key
    
    async def download_batch(
        self,
        links: list[ExtractedLink],
        max_concurrent: int = 3,
        progress_callback: Optional[Callable[[str, float], None]] = None,
    ) -> list[DownloadResult]:
        """Download multiple links concurrently.
        
        Args:
            links: List of links to download
            max_concurrent: Maximum concurrent downloads
            progress_callback: Optional callback(url, progress_percent)
            
        Returns:
            List of download results
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def download_with_semaphore(link: ExtractedLink) -> DownloadResult:
            async with semaphore:
                return await self.download(link, progress_callback)
        
        tasks = [download_with_semaphore(link) for link in links]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert exceptions to failed results
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                final_results.append(DownloadResult(
                    url=links[i].url,
                    status=DownloadStatus.FAILED,
                    error=str(result),
                ))
            else:
                final_results.append(result)
        
        return final_results

