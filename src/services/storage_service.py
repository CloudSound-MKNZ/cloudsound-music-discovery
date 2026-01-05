"""Storage service with quota monitoring.

Wraps the shared StorageClient with quota management, monitoring,
and music-discovery-specific functionality.
"""
import asyncio
from typing import Optional, BinaryIO, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime, timedelta
import structlog

from cloudsound_shared.storage import StorageClient
from cloudsound_shared.config.settings import app_settings

logger = structlog.get_logger(__name__)


@dataclass
class StorageQuota:
    """Storage quota information."""
    limit: int  # Limit in bytes
    used: int  # Used space in bytes
    available: int  # Available space in bytes
    usage_percent: float  # Usage percentage
    last_updated: datetime
    
    @property
    def limit_gb(self) -> float:
        """Limit in GB."""
        return self.limit / (1024 ** 3)
    
    @property
    def used_gb(self) -> float:
        """Used space in GB."""
        return self.used / (1024 ** 3)
    
    @property
    def available_gb(self) -> float:
        """Available space in GB."""
        return self.available / (1024 ** 3)
    
    def is_exceeded(self) -> bool:
        """Check if quota is exceeded."""
        return self.used >= self.limit
    
    def can_store(self, file_size: int) -> bool:
        """Check if file can be stored within quota."""
        return (self.used + file_size) <= self.limit


@dataclass
class StoredFile:
    """Information about a stored file."""
    key: str  # Storage key/path
    size: int  # Size in bytes
    content_type: str
    last_modified: datetime
    metadata: Dict[str, str]


class StorageQuotaExceeded(Exception):
    """Raised when storage quota is exceeded."""
    pass


class StorageService:
    """Service for managing music file storage with quota monitoring.
    
    Provides:
    - File upload/download with quota checking
    - Storage quota monitoring and alerts
    - Cleanup of old/unused files
    - Presigned URL generation for streaming
    
    Usage:
        storage = StorageService(quota_limit_gb=10)
        
        # Check quota before upload
        quota = await storage.get_quota()
        if quota.can_store(file_size):
            await storage.upload_file(key, file_data)
    """
    
    def __init__(
        self,
        quota_limit_gb: float = 10.0,
        warning_threshold_percent: float = 80.0,
        client: Optional[StorageClient] = None,
    ):
        """Initialize storage service.
        
        Args:
            quota_limit_gb: Storage quota limit in GB
            warning_threshold_percent: Percentage at which to warn about quota
            client: Optional pre-configured storage client
        """
        self.quota_limit = int(quota_limit_gb * 1024 ** 3)
        self.warning_threshold = warning_threshold_percent
        self.client = client or StorageClient()
        
        self._quota_cache: Optional[StorageQuota] = None
        self._quota_cache_ttl = timedelta(minutes=5)
        self._initialized = False
        
        logger.info(
            "storage_service_initialized",
            quota_limit_gb=quota_limit_gb,
            warning_threshold_percent=warning_threshold_percent,
        )
    
    async def initialize(self) -> None:
        """Initialize storage client and ensure bucket exists."""
        if self._initialized:
            return
        
        try:
            # Run connect in executor since it's blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.client.connect)
            self._initialized = True
            logger.info("storage_service_connected")
        except Exception as e:
            logger.error("storage_service_connection_failed", error=str(e))
            raise
    
    async def get_quota(self, force_refresh: bool = False) -> StorageQuota:
        """Get current storage quota status.
        
        Args:
            force_refresh: Force refresh of quota cache
            
        Returns:
            StorageQuota with current status
        """
        # Check cache
        if (
            not force_refresh
            and self._quota_cache
            and datetime.utcnow() - self._quota_cache.last_updated < self._quota_cache_ttl
        ):
            return self._quota_cache
        
        await self.initialize()
        
        # Calculate used space
        used = await self._calculate_used_space()
        available = max(0, self.quota_limit - used)
        usage_percent = (used / self.quota_limit * 100) if self.quota_limit > 0 else 0
        
        quota = StorageQuota(
            limit=self.quota_limit,
            used=used,
            available=available,
            usage_percent=usage_percent,
            last_updated=datetime.utcnow(),
        )
        
        # Cache the result
        self._quota_cache = quota
        
        # Log warning if approaching limit
        if usage_percent >= self.warning_threshold:
            logger.warning(
                "storage_quota_warning",
                usage_percent=usage_percent,
                used_gb=quota.used_gb,
                limit_gb=quota.limit_gb,
            )
        
        return quota
    
    async def _calculate_used_space(self) -> int:
        """Calculate total used space in storage.
        
        Returns:
            Used space in bytes
        """
        try:
            loop = asyncio.get_event_loop()
            
            def _list_objects():
                total = 0
                objects = self.client.client.list_objects(
                    self.client.bucket,
                    recursive=True,
                )
                for obj in objects:
                    total += obj.size
                return total
            
            return await loop.run_in_executor(None, _list_objects)
        except Exception as e:
            logger.error("storage_calculate_space_failed", error=str(e))
            return 0
    
    async def upload_file(
        self,
        key: str,
        file_data: BinaryIO,
        content_type: str = "audio/mpeg",
        metadata: Optional[Dict[str, str]] = None,
        check_quota: bool = True,
    ) -> StoredFile:
        """Upload file to storage.
        
        Args:
            key: Storage key/path for the file
            file_data: File data to upload
            content_type: MIME content type
            metadata: Optional metadata dict
            check_quota: Whether to check quota before upload
            
        Returns:
            StoredFile with upload info
            
        Raises:
            StorageQuotaExceeded: If quota would be exceeded
        """
        await self.initialize()
        
        # Get file size
        file_data.seek(0, 2)
        file_size = file_data.tell()
        file_data.seek(0)
        
        # Check quota
        if check_quota:
            quota = await self.get_quota()
            if not quota.can_store(file_size):
                raise StorageQuotaExceeded(
                    f"Cannot store {file_size / 1024 / 1024:.1f}MB. "
                    f"Available: {quota.available_gb:.2f}GB"
                )
        
        # Upload file
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self.client.upload_file(
                key,
                file_data,
                content_type=content_type,
                length=file_size,
            )
        )
        
        # Invalidate quota cache
        self._quota_cache = None
        
        logger.info(
            "storage_file_uploaded",
            key=key,
            size=file_size,
            content_type=content_type,
        )
        
        return StoredFile(
            key=key,
            size=file_size,
            content_type=content_type,
            last_modified=datetime.utcnow(),
            metadata=metadata or {},
        )
    
    async def get_file(self, key: str) -> bytes:
        """Download file from storage.
        
        Args:
            key: Storage key/path
            
        Returns:
            File data as bytes
        """
        await self.initialize()
        
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, self.client.get_file, key)
        
        logger.debug("storage_file_downloaded", key=key, size=len(data))
        return data
    
    async def delete_file(self, key: str) -> None:
        """Delete file from storage.
        
        Args:
            key: Storage key/path
        """
        await self.initialize()
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.client.delete_file, key)
        
        # Invalidate quota cache
        self._quota_cache = None
        
        logger.info("storage_file_deleted", key=key)
    
    async def file_exists(self, key: str) -> bool:
        """Check if file exists in storage.
        
        Args:
            key: Storage key/path
            
        Returns:
            True if file exists
        """
        await self.initialize()
        return await self.client.file_exists(key)
    
    async def get_presigned_url(
        self,
        key: str,
        expires_seconds: int = 3600,
    ) -> str:
        """Generate presigned URL for file access.
        
        Args:
            key: Storage key/path
            expires_seconds: URL expiration time
            
        Returns:
            Presigned URL string
        """
        await self.initialize()
        
        loop = asyncio.get_event_loop()
        url = await loop.run_in_executor(
            None,
            lambda: self.client.get_presigned_url(key, expires_seconds)
        )
        
        return url
    
    async def list_files(
        self,
        prefix: str = "",
        limit: Optional[int] = None,
    ) -> List[StoredFile]:
        """List files in storage.
        
        Args:
            prefix: Optional prefix to filter by
            limit: Optional limit on number of files
            
        Returns:
            List of StoredFile objects
        """
        await self.initialize()
        
        loop = asyncio.get_event_loop()
        
        def _list():
            files = []
            objects = self.client.client.list_objects(
                self.client.bucket,
                prefix=prefix,
                recursive=True,
            )
            for obj in objects:
                files.append(StoredFile(
                    key=obj.object_name,
                    size=obj.size,
                    content_type=obj.content_type or "application/octet-stream",
                    last_modified=obj.last_modified,
                    metadata={},
                ))
                if limit and len(files) >= limit:
                    break
            return files
        
        return await loop.run_in_executor(None, _list)
    
    async def cleanup_old_files(
        self,
        older_than_days: int = 30,
        prefix: str = "tracks/",
        dry_run: bool = True,
    ) -> List[str]:
        """Clean up old files to free space.
        
        Args:
            older_than_days: Delete files older than this
            prefix: Prefix to filter files
            dry_run: If True, only report what would be deleted
            
        Returns:
            List of deleted (or would-be-deleted) file keys
        """
        await self.initialize()
        
        cutoff_date = datetime.utcnow() - timedelta(days=older_than_days)
        files = await self.list_files(prefix=prefix)
        
        old_files = [f for f in files if f.last_modified < cutoff_date]
        deleted = []
        
        for file in old_files:
            if not dry_run:
                await self.delete_file(file.key)
            deleted.append(file.key)
            
            logger.info(
                "storage_cleanup",
                key=file.key,
                age_days=(datetime.utcnow() - file.last_modified).days,
                dry_run=dry_run,
            )
        
        return deleted
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics.
        
        Returns:
            Dict with storage statistics
        """
        quota = await self.get_quota()
        files = await self.list_files(prefix="tracks/")
        
        return {
            "quota_limit_gb": quota.limit_gb,
            "used_gb": quota.used_gb,
            "available_gb": quota.available_gb,
            "usage_percent": quota.usage_percent,
            "total_files": len(files),
            "is_exceeded": quota.is_exceeded(),
        }

