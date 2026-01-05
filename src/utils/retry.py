"""Retry logic with exponential backoff for external API calls.

Provides decorators and utilities for retrying failed operations with
configurable backoff strategies.
"""
import asyncio
import time
import random
from typing import Callable, Any, Optional, Type, Tuple, Union
from dataclasses import dataclass, field
from functools import wraps
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 60.0  # Maximum delay cap
    exponential_base: float = 2.0  # Multiplier for exponential backoff
    jitter: bool = True  # Add randomness to prevent thundering herd
    retryable_exceptions: Tuple[Type[Exception], ...] = field(
        default_factory=lambda: (Exception,)
    )
    non_retryable_exceptions: Tuple[Type[Exception], ...] = field(
        default_factory=tuple
    )


def calculate_delay(
    attempt: int,
    base_delay: float,
    max_delay: float,
    exponential_base: float,
    jitter: bool,
) -> float:
    """Calculate delay for retry attempt using exponential backoff.
    
    Args:
        attempt: Current attempt number (0-based)
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap
        exponential_base: Multiplier for exponential backoff
        jitter: Whether to add randomness
        
    Returns:
        Delay in seconds
    """
    delay = base_delay * (exponential_base ** attempt)
    delay = min(delay, max_delay)
    
    if jitter:
        # Add up to 25% jitter
        jitter_range = delay * 0.25
        delay = delay + random.uniform(-jitter_range, jitter_range)
        delay = max(0, delay)  # Ensure non-negative
    
    return delay


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    non_retryable_exceptions: Tuple[Type[Exception], ...] = (),
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable:
    """Decorator for retrying functions with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap
        exponential_base: Multiplier for exponential backoff  
        jitter: Add randomness to prevent thundering herd
        retryable_exceptions: Tuple of exceptions to retry on
        non_retryable_exceptions: Tuple of exceptions to never retry
        on_retry: Optional callback called on each retry (exception, attempt)
        
    Returns:
        Decorated function
        
    Usage:
        @retry_with_backoff(max_retries=3, base_delay=1.0)
        def fetch_data():
            return api.get("/data")
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            last_exception: Optional[Exception] = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except non_retryable_exceptions as e:
                    # Never retry these
                    logger.warning(
                        "retry_non_retryable_exception",
                        function=func.__name__,
                        exception=str(e),
                        exception_type=type(e).__name__,
                    )
                    raise
                except retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        # Last attempt failed
                        logger.error(
                            "retry_exhausted",
                            function=func.__name__,
                            attempts=attempt + 1,
                            exception=str(e),
                        )
                        raise
                    
                    delay = calculate_delay(
                        attempt, base_delay, max_delay, exponential_base, jitter
                    )
                    
                    logger.warning(
                        "retry_attempt",
                        function=func.__name__,
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        delay=delay,
                        exception=str(e),
                    )
                    
                    if on_retry:
                        on_retry(e, attempt)
                    
                    time.sleep(delay)
            
            # Should not reach here, but just in case
            if last_exception:
                raise last_exception
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            last_exception: Optional[Exception] = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except non_retryable_exceptions as e:
                    # Never retry these
                    logger.warning(
                        "retry_non_retryable_exception",
                        function=func.__name__,
                        exception=str(e),
                        exception_type=type(e).__name__,
                    )
                    raise
                except retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        # Last attempt failed
                        logger.error(
                            "retry_exhausted",
                            function=func.__name__,
                            attempts=attempt + 1,
                            exception=str(e),
                        )
                        raise
                    
                    delay = calculate_delay(
                        attempt, base_delay, max_delay, exponential_base, jitter
                    )
                    
                    logger.warning(
                        "retry_attempt",
                        function=func.__name__,
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        delay=delay,
                        exception=str(e),
                    )
                    
                    if on_retry:
                        on_retry(e, attempt)
                    
                    await asyncio.sleep(delay)
            
            # Should not reach here, but just in case
            if last_exception:
                raise last_exception
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


class RetryHandler:
    """Stateful retry handler for more complex retry scenarios.
    
    Useful when you need to track retry state across multiple operations
    or implement custom retry policies.
    
    Usage:
        handler = RetryHandler(RetryConfig(max_retries=3))
        
        async def fetch_with_retry():
            async for attempt in handler.attempts():
                with attempt:
                    return await api.fetch()
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self._attempt = 0
        self._last_exception: Optional[Exception] = None
    
    @property
    def attempt(self) -> int:
        """Current attempt number (0-based)."""
        return self._attempt
    
    @property
    def last_exception(self) -> Optional[Exception]:
        """Last exception encountered."""
        return self._last_exception
    
    def should_retry(self, exception: Exception) -> bool:
        """Check if operation should be retried.
        
        Args:
            exception: The exception that occurred
            
        Returns:
            True if should retry, False otherwise
        """
        if isinstance(exception, self.config.non_retryable_exceptions):
            return False
        
        if not isinstance(exception, self.config.retryable_exceptions):
            return False
        
        return self._attempt < self.config.max_retries
    
    def get_delay(self) -> float:
        """Get delay for current attempt."""
        return calculate_delay(
            self._attempt,
            self.config.base_delay,
            self.config.max_delay,
            self.config.exponential_base,
            self.config.jitter,
        )
    
    async def execute(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Execute function with retry logic.
        
        Args:
            func: Async function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Result of function
            
        Raises:
            Exception: If all retries exhausted
        """
        self._attempt = 0
        self._last_exception = None
        
        while True:
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
            except Exception as e:
                self._last_exception = e
                
                if not self.should_retry(e):
                    raise
                
                delay = self.get_delay()
                logger.warning(
                    "retry_handler_attempt",
                    attempt=self._attempt + 1,
                    max_retries=self.config.max_retries,
                    delay=delay,
                    exception=str(e),
                )
                
                self._attempt += 1
                
                if asyncio.iscoroutinefunction(func):
                    await asyncio.sleep(delay)
                else:
                    time.sleep(delay)
    
    def reset(self) -> None:
        """Reset retry state."""
        self._attempt = 0
        self._last_exception = None

