"""Circuit breaker implementation for external API calls.

The circuit breaker pattern prevents cascading failures when external services
are unavailable or responding slowly. It has three states:

- CLOSED: Normal operation, requests pass through
- OPEN: Circuit is tripped, requests fail immediately  
- HALF_OPEN: Testing if service has recovered
"""
import time
import threading
from enum import Enum
from typing import Callable, Any, Optional, Type
from dataclasses import dataclass
import structlog

logger = structlog.get_logger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open."""
    
    def __init__(self, name: str, reset_time: float):
        self.name = name
        self.reset_time = reset_time
        super().__init__(f"Circuit breaker '{name}' is open. Retry after {reset_time:.1f}s")


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5  # Number of failures before opening
    success_threshold: int = 2  # Number of successes to close from half-open
    timeout: float = 30.0  # Seconds before trying half-open
    excluded_exceptions: tuple = ()  # Exceptions that don't count as failures


class CircuitBreaker:
    """Circuit breaker for protecting external API calls.
    
    Usage:
        cb = CircuitBreaker("youtube_api")
        
        try:
            result = cb.call(youtube_client.search, query="music")
        except CircuitBreakerOpen as e:
            # Service is down, use fallback or fail gracefully
            pass
    """
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 30.0,
        excluded_exceptions: tuple = (),
    ):
        self.name = name
        self.config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            success_threshold=success_threshold,
            timeout=timeout,
            excluded_exceptions=excluded_exceptions,
        )
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()
        
        logger.info(
            "circuit_breaker_initialized",
            name=name,
            failure_threshold=failure_threshold,
            timeout=timeout,
        )
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                # Check if timeout has passed
                if self._should_try_reset():
                    self._state = CircuitState.HALF_OPEN
                    logger.info("circuit_breaker_half_open", name=self.name)
            return self._state
    
    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (normal operation)."""
        return self.state == CircuitState.CLOSED
    
    @property
    def is_open(self) -> bool:
        """Check if circuit is open (failing fast)."""
        return self.state == CircuitState.OPEN
    
    def _should_try_reset(self) -> bool:
        """Check if enough time has passed to try resetting."""
        if self._last_failure_time is None:
            return False
        return time.time() - self._last_failure_time >= self.config.timeout
    
    def _record_failure(self, error: Exception) -> None:
        """Record a failure and potentially open the circuit."""
        with self._lock:
            self._failure_count += 1
            self._success_count = 0
            self._last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                # Failed during recovery, reopen
                self._state = CircuitState.OPEN
                logger.warning(
                    "circuit_breaker_reopened",
                    name=self.name,
                    error=str(error),
                )
            elif self._failure_count >= self.config.failure_threshold:
                # Too many failures, open the circuit
                self._state = CircuitState.OPEN
                logger.warning(
                    "circuit_breaker_opened",
                    name=self.name,
                    failure_count=self._failure_count,
                    error=str(error),
                )
    
    def _record_success(self) -> None:
        """Record a success and potentially close the circuit."""
        with self._lock:
            self._success_count += 1
            
            if self._state == CircuitState.HALF_OPEN:
                if self._success_count >= self.config.success_threshold:
                    # Enough successes, close the circuit
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    logger.info("circuit_breaker_closed", name=self.name)
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0
    
    def call(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Positional arguments for function
            **kwargs: Keyword arguments for function
            
        Returns:
            Result of function call
            
        Raises:
            CircuitBreakerOpen: If circuit is open
            Exception: Original exception if call fails
        """
        current_state = self.state
        
        if current_state == CircuitState.OPEN:
            time_until_reset = (
                self._last_failure_time + self.config.timeout - time.time()
                if self._last_failure_time else 0
            )
            raise CircuitBreakerOpen(self.name, max(0, time_until_reset))
        
        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except self.config.excluded_exceptions:
            # Don't count excluded exceptions as failures
            raise
        except Exception as e:
            self._record_failure(e)
            raise
    
    async def call_async(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Execute async function with circuit breaker protection.
        
        Args:
            func: Async function to execute
            *args: Positional arguments for function
            **kwargs: Keyword arguments for function
            
        Returns:
            Result of async function call
            
        Raises:
            CircuitBreakerOpen: If circuit is open
            Exception: Original exception if call fails
        """
        current_state = self.state
        
        if current_state == CircuitState.OPEN:
            time_until_reset = (
                self._last_failure_time + self.config.timeout - time.time()
                if self._last_failure_time else 0
            )
            raise CircuitBreakerOpen(self.name, max(0, time_until_reset))
        
        try:
            result = await func(*args, **kwargs)
            self._record_success()
            return result
        except self.config.excluded_exceptions:
            # Don't count excluded exceptions as failures
            raise
        except Exception as e:
            self._record_failure(e)
            raise
    
    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            logger.info("circuit_breaker_reset", name=self.name)
    
    def get_stats(self) -> dict:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
                "failure_threshold": self.config.failure_threshold,
                "timeout": self.config.timeout,
            }

