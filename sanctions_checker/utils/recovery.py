"""
Recovery mechanisms for critical operations in the Sanctions Checker application.
"""

import logging
import time
import asyncio
from typing import Callable, Any, Optional, Dict, List
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta

from .error_handler import SanctionsCheckerError, ErrorCategory, ErrorSeverity
from .logger import get_logger


class RecoveryStrategy(Enum):
    """Recovery strategy types."""
    RETRY = "retry"
    FALLBACK = "fallback"
    CIRCUIT_BREAKER = "circuit_breaker"
    GRACEFUL_DEGRADATION = "graceful_degradation"


@dataclass
class RetryConfig:
    """Configuration for retry operations."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker pattern."""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    success_threshold: int = 3


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker implementation for preventing cascading failures."""
    
    def __init__(self, config: CircuitBreakerConfig, name: str = "default"):
        self.config = config
        self.name = name
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.logger = get_logger(f"{__name__}.CircuitBreaker.{name}")
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitBreakerState.HALF_OPEN
                self.logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
            else:
                raise SanctionsCheckerError(
                    f"Circuit breaker {self.name} is OPEN",
                    category=ErrorCategory.SYSTEM,
                    severity=ErrorSeverity.HIGH,
                    recoverable=False
                )
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if circuit breaker should attempt to reset."""
        if self.last_failure_time is None:
            return True
        
        time_since_failure = datetime.now() - self.last_failure_time
        return time_since_failure.total_seconds() >= self.config.recovery_timeout
    
    def _on_success(self):
        """Handle successful operation."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                self.logger.info(f"Circuit breaker {self.name} reset to CLOSED")
        else:
            self.failure_count = 0
    
    def _on_failure(self):
        """Handle failed operation."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            self.success_count = 0
            self.logger.warning(f"Circuit breaker {self.name} opened due to failure in HALF_OPEN state")
        elif self.failure_count >= self.config.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            self.logger.warning(f"Circuit breaker {self.name} opened due to {self.failure_count} failures")


class RecoveryManager:
    """Manages recovery strategies for critical operations."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.fallback_handlers: Dict[str, Callable] = {}
    
    def register_circuit_breaker(
        self,
        name: str,
        config: CircuitBreakerConfig
    ) -> CircuitBreaker:
        """Register a circuit breaker for a specific operation."""
        circuit_breaker = CircuitBreaker(config, name)
        self.circuit_breakers[name] = circuit_breaker
        return circuit_breaker
    
    def register_fallback(self, operation: str, fallback_func: Callable):
        """Register a fallback function for an operation."""
        self.fallback_handlers[operation] = fallback_func
    
    def execute_with_retry(
        self,
        func: Callable,
        config: RetryConfig,
        operation_name: str = "unknown",
        *args,
        **kwargs
    ) -> Any:
        """Execute function with retry logic."""
        last_exception = None
        
        for attempt in range(config.max_attempts):
            try:
                if attempt > 0:
                    delay = self._calculate_delay(attempt, config)
                    self.logger.info(f"Retrying {operation_name} (attempt {attempt + 1}/{config.max_attempts}) after {delay:.2f}s delay")
                    time.sleep(delay)
                
                result = func(*args, **kwargs)
                if attempt > 0:
                    self.logger.info(f"{operation_name} succeeded on attempt {attempt + 1}")
                return result
                
            except Exception as e:
                last_exception = e
                self.logger.warning(f"{operation_name} failed on attempt {attempt + 1}: {e}")
                
                # Don't retry for certain error types
                if isinstance(e, SanctionsCheckerError) and not e.recoverable:
                    break
        
        # All retries exhausted
        self.logger.error(f"{operation_name} failed after {config.max_attempts} attempts")
        if last_exception:
            raise last_exception
    
    def execute_with_circuit_breaker(
        self,
        func: Callable,
        circuit_breaker_name: str,
        *args,
        **kwargs
    ) -> Any:
        """Execute function with circuit breaker protection."""
        if circuit_breaker_name not in self.circuit_breakers:
            raise ValueError(f"Circuit breaker '{circuit_breaker_name}' not registered")
        
        circuit_breaker = self.circuit_breakers[circuit_breaker_name]
        return circuit_breaker.call(func, *args, **kwargs)
    
    def execute_with_fallback(
        self,
        func: Callable,
        operation_name: str,
        *args,
        **kwargs
    ) -> Any:
        """Execute function with fallback on failure."""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.logger.warning(f"{operation_name} failed, attempting fallback: {e}")
            
            if operation_name in self.fallback_handlers:
                try:
                    fallback_result = self.fallback_handlers[operation_name](*args, **kwargs)
                    self.logger.info(f"Fallback for {operation_name} succeeded")
                    return fallback_result
                except Exception as fallback_error:
                    self.logger.error(f"Fallback for {operation_name} also failed: {fallback_error}")
                    raise fallback_error
            else:
                self.logger.error(f"No fallback registered for {operation_name}")
                raise e
    
    def execute_with_graceful_degradation(
        self,
        func: Callable,
        degraded_func: Callable,
        operation_name: str,
        *args,
        **kwargs
    ) -> Any:
        """Execute function with graceful degradation on failure."""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.logger.warning(f"{operation_name} failed, using degraded functionality: {e}")
            try:
                result = degraded_func(*args, **kwargs)
                self.logger.info(f"Degraded {operation_name} succeeded")
                return result
            except Exception as degraded_error:
                self.logger.error(f"Degraded {operation_name} also failed: {degraded_error}")
                raise degraded_error
    
    def _calculate_delay(self, attempt: int, config: RetryConfig) -> float:
        """Calculate delay for retry attempt."""
        # For attempt 1, use base_delay directly (no exponential scaling)
        if attempt == 1:
            delay = config.base_delay
        else:
            delay = config.base_delay * (config.exponential_base ** (attempt - 1))
        
        delay = min(delay, config.max_delay)
        
        if config.jitter:
            import random
            delay *= (0.5 + random.random() * 0.5)  # Add 0-50% jitter
        
        return delay
    
    def get_circuit_breaker_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all circuit breakers."""
        status = {}
        for name, cb in self.circuit_breakers.items():
            status[name] = {
                "state": cb.state.value,
                "failure_count": cb.failure_count,
                "success_count": cb.success_count,
                "last_failure_time": cb.last_failure_time.isoformat() if cb.last_failure_time else None
            }
        return status


# Decorator functions for easy use
def with_retry(config: RetryConfig, operation_name: str = None):
    """Decorator for automatic retry functionality."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            recovery_manager = RecoveryManager()
            name = operation_name or f"{func.__module__}.{func.__name__}"
            return recovery_manager.execute_with_retry(func, config, name, *args, **kwargs)
        return wrapper
    return decorator


def with_circuit_breaker(circuit_breaker_name: str, config: CircuitBreakerConfig = None):
    """Decorator for circuit breaker functionality."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            recovery_manager = RecoveryManager()
            if config and circuit_breaker_name not in recovery_manager.circuit_breakers:
                recovery_manager.register_circuit_breaker(circuit_breaker_name, config)
            return recovery_manager.execute_with_circuit_breaker(func, circuit_breaker_name, *args, **kwargs)
        return wrapper
    return decorator


def with_fallback(fallback_func: Callable, operation_name: str = None):
    """Decorator for fallback functionality."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            recovery_manager = RecoveryManager()
            name = operation_name or f"{func.__module__}.{func.__name__}"
            recovery_manager.register_fallback(name, fallback_func)
            return recovery_manager.execute_with_fallback(func, name, *args, **kwargs)
        return wrapper
    return decorator