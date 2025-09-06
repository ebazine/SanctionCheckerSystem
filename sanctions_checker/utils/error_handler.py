"""
Comprehensive error handling system for the Sanctions Checker application.
"""

import logging
import traceback
import sys
from typing import Optional, Dict, Any, Callable, Type
from enum import Enum
from dataclasses import dataclass
from datetime import datetime


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories for classification."""
    NETWORK = "network"
    DATABASE = "database"
    DATA_PARSING = "data_parsing"
    VALIDATION = "validation"
    AUTHENTICATION = "authentication"
    PERMISSION = "permission"
    CONFIGURATION = "configuration"
    SYSTEM = "system"
    USER_INPUT = "user_input"
    BUSINESS_LOGIC = "business_logic"


@dataclass
class ErrorContext:
    """Context information for errors."""
    user_id: Optional[str] = None
    operation: Optional[str] = None
    component: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class SanctionsCheckerError(Exception):
    """Base exception class for Sanctions Checker application."""
    
    def __init__(
        self,
        message: str,
        category: ErrorCategory = ErrorCategory.SYSTEM,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        user_message: Optional[str] = None,
        context: Optional[ErrorContext] = None,
        recoverable: bool = True,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.category = category
        self.severity = severity
        self.user_message = user_message or self._generate_user_message()
        self.context = context or ErrorContext()
        self.recoverable = recoverable
        self.original_exception = original_exception
        self.error_id = self._generate_error_id()
    
    def _generate_user_message(self) -> str:
        """Generate user-friendly error message."""
        category_messages = {
            ErrorCategory.NETWORK: "Network connection issue. Please check your internet connection and try again.",
            ErrorCategory.DATABASE: "Database error occurred. The operation could not be completed.",
            ErrorCategory.DATA_PARSING: "Error processing data. The data format may be invalid.",
            ErrorCategory.VALIDATION: "Invalid input provided. Please check your input and try again.",
            ErrorCategory.AUTHENTICATION: "Authentication failed. Please check your credentials.",
            ErrorCategory.PERMISSION: "Permission denied. You don't have access to perform this operation.",
            ErrorCategory.CONFIGURATION: "Configuration error. Please check your settings.",
            ErrorCategory.SYSTEM: "System error occurred. Please try again later.",
            ErrorCategory.USER_INPUT: "Invalid input provided. Please correct the input and try again.",
            ErrorCategory.BUSINESS_LOGIC: "Operation could not be completed due to business rules."
        }
        return category_messages.get(self.category, "An unexpected error occurred.")
    
    def _generate_error_id(self) -> str:
        """Generate unique error ID for tracking."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"SC_{self.category.value.upper()}_{timestamp}_{id(self) % 10000:04d}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for logging/serialization."""
        return {
            "error_id": self.error_id,
            "message": self.message,
            "user_message": self.user_message,
            "category": self.category.value,
            "severity": self.severity.value,
            "recoverable": self.recoverable,
            "timestamp": self.context.timestamp.isoformat(),
            "context": {
                "user_id": self.context.user_id,
                "operation": self.context.operation,
                "component": self.context.component,
                "additional_data": self.context.additional_data
            },
            "original_exception": str(self.original_exception) if self.original_exception else None,
            "traceback": traceback.format_exc() if self.original_exception else None
        }


class NetworkError(SanctionsCheckerError):
    """Network-related errors."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )


class DatabaseError(SanctionsCheckerError):
    """Database-related errors."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.DATABASE,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )


class DataParsingError(SanctionsCheckerError):
    """Data parsing errors."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.DATA_PARSING,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )


class ValidationError(SanctionsCheckerError):
    """Input validation errors."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.LOW,
            **kwargs
        )


class ConfigurationError(SanctionsCheckerError):
    """Configuration-related errors."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )


class ErrorHandler:
    """Central error handling system."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.error_callbacks: Dict[ErrorCategory, list] = {}
        self.global_callbacks: list = []
    
    def register_callback(
        self,
        callback: Callable[[SanctionsCheckerError], None],
        category: Optional[ErrorCategory] = None
    ):
        """Register error callback for specific category or globally."""
        if category:
            if category not in self.error_callbacks:
                self.error_callbacks[category] = []
            self.error_callbacks[category].append(callback)
        else:
            self.global_callbacks.append(callback)
    
    def handle_error(
        self,
        error: Exception,
        context: Optional[ErrorContext] = None,
        user_message: Optional[str] = None
    ) -> SanctionsCheckerError:
        """
        Handle an error with comprehensive logging and user notification.
        
        Args:
            error: The original exception
            context: Error context information
            user_message: Custom user-friendly message
            
        Returns:
            SanctionsCheckerError instance
        """
        # Convert to SanctionsCheckerError if needed
        if isinstance(error, SanctionsCheckerError):
            sc_error = error
        else:
            sc_error = self._convert_exception(error, context, user_message)
        
        # Log the error
        self._log_error(sc_error)
        
        # Execute callbacks
        self._execute_callbacks(sc_error)
        
        return sc_error
    
    def _convert_exception(
        self,
        error: Exception,
        context: Optional[ErrorContext] = None,
        user_message: Optional[str] = None
    ) -> SanctionsCheckerError:
        """Convert standard exception to SanctionsCheckerError."""
        error_type = type(error).__name__
        message = str(error)
        
        # Determine category and severity based on exception type
        category_mapping = {
            'ConnectionError': ErrorCategory.NETWORK,
            'TimeoutError': ErrorCategory.NETWORK,
            'HTTPError': ErrorCategory.NETWORK,
            'URLError': ErrorCategory.NETWORK,
            'SQLAlchemyError': ErrorCategory.DATABASE,
            'IntegrityError': ErrorCategory.DATABASE,
            'OperationalError': ErrorCategory.DATABASE,
            'JSONDecodeError': ErrorCategory.DATA_PARSING,
            'XMLSyntaxError': ErrorCategory.DATA_PARSING,
            'ValueError': ErrorCategory.VALIDATION,
            'TypeError': ErrorCategory.VALIDATION,
            'FileNotFoundError': ErrorCategory.SYSTEM,
            'PermissionError': ErrorCategory.PERMISSION,
            'KeyError': ErrorCategory.CONFIGURATION,
            'AttributeError': ErrorCategory.CONFIGURATION
        }
        
        severity_mapping = {
            ErrorCategory.NETWORK: ErrorSeverity.MEDIUM,
            ErrorCategory.DATABASE: ErrorSeverity.HIGH,
            ErrorCategory.DATA_PARSING: ErrorSeverity.MEDIUM,
            ErrorCategory.VALIDATION: ErrorSeverity.LOW,
            ErrorCategory.SYSTEM: ErrorSeverity.HIGH,
            ErrorCategory.PERMISSION: ErrorSeverity.HIGH,
            ErrorCategory.CONFIGURATION: ErrorSeverity.HIGH
        }
        
        category = category_mapping.get(error_type, ErrorCategory.SYSTEM)
        severity = severity_mapping.get(category, ErrorSeverity.MEDIUM)
        
        return SanctionsCheckerError(
            message=f"{error_type}: {message}",
            category=category,
            severity=severity,
            user_message=user_message,
            context=context,
            original_exception=error
        )
    
    def _log_error(self, error: SanctionsCheckerError):
        """Log error with appropriate level."""
        error_dict = error.to_dict()
        # Remove 'message' key to avoid conflict with logging's message attribute
        error_dict.pop('message', None)
        
        if error.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(f"CRITICAL ERROR [{error.error_id}]: {error.message}", extra=error_dict)
        elif error.severity == ErrorSeverity.HIGH:
            self.logger.error(f"ERROR [{error.error_id}]: {error.message}", extra=error_dict)
        elif error.severity == ErrorSeverity.MEDIUM:
            self.logger.warning(f"WARNING [{error.error_id}]: {error.message}", extra=error_dict)
        else:
            self.logger.info(f"INFO [{error.error_id}]: {error.message}", extra=error_dict)
    
    def _execute_callbacks(self, error: SanctionsCheckerError):
        """Execute registered callbacks for the error."""
        # Execute category-specific callbacks
        if error.category in self.error_callbacks:
            for callback in self.error_callbacks[error.category]:
                try:
                    callback(error)
                except Exception as e:
                    self.logger.error(f"Error in callback execution: {e}")
        
        # Execute global callbacks
        for callback in self.global_callbacks:
            try:
                callback(error)
            except Exception as e:
                self.logger.error(f"Error in global callback execution: {e}")


def handle_exceptions(
    error_handler: ErrorHandler,
    context: Optional[ErrorContext] = None,
    reraise: bool = True
):
    """Decorator for automatic exception handling."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                handled_error = error_handler.handle_error(e, context)
                if reraise:
                    raise handled_error
                return None
        return wrapper
    return decorator


def setup_global_exception_handler(error_handler: ErrorHandler):
    """Set up global exception handler for unhandled exceptions."""
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            # Allow keyboard interrupt to work normally
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        context = ErrorContext(
            operation="global_exception_handler",
            component="system"
        )
        
        error_handler.handle_error(exc_value, context)
    
    sys.excepthook = handle_exception