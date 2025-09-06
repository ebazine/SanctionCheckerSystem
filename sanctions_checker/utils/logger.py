"""
Comprehensive logging system for the Sanctions Checker application.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


class SanctionsCheckerFormatter(logging.Formatter):
    """Custom formatter for sanctions checker logs."""
    
    def __init__(self):
        super().__init__()
        self.default_format = '[%(asctime)s] %(levelname)-8s [%(name)s:%(lineno)d] %(message)s'
        self.error_format = '[%(asctime)s] %(levelname)-8s [%(name)s:%(lineno)d] %(message)s\nException: %(exc_info)s'
    
    def format(self, record):
        if record.levelno >= logging.ERROR and record.exc_info:
            formatter = logging.Formatter(self.error_format)
        else:
            formatter = logging.Formatter(self.default_format)
        return formatter.format(record)


def setup_logging(
    log_level: str = "INFO",
    log_dir: Optional[Path] = None,
    enable_console: bool = True,
    enable_file: bool = True,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> None:
    """
    Set up comprehensive logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
        enable_console: Whether to enable console logging
        enable_file: Whether to enable file logging
        max_file_size: Maximum size of log files before rotation
        backup_count: Number of backup log files to keep
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Clear existing handlers and close them properly
    for handler in root_logger.handlers[:]:
        handler.close()
        root_logger.removeHandler(handler)
    
    # Clear audit logger handlers too
    audit_logger = logging.getLogger('audit')
    for handler in audit_logger.handlers[:]:
        handler.close()
        audit_logger.removeHandler(handler)
    
    # Create formatter
    formatter = SanctionsCheckerFormatter()
    
    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # File handlers
    if enable_file and log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Main application log
        app_log_file = log_dir / "sanctions_checker.log"
        app_handler = logging.handlers.RotatingFileHandler(
            app_log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        app_handler.setLevel(numeric_level)
        app_handler.setFormatter(formatter)
        root_logger.addHandler(app_handler)
        
        # Error-only log
        error_log_file = log_dir / "errors.log"
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        root_logger.addHandler(error_handler)
        
        # Audit log for compliance tracking
        audit_log_file = log_dir / "audit.log"
        audit_handler = logging.handlers.RotatingFileHandler(
            audit_log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        audit_handler.setLevel(logging.INFO)
        audit_handler.setFormatter(formatter)
        
        # Create audit logger
        audit_logger.addHandler(audit_handler)
        audit_logger.setLevel(logging.INFO)
        audit_logger.propagate = False


def cleanup_logging():
    """Clean up logging handlers to prevent file locks."""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        handler.close()
        root_logger.removeHandler(handler)
    
    audit_logger = logging.getLogger('audit')
    for handler in audit_logger.handlers[:]:
        handler.close()
        audit_logger.removeHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the specified module.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def get_audit_logger() -> logging.Logger:
    """
    Get the audit logger for compliance tracking.
    
    Returns:
        Audit logger instance
    """
    return logging.getLogger('audit')


class LogContext:
    """Context manager for adding contextual information to logs."""
    
    def __init__(self, logger: logging.Logger, **context):
        self.logger = logger
        self.context = context
        self.old_factory = None
    
    def __enter__(self):
        self.old_factory = logging.getLogRecordFactory()
        
        def record_factory(*args, **kwargs):
            record = self.old_factory(*args, **kwargs)
            for key, value in self.context.items():
                setattr(record, key, value)
            return record
        
        logging.setLogRecordFactory(record_factory)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.setLogRecordFactory(self.old_factory)


def log_function_call(logger: logging.Logger):
    """Decorator to log function calls with parameters and results."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            func_name = f"{func.__module__}.{func.__name__}"
            logger.debug(f"Calling {func_name} with args={args}, kwargs={kwargs}")
            
            try:
                result = func(*args, **kwargs)
                logger.debug(f"{func_name} completed successfully")
                return result
            except Exception as e:
                logger.error(f"{func_name} failed with error: {e}", exc_info=True)
                raise
        
        return wrapper
    return decorator


def log_performance(logger: logging.Logger, operation_name: str):
    """Decorator to log performance metrics for operations."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            logger.info(f"Starting {operation_name}")
            
            try:
                result = func(*args, **kwargs)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"{operation_name} completed in {duration:.2f} seconds")
                return result
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                logger.error(f"{operation_name} failed after {duration:.2f} seconds: {e}")
                raise
        
        return wrapper
    return decorator