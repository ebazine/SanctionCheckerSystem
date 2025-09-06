"""
Health monitoring service for data sources and system components.
"""
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from enum import Enum
import requests

from ..config import Config
from .notification_service import get_notification_service, NotificationLevel, NotificationType

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ComponentType(Enum):
    """Types of system components to monitor."""
    DATA_SOURCE = "data_source"
    DATABASE = "database"
    SCHEDULER = "scheduler"
    NETWORK = "network"


class HealthCheck:
    """Represents a health check result."""
    
    def __init__(self, component_name: str, component_type: ComponentType,
                 status: HealthStatus, message: str, timestamp: datetime = None,
                 details: Dict[str, Any] = None, response_time_ms: float = None):
        self.component_name = component_name
        self.component_type = component_type
        self.status = status
        self.message = message
        self.timestamp = timestamp or datetime.now()
        self.details = details or {}
        self.response_time_ms = response_time_ms
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert health check to dictionary."""
        return {
            'component_name': self.component_name,
            'component_type': self.component_type.value,
            'status': self.status.value,
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'details': self.details,
            'response_time_ms': self.response_time_ms
        }


class HealthMonitor:
    """
    Service for monitoring the health of system components.
    
    Features:
    - Periodic health checks for data sources
    - Database connectivity monitoring
    - Network connectivity checks
    - Automatic alerting on health degradation
    - Historical health data tracking
    """
    
    def __init__(self, config: Config = None):
        """
        Initialize the health monitor.
        
        Args:
            config: Configuration instance
        """
        self.config = config or Config()
        self.notification_service = get_notification_service()
        
        # Monitoring state
        self._running = False
        self._monitor_thread = None
        self._health_history = {}
        self._current_status = {}
        self._check_callbacks = []
        self._lock = threading.RLock()
        
        # Configuration
        self.check_interval_minutes = self.config.get('health_monitor.check_interval_minutes', 15)
        self.timeout_seconds = self.config.get('health_monitor.timeout_seconds', 10)
        self.max_history_entries = self.config.get('health_monitor.max_history_entries', 100)
        
        # Data sources to monitor
        self.data_sources = self.config.get('data_sources', {})
        
        # Initialize component status
        self._initialize_component_status()
    
    def _initialize_component_status(self):
        """Initialize status tracking for all components."""
        with self._lock:
            # Initialize data sources
            for source_name in self.data_sources:
                self._current_status[source_name] = HealthStatus.UNKNOWN
                self._health_history[source_name] = []
            
            # Initialize system components
            system_components = ['database', 'scheduler', 'network']
            for component in system_components:
                self._current_status[component] = HealthStatus.UNKNOWN
                self._health_history[component] = []
    
    def start_monitoring(self):
        """Start the health monitoring service."""
        if self._running:
            logger.warning("Health monitor is already running")
            return
        
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        
        logger.info(f"Health monitor started with {self.check_interval_minutes}min interval")
        self.notification_service.add_notification(
            "Health Monitor Started",
            f"System health monitoring started with {self.check_interval_minutes} minute intervals",
            NotificationLevel.INFO,
            NotificationType.SYSTEM_STATUS,
            source="HEALTH_MONITOR"
        )
    
    def stop_monitoring(self):
        """Stop the health monitoring service."""
        if not self._running:
            return
        
        self._running = False
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)
        
        logger.info("Health monitor stopped")
        self.notification_service.add_notification(
            "Health Monitor Stopped",
            "System health monitoring has been stopped",
            NotificationLevel.INFO,
            NotificationType.SYSTEM_STATUS,
            source="HEALTH_MONITOR"
        )
    
    def _monitor_loop(self):
        """Main monitoring loop running in background thread."""
        logger.info("Health monitor loop started")
        
        while self._running:
            try:
                # Perform all health checks
                self._perform_all_checks()
                
                # Sleep for the configured interval
                time.sleep(self.check_interval_minutes * 60)
                
            except Exception as e:
                logger.error(f"Error in health monitor loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def _perform_all_checks(self):
        """Perform health checks for all monitored components."""
        logger.debug("Performing health checks")
        
        # Check data sources
        for source_name in self.data_sources:
            self._check_data_source_health(source_name)
        
        # Check database
        self._check_database_health()
        
        # Check network connectivity
        self._check_network_health()
        
        # Notify callbacks
        self._notify_check_callbacks()
    
    def _check_data_source_health(self, source_name: str):
        """Check health of a specific data source."""
        source_config = self.data_sources.get(source_name, {})
        url = source_config.get('url')
        
        if not url:
            self._record_health_check(
                source_name, ComponentType.DATA_SOURCE, HealthStatus.UNKNOWN,
                "No URL configured for data source"
            )
            return
        
        start_time = time.time()
        
        try:
            # Perform HEAD request to check if source is accessible
            response = requests.head(url, timeout=self.timeout_seconds, allow_redirects=True)
            response_time_ms = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                status = HealthStatus.HEALTHY
                message = f"Data source is accessible (HTTP {response.status_code})"
            elif 400 <= response.status_code < 500:
                status = HealthStatus.DEGRADED
                message = f"Data source returned client error (HTTP {response.status_code})"
            else:
                status = HealthStatus.UNHEALTHY
                message = f"Data source returned server error (HTTP {response.status_code})"
            
            details = {
                'status_code': response.status_code,
                'headers': dict(response.headers),
                'url': url
            }
            
        except requests.exceptions.Timeout:
            response_time_ms = self.timeout_seconds * 1000
            status = HealthStatus.UNHEALTHY
            message = f"Data source request timed out after {self.timeout_seconds}s"
            details = {'error': 'timeout', 'url': url}
            
        except requests.exceptions.ConnectionError:
            response_time_ms = (time.time() - start_time) * 1000
            status = HealthStatus.UNHEALTHY
            message = "Cannot connect to data source"
            details = {'error': 'connection_error', 'url': url}
            
        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            status = HealthStatus.UNHEALTHY
            message = f"Data source check failed: {str(e)}"
            details = {'error': str(e), 'url': url}
        
        self._record_health_check(
            source_name, ComponentType.DATA_SOURCE, status, message,
            details=details, response_time_ms=response_time_ms
        )
    
    def _check_database_health(self):
        """Check database connectivity and health."""
        try:
            from ..database.manager import DatabaseManager
            
            start_time = time.time()
            db_manager = DatabaseManager()
            
            # Try to get a database session
            session = db_manager.get_session()
            
            # Perform a simple query
            session.execute("SELECT 1")
            
            db_manager.close_session(session)
            response_time_ms = (time.time() - start_time) * 1000
            
            status = HealthStatus.HEALTHY
            message = "Database is accessible and responsive"
            details = {'response_time_ms': response_time_ms}
            
        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            status = HealthStatus.UNHEALTHY
            message = f"Database check failed: {str(e)}"
            details = {'error': str(e)}
        
        self._record_health_check(
            'database', ComponentType.DATABASE, status, message,
            details=details, response_time_ms=response_time_ms
        )
    
    def _check_network_health(self):
        """Check general network connectivity."""
        # Test connectivity to a reliable external service
        test_urls = [
            'https://www.google.com',
            'https://www.cloudflare.com',
            'https://httpbin.org/status/200'
        ]
        
        successful_checks = 0
        total_response_time = 0
        
        for url in test_urls:
            try:
                start_time = time.time()
                response = requests.head(url, timeout=self.timeout_seconds)
                response_time = (time.time() - start_time) * 1000
                
                if response.status_code == 200:
                    successful_checks += 1
                    total_response_time += response_time
                    
            except Exception:
                continue
        
        if successful_checks == len(test_urls):
            status = HealthStatus.HEALTHY
            message = "Network connectivity is good"
            avg_response_time = total_response_time / successful_checks
        elif successful_checks > 0:
            status = HealthStatus.DEGRADED
            message = f"Partial network connectivity ({successful_checks}/{len(test_urls)} checks passed)"
            avg_response_time = total_response_time / successful_checks if successful_checks > 0 else None
        else:
            status = HealthStatus.UNHEALTHY
            message = "No network connectivity detected"
            avg_response_time = None
        
        details = {
            'successful_checks': successful_checks,
            'total_checks': len(test_urls),
            'test_urls': test_urls
        }
        
        self._record_health_check(
            'network', ComponentType.NETWORK, status, message,
            details=details, response_time_ms=avg_response_time
        )
    
    def _record_health_check(self, component_name: str, component_type: ComponentType,
                           status: HealthStatus, message: str, details: Dict[str, Any] = None,
                           response_time_ms: float = None):
        """Record a health check result."""
        health_check = HealthCheck(
            component_name, component_type, status, message,
            details=details, response_time_ms=response_time_ms
        )
        
        with self._lock:
            # Update current status
            previous_status = self._current_status.get(component_name, HealthStatus.UNKNOWN)
            self._current_status[component_name] = status
            
            # Add to history
            if component_name not in self._health_history:
                self._health_history[component_name] = []
            
            self._health_history[component_name].append(health_check)
            
            # Maintain history size limit
            if len(self._health_history[component_name]) > self.max_history_entries:
                self._health_history[component_name] = self._health_history[component_name][-self.max_history_entries:]
        
        # Log status changes
        if previous_status != status:
            log_level = {
                HealthStatus.HEALTHY: logging.INFO,
                HealthStatus.DEGRADED: logging.WARNING,
                HealthStatus.UNHEALTHY: logging.ERROR,
                HealthStatus.UNKNOWN: logging.WARNING
            }.get(status, logging.INFO)
            
            logger.log(log_level, f"Health status changed for {component_name}: {previous_status.value} -> {status.value}")
            
            # Send notification for status changes
            self._send_health_notification(component_name, previous_status, status, message)
    
    def _send_health_notification(self, component_name: str, previous_status: HealthStatus,
                                 new_status: HealthStatus, message: str):
        """Send notification about health status changes."""
        if new_status == HealthStatus.HEALTHY and previous_status != HealthStatus.HEALTHY:
            # Component recovered
            self.notification_service.add_notification(
                f"{component_name.title()} Recovered",
                f"{component_name} is now healthy: {message}",
                NotificationLevel.SUCCESS,
                NotificationType.SYSTEM_STATUS,
                source="HEALTH_MONITOR",
                details={'component': component_name, 'status': new_status.value}
            )
        elif new_status == HealthStatus.UNHEALTHY:
            # Component failed
            self.notification_service.add_notification(
                f"{component_name.title()} Unhealthy",
                f"{component_name} is unhealthy: {message}",
                NotificationLevel.ERROR,
                NotificationType.SYSTEM_STATUS,
                source="HEALTH_MONITOR",
                action_required=True,
                details={'component': component_name, 'status': new_status.value}
            )
        elif new_status == HealthStatus.DEGRADED and previous_status == HealthStatus.HEALTHY:
            # Component degraded
            self.notification_service.add_notification(
                f"{component_name.title()} Degraded",
                f"{component_name} performance is degraded: {message}",
                NotificationLevel.WARNING,
                NotificationType.SYSTEM_STATUS,
                source="HEALTH_MONITOR",
                details={'component': component_name, 'status': new_status.value}
            )
    
    def get_current_status(self, component_name: str = None) -> Dict[str, HealthStatus]:
        """
        Get current health status for components.
        
        Args:
            component_name: Optional specific component name
            
        Returns:
            Dictionary mapping component names to their current status
        """
        with self._lock:
            if component_name:
                return {component_name: self._current_status.get(component_name, HealthStatus.UNKNOWN)}
            else:
                return self._current_status.copy()
    
    def get_health_history(self, component_name: str, limit: int = None) -> List[HealthCheck]:
        """
        Get health check history for a component.
        
        Args:
            component_name: Name of the component
            limit: Optional limit on number of entries to return
            
        Returns:
            List of HealthCheck objects, newest first
        """
        with self._lock:
            history = self._health_history.get(component_name, [])
            
            # Sort by timestamp, newest first
            history = sorted(history, key=lambda h: h.timestamp, reverse=True)
            
            if limit:
                history = history[:limit]
            
            return history
    
    def get_overall_health(self) -> HealthStatus:
        """Get overall system health status."""
        with self._lock:
            statuses = list(self._current_status.values())
        
        if not statuses:
            return HealthStatus.UNKNOWN
        
        # If any component is unhealthy, system is unhealthy
        if HealthStatus.UNHEALTHY in statuses:
            return HealthStatus.UNHEALTHY
        
        # If any component is degraded, system is degraded
        if HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED
        
        # If all components are healthy, system is healthy
        if all(status == HealthStatus.HEALTHY for status in statuses):
            return HealthStatus.HEALTHY
        
        # Otherwise, status is unknown
        return HealthStatus.UNKNOWN
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get comprehensive health summary."""
        with self._lock:
            current_status = self._current_status.copy()
        
        overall_health = self.get_overall_health()
        
        # Count components by status
        status_counts = {}
        for status in HealthStatus:
            status_counts[status.value] = sum(1 for s in current_status.values() if s == status)
        
        # Get recent issues (last 24 hours)
        recent_issues = []
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        for component_name, history in self._health_history.items():
            for check in history:
                if (check.timestamp >= cutoff_time and 
                    check.status in [HealthStatus.UNHEALTHY, HealthStatus.DEGRADED]):
                    recent_issues.append(check.to_dict())
        
        # Sort recent issues by timestamp
        recent_issues.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return {
            'overall_health': overall_health.value,
            'component_status': {name: status.value for name, status in current_status.items()},
            'status_counts': status_counts,
            'recent_issues': recent_issues[:10],  # Last 10 issues
            'monitoring_active': self._running,
            'last_check_time': max(
                (max(history, key=lambda h: h.timestamp).timestamp for history in self._health_history.values() if history),
                default=None
            )
        }
    
    def force_check(self, component_name: str = None):
        """
        Force immediate health check for specified component(s).
        
        Args:
            component_name: Optional specific component to check. If None, checks all.
        """
        if component_name:
            if component_name in self.data_sources:
                self._check_data_source_health(component_name)
            elif component_name == 'database':
                self._check_database_health()
            elif component_name == 'network':
                self._check_network_health()
            else:
                logger.warning(f"Unknown component for health check: {component_name}")
        else:
            self._perform_all_checks()
    
    def add_check_callback(self, callback: Callable[[], None]):
        """
        Add a callback function to be called after each health check cycle.
        
        Args:
            callback: Function to call after health checks
        """
        with self._lock:
            self._check_callbacks.append(callback)
    
    def remove_check_callback(self, callback: Callable[[], None]):
        """Remove a health check callback."""
        with self._lock:
            if callback in self._check_callbacks:
                self._check_callbacks.remove(callback)
    
    def _notify_check_callbacks(self):
        """Notify all registered callbacks about completed health checks."""
        with self._lock:
            callbacks = self._check_callbacks.copy()
        
        for callback in callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"Error in health check callback: {e}")
    
    def is_running(self) -> bool:
        """Check if the health monitor is currently running."""
        return self._running
    
    def __del__(self):
        """Cleanup when monitor is destroyed."""
        self.stop_monitoring()