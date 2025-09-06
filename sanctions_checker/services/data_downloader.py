"""
Data downloader module for fetching sanctions lists from various sources.
"""
import requests
import time
import hashlib
from typing import Dict, Optional, Tuple
from datetime import datetime
from pathlib import Path
import os

from ..utils.logger import get_logger, log_performance
from ..utils.error_handler import (
    NetworkError, DataParsingError, ErrorContext, handle_exceptions
)
from ..utils.recovery import RecoveryManager, RetryConfig, CircuitBreakerConfig

logger = get_logger(__name__)


class DataDownloader:
    """Downloads sanctions data from multiple sources with retry logic and integrity checking."""
    
    # Data source URLs with status information
    DATA_SOURCES = {
        'EU': {
            'url': 'https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw',
            'format': 'xml',
            'description': 'EU Consolidated Sanctions List',
            'status': 'active',
            'note': 'EU sanctions list is working normally.'
        },
        'UN': {
            'url': 'https://scsanctions.un.org/resources/xml/en/consolidated.xml',
            'format': 'xml', 
            'description': 'UN Security Council Consolidated List',
            'status': 'blocked',
            'note': 'UN blocks automated access. Manual download required from Security Council website.',
            'manual_url': 'https://www.un.org/securitycouncil/sanctions/',
            'instructions': 'Visit the UN Security Council sanctions page and download the consolidated list manually.'
        },
        'OFAC': {
            'url': 'https://sanctionslistservice.ofac.treas.gov/api/download/sdn.xml',
            'format': 'xml',
            'description': 'OFAC Specially Designated Nationals List',
            'status': 'deprecated',
            'note': 'OFAC API endpoint has changed. Manual download may be required.',
            'manual_url': 'https://home.treasury.gov/policy-issues/financial-sanctions/specially-designated-nationals-and-blocked-persons-list-sdn-human-readable-lists',
            'instructions': 'Visit the OFAC website to download the SDN list manually.'
        }
    }
    
    def __init__(self, data_dir: str = "data", timeout: int = 30, max_retries: int = 3):
        """
        Initialize the data downloader.
        
        Args:
            data_dir: Directory to store downloaded files
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SanctionsChecker/1.0 (Compliance Tool)'
        })
        
        # Initialize recovery manager
        self.recovery_manager = RecoveryManager()
        
        # Setup retry configuration
        self.retry_config = RetryConfig(
            max_attempts=max_retries,
            base_delay=1.0,
            max_delay=30.0,
            exponential_base=2.0,
            jitter=True
        )
        
        # Setup circuit breakers for each data source
        circuit_config = CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=300.0,  # 5 minutes
            success_threshold=2
        )
        
        for source_name in self.DATA_SOURCES:
            self.recovery_manager.register_circuit_breaker(
                f"download_{source_name.lower()}",
                circuit_config
            )
    
    
    def _handle_download_error(self, source_name: str, error: Exception) -> Dict:
        """
        Handle download errors gracefully with user-friendly messages.
        
        Args:
            source_name: Name of the data source that failed
            error: The exception that occurred
            
        Returns:
            Dictionary with error information and user guidance
        """
        source_config = self.DATA_SOURCES.get(source_name, {})
        status = source_config.get('status', 'unknown')
        
        # Determine error type and provide specific guidance
        error_str = str(error).lower()
        
        if '404' in error_str or 'not found' in error_str:
            if source_name == 'UN':
                message = "UN sanctions list is currently unavailable (404 error)"
                suggestion = f"The UN has changed their API. Visit {source_config.get('manual_url', 'https://www.un.org/securitycouncil/sanctions/')} for manual updates."
                impact = "UN sanctions screening will use cached data if available."
                user_action = "Download the UN consolidated list XML file manually and import it through Settings → Data Management."
            elif source_name == 'OFAC':
                message = "OFAC sanctions list endpoint not found (404 error)"
                suggestion = f"The OFAC API has changed. Visit {source_config.get('manual_url', 'https://home.treasury.gov/policy-issues/financial-sanctions/')} for manual updates."
                impact = "OFAC sanctions screening will use cached data if available."
                user_action = "Download the OFAC SDN list manually and import it through Settings → Data Management."
            else:
                message = f"{source_name} sanctions list is currently unavailable (404 error)"
                suggestion = f"The {source_name} API may have changed. Check the official website for updates."
                impact = f"{source_name} sanctions screening will use cached data if available."
                user_action = f"Check the {source_name} official website for manual download options."
                
        elif '405' in error_str or 'method not allowed' in error_str:
            if source_name == 'OFAC':
                message = "OFAC API method not allowed (405 error)"
                suggestion = f"The OFAC API has changed their access method. Visit {source_config.get('manual_url', 'https://home.treasury.gov/policy-issues/financial-sanctions/')} for manual updates."
                impact = "OFAC sanctions screening will use cached data if available."
                user_action = "Download the OFAC SDN list manually and import it through Settings → Data Management."
            else:
                message = f"{source_name} API method not allowed (405 error)"
                suggestion = f"The {source_name} API has changed their access method. Check the official website."
                impact = f"{source_name} sanctions screening will use cached data if available."
                user_action = f"Check the {source_name} official website for updated API documentation."
                
        elif '403' in error_str or 'forbidden' in error_str:
            if source_name == 'UN':
                message = "UN blocks automated access to sanctions data (403 Forbidden)"
                suggestion = f"The UN now requires manual download. Visit {source_config.get('manual_url', 'https://www.un.org/securitycouncil/sanctions/')} to download the consolidated list."
                impact = "UN sanctions screening will use cached data if available."
                user_action = "Download the UN consolidated list XML file manually and import it through Settings → Data Management."
            else:
                message = f"{source_name} blocks automated access (403 Forbidden)"
                suggestion = f"The {source_name} now requires manual download. Check their official website."
                impact = f"{source_name} sanctions screening will use cached data if available."
                user_action = f"Visit the {source_name} official website for manual download instructions."
        else:
            message = f"{source_name} sanctions list download failed"
            suggestion = f"Network or server error occurred. The {source_name} service may be temporarily unavailable."
            impact = f"{source_name} sanctions screening will use cached data if available."
            user_action = "Try again later or check the official website for service status."
        
        # Log user-friendly message instead of technical error
        logger.warning(f"{message}")
        logger.info(f"Suggestion: {suggestion}")
        logger.info(f"Impact: {impact}")
        logger.info(f"User Action: {user_action}")
        
        return {
            'success': False,
            'error': message,
            'suggestion': suggestion,
            'impact': impact,
            'user_action': user_action,
            'manual_url': source_config.get('manual_url'),
            'instructions': source_config.get('instructions'),
            'status': status,
            'timestamp': datetime.now(),
            'original_error': str(error)
        }

    @log_performance(logger, "download_all_sources")
    def download_all_sources(self) -> Dict[str, Dict]:
        """
        Download data from all configured sources.
        
        Returns:
            Dictionary with source names as keys and download results as values
        """
        results = {}
        
        for source_name in self.DATA_SOURCES:
            context = ErrorContext(
                operation="download_all_sources",
                component="data_downloader",
                additional_data={"source": source_name}
            )
            
            try:
                result = self.download_source(source_name)
                results[source_name] = result
                logger.info(f"Successfully downloaded {source_name} data")
            except Exception as e:
                # Use graceful error handling instead of technical errors
                results[source_name] = self._handle_download_error(source_name, e)
        
        return results
    
    def download_source(self, source_name: str) -> Dict:
        """
        Download data from a specific source with retry logic and circuit breaker protection.
        
        Args:
            source_name: Name of the data source (EU, UN, OFAC)
            
        Returns:
            Dictionary containing download results and metadata
            
        Raises:
            ValueError: If source_name is not recognized
            NetworkError: If download fails after all retries
        """
        if source_name not in self.DATA_SOURCES:
            raise ValueError(f"Unknown data source: {source_name}")
        
        context = ErrorContext(
            operation="download_source",
            component="data_downloader",
            additional_data={"source": source_name}
        )
        
        # Use circuit breaker and retry logic
        circuit_breaker_name = f"download_{source_name.lower()}"
        
        def _download_with_retry():
            return self.recovery_manager.execute_with_retry(
                self._perform_download,
                self.retry_config,
                f"download_{source_name}",
                source_name
            )
        
        try:
            return self.recovery_manager.execute_with_circuit_breaker(
                _download_with_retry,
                circuit_breaker_name
            )
        except Exception as e:
            raise NetworkError(
                f"Failed to download {source_name} data after all retry attempts",
                context=context,
                original_exception=e,
                recoverable=False
            )
    
    def _perform_download(self, source_name: str) -> Dict:
        """
        Perform the actual download operation.
        
        Args:
            source_name: Name of the data source
            
        Returns:
            Dictionary containing download results and metadata
        """
        source_config = self.DATA_SOURCES[source_name]
        url = source_config['url']
        
        logger.info(f"Downloading {source_name} data from {url}")
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            # Verify content type matches expected format
            content_type = response.headers.get('content-type', '').lower()
            expected_format = source_config['format']
            
            if not self._validate_content_type(content_type, expected_format):
                logger.warning(f"Unexpected content type for {source_name}: {content_type}")
            
            # Generate file path
            file_extension = expected_format
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            filename = f"{source_name.lower()}_sanctions_{timestamp}.{file_extension}"
            file_path = self.data_dir / filename
            
            # Save file and calculate hash
            content = response.content
            content_hash = hashlib.sha256(content).hexdigest()
            
            with open(file_path, 'wb') as f:
                f.write(content)
            
            # Create metadata
            metadata = {
                'success': True,
                'source': source_name,
                'url': url,
                'file_path': str(file_path),
                'file_size': len(content),
                'content_hash': content_hash,
                'download_timestamp': datetime.now(),
                'content_type': content_type,
                'format': expected_format,
                'description': source_config['description']
            }
            
            logger.info(f"Successfully downloaded {source_name} data to {file_path}")
            return metadata
            
        except requests.RequestException as e:
            raise NetworkError(
                f"Failed to download {source_name} data: {e}",
                context=ErrorContext(
                    operation="_perform_download",
                    component="data_downloader",
                    additional_data={"source": source_name, "url": url}
                ),
                original_exception=e,
                recoverable=True
            )
    
    def _validate_content_type(self, content_type: str, expected_format: str) -> bool:
        """
        Validate that the content type matches the expected format.
        
        Args:
            content_type: HTTP content-type header value
            expected_format: Expected file format (xml, csv, json)
            
        Returns:
            True if content type is valid for the expected format
        """
        format_mappings = {
            'xml': ['application/xml', 'text/xml'],
            'csv': ['text/csv', 'application/csv'],
            'json': ['application/json', 'text/json']
        }
        
        if expected_format not in format_mappings:
            return True  # Unknown format, skip validation
        
        valid_types = format_mappings[expected_format]
        return any(valid_type in content_type for valid_type in valid_types)
    
    def verify_file_integrity(self, file_path: str, expected_hash: str) -> bool:
        """
        Verify the integrity of a downloaded file using SHA-256 hash.
        
        Args:
            file_path: Path to the file to verify
            expected_hash: Expected SHA-256 hash
            
        Returns:
            True if file integrity is verified
        """
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
                actual_hash = hashlib.sha256(content).hexdigest()
                return actual_hash == expected_hash
        except Exception as e:
            logger.error(f"Error verifying file integrity: {e}")
            return False
    
    def get_latest_file(self, source_name: str) -> Optional[str]:
        """
        Get the path to the most recently downloaded file for a source.
        
        Args:
            source_name: Name of the data source
            
        Returns:
            Path to the latest file, or None if no files found
        """
        pattern = f"{source_name.lower()}_sanctions_*.{self.DATA_SOURCES[source_name]['format']}"
        files = list(self.data_dir.glob(pattern))
        
        if not files:
            return None
        
        # Sort by modification time, return the newest
        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        return str(latest_file)
    
    def cleanup_old_files(self, source_name: str, keep_count: int = 5):
        """
        Clean up old downloaded files, keeping only the most recent ones.
        
        Args:
            source_name: Name of the data source
            keep_count: Number of recent files to keep
        """
        pattern = f"{source_name.lower()}_sanctions_*.{self.DATA_SOURCES[source_name]['format']}"
        files = list(self.data_dir.glob(pattern))
        
        if len(files) <= keep_count:
            return
        
        # Sort by modification time, oldest first
        files.sort(key=lambda f: f.stat().st_mtime)
        
        # Remove oldest files
        files_to_remove = files[:-keep_count]
        for file_path in files_to_remove:
            try:
                os.remove(file_path)
                logger.info(f"Removed old file: {file_path}")
            except Exception as e:
                logger.error(f"Error removing file {file_path}: {e}")