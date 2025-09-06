#!/usr/bin/env python3
"""
Application update service for Sanctions Checker.
Handles checking for updates, downloading, and installing new versions.
"""

import json
import os
import sys
import hashlib
import tempfile
import subprocess
import requests
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
import logging

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger(__name__)

class UpdateService:
    """Service for handling application updates."""
    
    def __init__(self, config: Config):
        """Initialize the update service."""
        self.config = config
        self.current_version = self._get_current_version()
        self.update_url = config.get('update_url', 'https://api.github.com/repos/sanctions-checker/sanctions-checker/releases/latest')
        self.check_interval = timedelta(days=config.get('update_check_interval_days', 7))
        
    def _get_current_version(self) -> str:
        """Get the current application version."""
        try:
            version_file = Path(__file__).parent.parent.parent / 'VERSION'
            if version_file.exists():
                return version_file.read_text().strip()
            else:
                return '1.0.0'  # Default version
        except Exception as e:
            logger.error(f"Error reading version file: {e}")
            return '1.0.0'
    
    def should_check_for_updates(self) -> bool:
        """Check if it's time to check for updates."""
        try:
            last_check_file = Path(self.config.get_data_dir()) / 'last_update_check.json'
            
            if not last_check_file.exists():
                return True
            
            with open(last_check_file, 'r') as f:
                data = json.load(f)
                last_check = datetime.fromisoformat(data['last_check'])
                return datetime.now() - last_check > self.check_interval
                
        except Exception as e:
            logger.error(f"Error checking update schedule: {e}")
            return True
    
    def check_for_updates(self) -> Optional[Dict]:
        """Check for available updates."""
        try:
            logger.info("Checking for application updates...")
            
            # Record the check time
            self._record_update_check()
            
            # Get latest release info
            response = requests.get(self.update_url, timeout=30)
            response.raise_for_status()
            
            release_info = response.json()
            latest_version = release_info['tag_name'].lstrip('v')
            
            if self._is_newer_version(latest_version, self.current_version):
                logger.info(f"Update available: {latest_version} (current: {self.current_version})")
                
                # Find the appropriate asset for this platform
                asset = self._find_platform_asset(release_info['assets'])
                if asset:
                    return {
                        'version': latest_version,
                        'release_notes': release_info['body'],
                        'download_url': asset['browser_download_url'],
                        'file_size': asset['size'],
                        'checksum': asset.get('checksum'),  # If provided
                        'published_at': release_info['published_at']
                    }
            else:
                logger.info(f"Application is up to date (version {self.current_version})")
                
        except requests.RequestException as e:
            logger.error(f"Network error checking for updates: {e}")
        except Exception as e:
            logger.error(f"Error checking for updates: {e}")
        
        return None
    
    def _record_update_check(self):
        """Record when we last checked for updates."""
        try:
            last_check_file = Path(self.config.get_data_dir()) / 'last_update_check.json'
            data = {
                'last_check': datetime.now().isoformat(),
                'current_version': self.current_version
            }
            
            with open(last_check_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error recording update check: {e}")
    
    def _is_newer_version(self, latest: str, current: str) -> bool:
        """Compare version strings to determine if latest is newer."""
        try:
            def version_tuple(v):
                return tuple(map(int, v.split('.')))
            
            return version_tuple(latest) > version_tuple(current)
        except Exception:
            # Fallback to string comparison if version parsing fails
            return latest > current
    
    def _find_platform_asset(self, assets: list) -> Optional[Dict]:
        """Find the appropriate download asset for the current platform."""
        platform_map = {
            'win32': ['windows', 'win', '.exe'],
            'linux': ['linux', 'appimage'],
            'darwin': ['macos', 'mac', '.dmg']
        }
        
        current_platform = sys.platform
        platform_keywords = platform_map.get(current_platform, [])
        
        for asset in assets:
            name = asset['name'].lower()
            if any(keyword in name for keyword in platform_keywords):
                return asset
        
        return None
    
    def download_update(self, update_info: Dict, progress_callback=None) -> Optional[Path]:
        """Download the update file."""
        try:
            download_url = update_info['download_url']
            file_size = update_info.get('file_size', 0)
            
            logger.info(f"Downloading update from {download_url}")
            
            # Create temporary file
            temp_dir = Path(tempfile.gettempdir()) / 'sanctions_checker_update'
            temp_dir.mkdir(exist_ok=True)
            
            filename = Path(download_url).name
            temp_file = temp_dir / filename
            
            # Download with progress tracking
            response = requests.get(download_url, stream=True, timeout=60)
            response.raise_for_status()
            
            downloaded = 0
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if progress_callback and file_size > 0:
                            progress = (downloaded / file_size) * 100
                            progress_callback(progress)
            
            logger.info(f"Update downloaded to {temp_file}")
            
            # Verify checksum if provided
            if update_info.get('checksum'):
                if not self._verify_checksum(temp_file, update_info['checksum']):
                    logger.error("Update file checksum verification failed")
                    temp_file.unlink()
                    return None
            
            return temp_file
            
        except Exception as e:
            logger.error(f"Error downloading update: {e}")
            return None
    
    def _verify_checksum(self, file_path: Path, expected_checksum: str) -> bool:
        """Verify the downloaded file's checksum."""
        try:
            sha256_hash = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            
            actual_checksum = sha256_hash.hexdigest()
            return actual_checksum.lower() == expected_checksum.lower()
            
        except Exception as e:
            logger.error(f"Error verifying checksum: {e}")
            return False
    
    def install_update(self, update_file: Path) -> bool:
        """Install the downloaded update."""
        try:
            logger.info(f"Installing update from {update_file}")
            
            if sys.platform == 'win32':
                return self._install_windows_update(update_file)
            elif sys.platform == 'linux':
                return self._install_linux_update(update_file)
            elif sys.platform == 'darwin':
                return self._install_macos_update(update_file)
            else:
                logger.error(f"Update installation not supported on {sys.platform}")
                return False
                
        except Exception as e:
            logger.error(f"Error installing update: {e}")
            return False
    
    def _install_windows_update(self, update_file: Path) -> bool:
        """Install Windows update."""
        try:
            # For Windows, we typically run the installer
            if update_file.suffix.lower() == '.exe':
                # Run installer in silent mode
                result = subprocess.run([
                    str(update_file),
                    '/S',  # Silent installation
                    '/D=' + str(Path(sys.executable).parent.parent)  # Installation directory
                ], capture_output=True, text=True)
                
                return result.returncode == 0
            else:
                logger.error(f"Unsupported update file format: {update_file.suffix}")
                return False
                
        except Exception as e:
            logger.error(f"Error installing Windows update: {e}")
            return False
    
    def _install_linux_update(self, update_file: Path) -> bool:
        """Install Linux update."""
        try:
            if update_file.suffix.lower() == '.appimage':
                # For AppImage, we replace the current executable
                current_exe = Path(sys.executable)
                backup_exe = current_exe.with_suffix('.bak')
                
                # Create backup
                current_exe.rename(backup_exe)
                
                # Copy new version
                import shutil
                shutil.copy2(update_file, current_exe)
                current_exe.chmod(0o755)
                
                logger.info("Linux update installed successfully")
                return True
            else:
                logger.error(f"Unsupported update file format: {update_file.suffix}")
                return False
                
        except Exception as e:
            logger.error(f"Error installing Linux update: {e}")
            return False
    
    def _install_macos_update(self, update_file: Path) -> bool:
        """Install macOS update."""
        try:
            # For macOS, we would typically handle .dmg files
            # This is a simplified implementation
            logger.info("macOS update installation not fully implemented")
            return False
            
        except Exception as e:
            logger.error(f"Error installing macOS update: {e}")
            return False
    
    def get_update_history(self) -> list:
        """Get the history of update checks and installations."""
        try:
            history_file = Path(self.config.get_data_dir()) / 'update_history.json'
            
            if history_file.exists():
                with open(history_file, 'r') as f:
                    return json.load(f)
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error reading update history: {e}")
            return []
    
    def record_update_installation(self, version: str, success: bool):
        """Record an update installation attempt."""
        try:
            history = self.get_update_history()
            
            record = {
                'timestamp': datetime.now().isoformat(),
                'version': version,
                'success': success,
                'previous_version': self.current_version
            }
            
            history.append(record)
            
            # Keep only last 50 records
            history = history[-50:]
            
            history_file = Path(self.config.get_data_dir()) / 'update_history.json'
            with open(history_file, 'w') as f:
                json.dump(history, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error recording update installation: {e}")
    
    def cleanup_old_updates(self):
        """Clean up old update files."""
        try:
            temp_dir = Path(tempfile.gettempdir()) / 'sanctions_checker_update'
            if temp_dir.exists():
                import shutil
                shutil.rmtree(temp_dir)
                logger.info("Cleaned up old update files")
                
        except Exception as e:
            logger.error(f"Error cleaning up update files: {e}")


class AutoUpdateManager:
    """Manager for automatic update checking and installation."""
    
    def __init__(self, config: Config, update_service: UpdateService):
        """Initialize the auto-update manager."""
        self.config = config
        self.update_service = update_service
        self.auto_check_enabled = config.get('auto_check_updates', True)
        self.auto_install_enabled = config.get('auto_install_updates', False)
        
    def check_and_notify(self, notification_callback=None):
        """Check for updates and notify if available."""
        if not self.auto_check_enabled:
            return
        
        if not self.update_service.should_check_for_updates():
            return
        
        update_info = self.update_service.check_for_updates()
        if update_info and notification_callback:
            notification_callback(update_info)
    
    def auto_install_if_enabled(self, update_info: Dict) -> bool:
        """Automatically install update if auto-install is enabled."""
        if not self.auto_install_enabled:
            return False
        
        logger.info("Auto-installing update...")
        
        # Download update
        update_file = self.update_service.download_update(update_info)
        if not update_file:
            return False
        
        # Install update
        success = self.update_service.install_update(update_file)
        
        # Record the installation attempt
        self.update_service.record_update_installation(
            update_info['version'], success
        )
        
        # Clean up
        if update_file.exists():
            update_file.unlink()
        
        return success