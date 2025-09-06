#!/usr/bin/env python3
"""
Resource manager for handling application assets like logos, icons, etc.
"""

import os
import sys
from pathlib import Path
from PyQt6.QtGui import QPixmap, QIcon
from PyQt6.QtCore import Qt


class ResourceManager:
    """Manages application resources like logos, icons, and images."""
    
    def __init__(self):
        # Get the assets directory path - handle both development and packaged environments
        try:
            # Try to get the assets directory relative to this file
            self.assets_dir = Path(__file__).parent.parent / "assets"
            
            # Only create directory if we're not in a packaged environment
            if not getattr(sys, 'frozen', False):
                self.assets_dir.mkdir(exist_ok=True)
            elif not self.assets_dir.exists():
                # In packaged environment, use a user data directory for writable assets
                import tempfile
                user_data_dir = Path(tempfile.gettempdir()) / "SanctionsChecker" / "assets"
                user_data_dir.mkdir(parents=True, exist_ok=True)
                self.assets_dir = user_data_dir
        except Exception:
            # Fallback to temp directory if all else fails
            import tempfile
            self.assets_dir = Path(tempfile.gettempdir()) / "SanctionsChecker" / "assets"
            try:
                self.assets_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                # If we can't create any directory, just use the temp path
                pass
        
        # Define resource paths
        self.logo_path = self.assets_dir / "logo.png"
        self.icon_path = self.assets_dir / "icon.ico"
        self.logo_small_path = self.assets_dir / "logo_small.png"
    
    def get_logo_pixmap(self, width: int = None, height: int = None) -> QPixmap:
        """Get the main logo as a QPixmap, optionally scaled."""
        if self.logo_path.exists():
            pixmap = QPixmap(str(self.logo_path))
            
            if width or height:
                # Scale the pixmap while maintaining aspect ratio
                if width and height:
                    pixmap = pixmap.scaled(width, height, Qt.AspectRatioMode.KeepAspectRatio, 
                                         Qt.TransformationMode.SmoothTransformation)
                elif width:
                    pixmap = pixmap.scaledToWidth(width, Qt.TransformationMode.SmoothTransformation)
                elif height:
                    pixmap = pixmap.scaledToHeight(height, Qt.TransformationMode.SmoothTransformation)
            
            return pixmap
        else:
            # Return a placeholder pixmap if logo doesn't exist
            return self._create_placeholder_logo(width or 200, height or 100)
    
    def get_application_icon(self) -> QIcon:
        """Get the application icon."""
        if self.icon_path.exists():
            return QIcon(str(self.icon_path))
        elif self.logo_path.exists():
            # Use logo as icon if no dedicated icon exists
            return QIcon(str(self.logo_path))
        else:
            # Return empty icon if no resources available
            return QIcon()
    
    def get_small_logo_pixmap(self, size: int = 32) -> QPixmap:
        """Get a small version of the logo for toolbars, etc."""
        if self.logo_small_path.exists():
            pixmap = QPixmap(str(self.logo_small_path))
            return pixmap.scaled(size, size, Qt.AspectRatioMode.KeepAspectRatio, 
                               Qt.TransformationMode.SmoothTransformation)
        elif self.logo_path.exists():
            pixmap = QPixmap(str(self.logo_path))
            return pixmap.scaled(size, size, Qt.AspectRatioMode.KeepAspectRatio, 
                               Qt.TransformationMode.SmoothTransformation)
        else:
            return self._create_placeholder_logo(size, size)
    
    def _create_placeholder_logo(self, width: int, height: int) -> QPixmap:
        """Create a placeholder logo when the actual logo is not available."""
        from PyQt6.QtGui import QPainter, QBrush, QColor, QFont
        
        pixmap = QPixmap(width, height)
        pixmap.fill(QColor(240, 240, 240))  # Light gray background
        
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Draw a simple placeholder
        painter.setBrush(QBrush(QColor(100, 150, 200)))
        painter.drawRoundedRect(10, 10, width-20, height-20, 10, 10)
        
        # Add text
        painter.setPen(QColor(255, 255, 255))
        font = QFont("Arial", max(8, width // 20))
        painter.setFont(font)
        painter.drawText(pixmap.rect(), Qt.AlignmentFlag.AlignCenter, "SC")
        
        painter.end()
        return pixmap
    
    def has_logo(self) -> bool:
        """Check if the logo file exists."""
        return self.logo_path.exists()
    
    def has_icon(self) -> bool:
        """Check if the icon file exists."""
        return self.icon_path.exists()
    
    def get_assets_directory(self) -> Path:
        """Get the assets directory path."""
        return self.assets_dir
    
    def install_logo(self, source_path: str) -> bool:
        """Install a logo file from a source path."""
        try:
            source = Path(source_path)
            if source.exists():
                import shutil
                shutil.copy2(source, self.logo_path)
                return True
        except Exception as e:
            print(f"Error installing logo: {e}")
        return False
    
    def reset_to_default_logo(self) -> bool:
        """Reset logo to default (remove custom logo)."""
        try:
            if self.logo_path.exists():
                self.logo_path.unlink()
            if self.logo_small_path.exists():
                self.logo_small_path.unlink()
            return True
        except Exception as e:
            print(f"Error resetting logo: {e}")
            return False


# Global resource manager instance
resource_manager = ResourceManager()