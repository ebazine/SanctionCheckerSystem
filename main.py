#!/usr/bin/env python3
"""
Main entry point for the Sanctions Checker application.
"""

import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from sanctions_checker.config import Config
from sanctions_checker.gui.main_window import MainWindow
from sanctions_checker.utils.resources import resource_manager

try:
    from PyQt6.QtWidgets import QApplication
    from PyQt6.QtCore import Qt
except ImportError:
    print("PyQt6 is not installed. Please install it using: pip install PyQt6")
    sys.exit(1)


def main():
    """Main application entry point."""
    # Initialize configuration
    config = Config()
    
    # Create QApplication
    app = QApplication(sys.argv)
    app.setApplicationName("Sanctions Checker")
    app.setApplicationVersion("1.0.0")
    app.setOrganizationName("Sanctions Checker Team")
    
    # Set application icon
    app_icon = resource_manager.get_application_icon()
    if not app_icon.isNull():
        app.setWindowIcon(app_icon)
    
    # Set application attributes (with compatibility for different PyQt6 versions)
    try:
        app.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
        app.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)
    except AttributeError:
        # These attributes were deprecated in newer PyQt6 versions
        # High DPI scaling is enabled by default in newer versions
        pass
    
    # Create and show main window
    main_window = MainWindow(config)
    main_window.show()
    
    # Start event loop
    return app.exec()


if __name__ == "__main__":
    sys.exit(main())