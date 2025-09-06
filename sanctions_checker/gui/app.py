"""
Main application entry point for the Sanctions Checker GUI.
"""

import sys
import logging
from pathlib import Path

from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt

from sanctions_checker.config import Config
from sanctions_checker.gui.main_window import MainWindow


def setup_logging(config: Config):
    """Set up application logging."""
    log_level = getattr(logging, config.get('audit.log_level', 'INFO').upper())
    log_file = config.logs_directory / 'sanctions_checker.log'
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


def main():
    """Main application entry point."""
    # Create QApplication
    app = QApplication(sys.argv)
    app.setApplicationName("Sanctions Checker")
    app.setApplicationVersion("1.0")
    app.setOrganizationName("Sanctions Checker")
    
    # Enable high DPI scaling (PyQt6 handles this automatically)
    # app.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
    # app.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)
    
    try:
        # Load configuration
        config = Config()
        
        # Set up logging
        setup_logging(config)
        
        logger = logging.getLogger(__name__)
        logger.info("Starting Sanctions Checker application")
        
        # Create and show main window
        main_window = MainWindow(config)
        main_window.show()
        
        # Run application
        sys.exit(app.exec())
        
    except Exception as e:
        logging.error(f"Failed to start application: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()