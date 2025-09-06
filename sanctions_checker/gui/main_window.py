"""
Main window for the Sanctions Checker application.
"""

import re
import logging
from typing import Optional, List
from datetime import datetime

from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, QTextEdit, QProgressBar,
    QMenuBar, QMenu, QStatusBar, QFrame, QGroupBox, QSplitter,
    QMessageBox, QTabWidget
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QThread, pyqtSlot
from PyQt6.QtGui import QAction, QFont, QIcon

from sanctions_checker.config import Config
from sanctions_checker.services.search_service import SearchService, EntityMatch
from sanctions_checker.database.manager import DatabaseManager
from sanctions_checker.utils.resources import resource_manager
from .results_widget import SearchResultsWidget
from .logo_upload_dialog import LogoUploadDialog
from .batch_search_dialog import BatchSearchDialog

logger = logging.getLogger(__name__)


class SearchWorker(QThread):
    """Worker thread for performing searches without blocking the UI."""
    
    # Signals
    search_started = pyqtSignal()
    search_progress = pyqtSignal(int)  # Progress percentage
    search_completed = pyqtSignal(list, str)  # Search results and record ID
    search_error = pyqtSignal(str)  # Error message
    
    def __init__(self, search_service: SearchService, search_query: str, entity_type: str, user_id: str = None, tags: list = None):
        super().__init__()
        self.search_service = search_service
        self.search_query = search_query
        self.entity_type = entity_type
        self.user_id = user_id
        self.tags = tags or []
        self._is_cancelled = False
    
    def run(self):
        """Execute the search operation."""
        try:
            self.search_started.emit()
            
            if self._is_cancelled:
                return
            
            # Perform actual search
            self.search_progress.emit(25)
            
            # Convert entity type for search service
            search_entity_type = None
            if self.entity_type != "All":
                search_entity_type = self.entity_type.upper()
            
            self.search_progress.emit(50)
            
            if self._is_cancelled:
                return
            
            # Execute search
            matches, search_record_id = self.search_service.search_entities(
                query=self.search_query,
                entity_type=search_entity_type,
                user_id=self.user_id,
                tags=self.tags
            )
            
            self.search_progress.emit(100)
            
            if not self._is_cancelled:
                self.search_completed.emit(matches, search_record_id)
            
        except Exception as e:
            if not self._is_cancelled:
                self.search_error.emit(str(e))
    
    def cancel(self):
        """Cancel the search operation."""
        self._is_cancelled = True


class MainWindow(QMainWindow):
    """Main application window."""
    
    def __init__(self, config: Config, search_service: SearchService = None):
        """Initialize main window.
        
        Args:
            config: Application configuration
            search_service: Search service instance (optional, will create if not provided)
        """
        super().__init__()
        self.config = config
        self.search_service = search_service
        self.search_worker: Optional[SearchWorker] = None
        self.current_search_record_id: Optional[str] = None
        self.validation_timer = QTimer()
        self.validation_timer.setSingleShot(True)
        self.validation_timer.timeout.connect(self._validate_input)
        
        # Initialize database manager
        self.db_manager = DatabaseManager()
        self.db_manager.initialize_database()
        
        # Initialize search service if not provided
        if not self.search_service:
            try:
                self.search_service = SearchService(self.db_manager)
            except Exception as e:
                logger.warning(f"Could not initialize search service: {e}")
                self.search_service = None
        
        # Initialize data service for history widget
        self.data_service = None
        if self.search_service and self.search_service.db_manager:
            try:
                from ..services.data_service import DataService
                self.data_service = DataService(self.search_service.db_manager)
            except Exception as e:
                logger.warning(f"Could not initialize data service: {e}")
                self.data_service = None
        
        self.setup_ui()
        self.setup_menu()
        self.setup_status_bar()
        self.setup_connections()
    
    def setup_ui(self):
        """Set up the user interface."""
        self.setWindowTitle("Sanctions Checker")
        self.setGeometry(
            100, 100,
            self.config.get('gui.window_width', 1200),
            self.config.get('gui.window_height', 800)
        )
        
        # Set application icon
        app_icon = resource_manager.get_application_icon()
        if not app_icon.isNull():
            self.setWindowIcon(app_icon)
        
        # Create central widget with splitter
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)
        
        # Create splitter for resizable panels
        splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(splitter)
        
        # Left panel - Search interface
        search_panel = self._create_search_panel()
        splitter.addWidget(search_panel)
        
        # Right panel - Tabs for results, history, settings
        tabs_panel = self._create_tabs_panel()
        splitter.addWidget(tabs_panel)
        
        # Set splitter proportions (30% search, 70% results)
        splitter.setSizes([360, 840])
        
        # Progress bar (initially hidden)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        
        # Set minimum size
        self.setMinimumSize(800, 600)
    
    def refresh_tags_combo(self):
        """Refresh the tags combo box with available tags from settings."""
        self.tags_combo.clear()
        self.tags_combo.addItem("")  # Empty option
        
        # Get tags from config
        tags = self.config.get('search.tags', [])
        for tag in tags:
            self.tags_combo.addItem(tag)
    
    def _create_search_panel(self) -> QWidget:
        """Create the search input panel."""
        panel = QFrame()
        panel.setFrameStyle(QFrame.Shape.StyledPanel)
        panel.setMaximumWidth(400)
        
        layout = QVBoxLayout(panel)
        layout.setSpacing(15)
        
        # Logo section
        logo_layout = QHBoxLayout()
        logo_label = QLabel()
        logo_pixmap = resource_manager.get_logo_pixmap(width=300)  # Scale to fit panel
        if not logo_pixmap.isNull():
            logo_label.setPixmap(logo_pixmap)
            logo_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            logo_layout.addWidget(logo_label)
            layout.addLayout(logo_layout)
        
        # Title
        title = QLabel("Sanctions Search")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        title.setFont(title_font)
        layout.addWidget(title)
        
        # Search input group
        search_group = QGroupBox("Search Parameters")
        search_layout = QGridLayout(search_group)
        
        # Entity name input
        search_layout.addWidget(QLabel("Name/Company:"), 0, 0)
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Enter name or company to search...")
        search_layout.addWidget(self.name_input, 0, 1)
        
        # Entity type selection
        search_layout.addWidget(QLabel("Entity Type:"), 1, 0)
        self.entity_type_combo = QComboBox()
        self.entity_type_combo.addItems(["All", "Individual", "Company"])
        search_layout.addWidget(self.entity_type_combo, 1, 1)
        
        # Tag selection
        search_layout.addWidget(QLabel("Tags:"), 2, 0)
        self.tags_combo = QComboBox()
        self.tags_combo.setEditable(True)
        self.tags_combo.setPlaceholderText("Select or enter tags...")
        self.refresh_tags_combo()
        search_layout.addWidget(self.tags_combo, 2, 1)
        
        # Search button
        self.search_button = QPushButton("Search")
        self.search_button.setEnabled(False)
        self.search_button.setMinimumHeight(35)
        search_layout.addWidget(self.search_button, 3, 0, 1, 2)
        
        layout.addWidget(search_group)
        
        # Validation feedback
        self.validation_label = QLabel()
        self.validation_label.setStyleSheet("color: red; font-size: 11px;")
        self.validation_label.setWordWrap(True)
        layout.addWidget(self.validation_label)
        
        # Search options group
        options_group = QGroupBox("Search Options")
        options_layout = QVBoxLayout(options_group)
        
        # Confidence threshold
        threshold_layout = QHBoxLayout()
        threshold_layout.addWidget(QLabel("Min. Confidence:"))
        self.confidence_combo = QComboBox()
        self.confidence_combo.addItems(["40%", "50%", "60%", "70%", "80%", "90%"])
        self.confidence_combo.setCurrentText("70%")
        threshold_layout.addWidget(self.confidence_combo)
        options_layout.addLayout(threshold_layout)
        
        layout.addWidget(options_group)
        
        # Add stretch to push everything to top
        layout.addStretch()
        
        return panel
    
    def _create_tabs_panel(self) -> QWidget:
        """Create the tabbed panel for results, history, and settings."""
        tabs = QTabWidget()
        
        # Results tab - use the new SearchResultsWidget
        self.results_widget = SearchResultsWidget()
        tabs.addTab(self.results_widget, "Results")
        
        # History tab - use the new SearchHistoryWidget
        from .history_widget import SearchHistoryWidget
        self.history_widget = SearchHistoryWidget(self.data_service)
        tabs.addTab(self.history_widget, "History")
        
        # Data Status tab
        from .data_status_widget import DataStatusWidget
        from ..services.data_status_service import DataStatusService
        try:
            if self.search_service and self.search_service.db_manager:
                data_status_service = DataStatusService(self.config, self.search_service.db_manager)
                self.data_status_widget = DataStatusWidget(self.config, data_status_service)
                tabs.addTab(self.data_status_widget, "📊 Data Status")
            else:
                # Create placeholder if no database connection
                placeholder = QLabel("Database connection required for data status monitoring")
                placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
                tabs.addTab(placeholder, "📊 Data Status")
        except Exception as e:
            logger.warning(f"Could not create data status widget: {e}")
            placeholder = QLabel(f"Error loading data status: {str(e)}")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            tabs.addTab(placeholder, "📊 Data Status")
        
        # Statistics tab
        from .statistics_widget import StatisticsWidget
        try:
            if self.search_service and self.search_service.db_manager:
                self.statistics_widget = StatisticsWidget(self.config, data_status_service)
                tabs.addTab(self.statistics_widget, "📈 Statistics")
            else:
                # Create placeholder if no database connection
                placeholder = QLabel("Database connection required for statistics")
                placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
                tabs.addTab(placeholder, "📈 Statistics")
        except Exception as e:
            logger.warning(f"Could not create statistics widget: {e}")
            placeholder = QLabel(f"Error loading statistics: {str(e)}")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            tabs.addTab(placeholder, "📈 Statistics")
        
        # Custom Sanctions tab
        from .custom_sanctions_management_widget import CustomSanctionsManagementWidget
        from ..services.custom_sanctions_service import CustomSanctionsService
        try:
            if self.search_service and self.search_service.db_manager:
                custom_sanctions_service = CustomSanctionsService(self.search_service.db_manager)
                self.custom_sanctions_widget = CustomSanctionsManagementWidget(custom_sanctions_service)
                tabs.addTab(self.custom_sanctions_widget, "📝 Custom Sanctions")
            else:
                # Create placeholder if no database connection
                placeholder = QLabel("Database connection required for custom sanctions management")
                placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
                tabs.addTab(placeholder, "📝 Custom Sanctions")
        except Exception as e:
            logger.warning(f"Could not create custom sanctions widget: {e}")
            placeholder = QLabel(f"Error loading custom sanctions: {str(e)}")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            tabs.addTab(placeholder, "📝 Custom Sanctions")
        
        # Settings tab - use the new SettingsWidget
        from .settings_widget import SettingsWidget
        self.settings_widget = SettingsWidget(self.config)
        # Connect settings changes to refresh tags combo
        self.settings_widget.settings_saved.connect(self.refresh_tags_combo)
        tabs.addTab(self.settings_widget, "⚙️ Settings")
        
        return tabs
    
    def setup_menu(self):
        """Set up the application menu bar."""
        menubar = self.menuBar()
        
        # File menu
        file_menu = menubar.addMenu("&File")
        
        # New search action
        new_search_action = QAction("&New Search", self)
        new_search_action.setShortcut("Ctrl+N")
        new_search_action.triggered.connect(self._new_search)
        file_menu.addAction(new_search_action)
        
        file_menu.addSeparator()
        
        # Export results action
        export_action = QAction("&Export Results", self)
        export_action.setShortcut("Ctrl+E")
        export_action.setEnabled(False)  # Will be enabled when results are available
        export_action.triggered.connect(self._export_results)
        file_menu.addAction(export_action)
        self.export_action = export_action
        
        # Batch export action
        batch_export_action = QAction("&Batch Export...", self)
        batch_export_action.setShortcut("Ctrl+Shift+E")
        batch_export_action.triggered.connect(self._batch_export)
        file_menu.addAction(batch_export_action)
        
        file_menu.addSeparator()
        
        # Verify report action
        verify_action = QAction("&Verify Report...", self)
        verify_action.setShortcut("Ctrl+V")
        verify_action.triggered.connect(self._verify_report)
        file_menu.addAction(verify_action)
        
        file_menu.addSeparator()
        
        # Exit action
        exit_action = QAction("E&xit", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Tools menu
        tools_menu = menubar.addMenu("&Tools")
        
        # Update data action
        update_data_action = QAction("&Update Sanctions Data", self)
        update_data_action.triggered.connect(self._update_data)
        tools_menu.addAction(update_data_action)
        
        # Settings action
        settings_action = QAction("&Settings", self)
        settings_action.setShortcut("Ctrl+,")
        settings_action.triggered.connect(self._show_settings)
        tools_menu.addAction(settings_action)
        
        tools_menu.addSeparator()
        
        # Custom Sanctions submenu
        custom_sanctions_menu = tools_menu.addMenu("&Custom Sanctions")
        
        # Manage custom sanctions action
        manage_custom_action = QAction("&Manage Custom Sanctions", self)
        manage_custom_action.setShortcut("Ctrl+M")
        manage_custom_action.triggered.connect(self._show_custom_sanctions)
        custom_sanctions_menu.addAction(manage_custom_action)
        
        custom_sanctions_menu.addSeparator()
        
        # Import custom sanctions action
        import_custom_action = QAction("&Import Custom Sanctions...", self)
        import_custom_action.triggered.connect(self._import_custom_sanctions)
        custom_sanctions_menu.addAction(import_custom_action)
        
        # Export custom sanctions action
        export_custom_action = QAction("&Export Custom Sanctions...", self)
        export_custom_action.triggered.connect(self._export_custom_sanctions)
        custom_sanctions_menu.addAction(export_custom_action)
        
        tools_menu.addSeparator()
        
        # Logo upload action
        logo_action = QAction("Upload &Logo", self)
        logo_action.triggered.connect(self._upload_logo)
        tools_menu.addAction(logo_action)

        
        # Help menu
        help_menu = menubar.addMenu("&Help")
        
        # Buy Me a Coffee action
        coffee_action = QAction("☕ &Buy Me a Coffee", self)
        coffee_action.triggered.connect(self._open_coffee_link)
        help_menu.addAction(coffee_action)
        
        help_menu.addSeparator()
        
        # About action
        about_action = QAction("&About", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)
    
    def setup_status_bar(self):
        """Set up the status bar."""
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")
        
        # Add Buy Me a Coffee link to the right side of status bar
        coffee_label = QLabel('<a href="https://buymeacoffee.com/eliesbazine" style="color: #FF813F; text-decoration: none;">☕ Buy Me a Coffee</a>')
        coffee_label.setOpenExternalLinks(True)
        coffee_label.setToolTip("Support the developer - Buy me a coffee!")
        self.status_bar.addPermanentWidget(coffee_label)
    
    def setup_connections(self):
        """Set up signal-slot connections."""
        # Connect input validation
        self.name_input.textChanged.connect(self._on_input_changed)
        
        # Connect search button
        self.search_button.clicked.connect(self._perform_search)
        
        # Connect Enter key in name input to search
        self.name_input.returnPressed.connect(self._perform_search)
        
        # Connect results widget export signal
        self.results_widget.export_requested.connect(self._export_results_list)
        
        # Connect history widget signals
        if hasattr(self, 'history_widget'):
            self.history_widget.search_replay_requested.connect(self._replay_search)
            self.history_widget.search_comparison_requested.connect(self._compare_searches)
    
    def _on_input_changed(self):
        """Handle input text changes for real-time validation."""
        # Restart validation timer (debounce rapid typing)
        self.validation_timer.stop()
        self.validation_timer.start(300)  # 300ms delay
    
    def _validate_input(self):
        """Validate search input and update UI accordingly."""
        name = self.name_input.text().strip()
        
        # Clear previous validation message
        self.validation_label.clear()
        
        if not name:
            self.search_button.setEnabled(False)
            return
        
        # Validate name length
        if len(name) < 2:
            self.validation_label.setText("Name must be at least 2 characters long")
            self.search_button.setEnabled(False)
            return
        
        # Validate name contains letters
        if not re.search(r'[a-zA-Z]', name):
            self.validation_label.setText("Name must contain at least one letter")
            self.search_button.setEnabled(False)
            return
        
        # Check for potentially problematic characters
        if re.search(r'[<>"\']', name):
            self.validation_label.setText("Name contains invalid characters")
            self.search_button.setEnabled(False)
            return
        
        # Input is valid
        self.search_button.setEnabled(True)
        self.validation_label.setText("")
    
    def _perform_search(self):
        """Perform sanctions search."""
        if not self.search_button.isEnabled():
            return
        
        if not self.search_service:
            QMessageBox.warning(
                self, 
                "Search Unavailable", 
                "Search service is not available. Please check the database connection."
            )
            return
        
        name = self.name_input.text().strip()
        entity_type = self.entity_type_combo.currentText()
        
        # Get tags from combo box
        tags = []
        tag_text = self.tags_combo.currentText().strip()
        if tag_text:
            # Split by comma and clean up
            tags = [tag.strip() for tag in tag_text.split(',') if tag.strip()]
        
        # Get user ID from settings
        user_id = self.config.get('branding.user_id', 'gui_user')
        
        # Disable search button and show progress
        self.search_button.setEnabled(False)
        self.search_button.setText("Searching...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.status_bar.showMessage(f"Searching for: {name}")
        
        # Clear previous results
        self.results_widget.clear_results()
        self.current_search_record_id = None
        
        # Start search worker thread
        self.search_worker = SearchWorker(
            search_service=self.search_service,
            search_query=name,
            entity_type=entity_type,
            user_id=user_id,
            tags=tags
        )
        self.search_worker.search_started.connect(self._on_search_started)
        self.search_worker.search_progress.connect(self._on_search_progress)
        self.search_worker.search_completed.connect(self._on_search_completed)
        self.search_worker.search_error.connect(self._on_search_error)
        self.search_worker.start()
    
    @pyqtSlot()
    def _on_search_started(self):
        """Handle search started signal."""
        self.status_bar.showMessage("Search in progress...")
    
    @pyqtSlot(int)
    def _on_search_progress(self, progress: int):
        """Handle search progress updates."""
        self.progress_bar.setValue(progress)
    
    @pyqtSlot(list, str)
    def _on_search_completed(self, results, search_record_id):
        """Handle search completion."""
        # Reset UI
        self.search_button.setEnabled(True)
        self.search_button.setText("Search")
        self.progress_bar.setVisible(False)
        self.export_action.setEnabled(len(results) > 0)
        
        # Store search record ID for potential export
        self.current_search_record_id = search_record_id
        
        # Get tags that were used for this search
        tag_text = self.tags_combo.currentText().strip()
        search_tags = []
        if tag_text:
            search_tags = [tag.strip() for tag in tag_text.split(',') if tag.strip()]
        
        # Display results in the results widget
        self.results_widget.set_results(results, search_tags)
        
        # Update status
        if results:
            self.status_bar.showMessage(f"Search completed - {len(results)} matches found")
        else:
            self.status_bar.showMessage("Search completed - no matches found")
        
        # Refresh history widget to show the new search
        if hasattr(self, 'history_widget'):
            # Use a timer to refresh after a short delay to ensure the record is saved
            QTimer.singleShot(1000, self.history_widget.refresh_history)
    
    @pyqtSlot(str)
    def _on_search_error(self, error_message: str):
        """Handle search errors."""
        # Reset UI
        self.search_button.setEnabled(True)
        self.search_button.setText("Search")
        self.progress_bar.setVisible(False)
        
        # Show error message
        QMessageBox.critical(self, "Search Error", f"An error occurred during search:\n{error_message}")
        self.status_bar.showMessage("Search failed")
    
    def _new_search(self):
        """Start a new search by clearing inputs."""
        self.name_input.clear()
        self.entity_type_combo.setCurrentIndex(0)
        self.confidence_combo.setCurrentText("70%")
        self.results_widget.clear_results()
        self.validation_label.clear()
        self.name_input.setFocus()
        self.current_search_record_id = None
        self.export_action.setEnabled(False)
        self.status_bar.showMessage("Ready for new search")
    
    def _export_results(self):
        """Export search results."""
        if not self.current_search_record_id:
            QMessageBox.information(self, "Export", "No search results to export.")
            return
        
        # Get current results from the results widget
        matches = self.results_widget.filtered_matches
        if not matches:
            QMessageBox.information(self, "Export", "No search results to export.")
            return
        
        self._export_results_list(matches)
    
    def _export_results_list(self, matches: List[EntityMatch]):
        """Export a list of search results using the export dialog."""
        if not matches:
            QMessageBox.information(self, "Export", "No results to export.")
            return
        
        try:
            from .export_dialog import ExportDialog
            
            # Get search parameters
            search_query = self.name_input.text().strip()
            entity_type = self.entity_type_combo.currentText()
            
            # Create and show export dialog
            export_dialog = ExportDialog(
                matches=matches,
                search_query=search_query,
                entity_type=entity_type,
                parent=self
            )
            
            export_dialog.exec()
            
        except ImportError as e:
            QMessageBox.warning(
                self, 
                "Export Unavailable", 
                f"Export functionality is not available: {e}\nPlease ensure all dependencies are installed."
            )
        except Exception as e:
            QMessageBox.critical(
                self, 
                "Export Error", 
                f"An error occurred while opening export dialog:\n{str(e)}"
            )
    
    def _update_data(self):
        """Update sanctions data (placeholder)."""
        QMessageBox.information(self, "Update Data", "Data update functionality will be implemented in a future task.")
    
    def _verify_report(self):
        """Show report verification dialog."""
        try:
            from .verification_dialog import VerificationDialog
            
            dialog = VerificationDialog(self)
            dialog.exec()
            
        except ImportError as e:
            QMessageBox.warning(
                self, 
                "Verification Unavailable", 
                f"Report verification functionality is not available: {e}"
            )
        except Exception as e:
            QMessageBox.critical(
                self, 
                "Verification Error", 
                f"An error occurred while opening verification dialog:\n{str(e)}"
            )
    
    def _show_settings(self):
        """Show settings tab."""
        # Find the tabs widget and switch to settings tab
        tabs_widget = self.findChild(QTabWidget)
        if tabs_widget:
            # Find the settings tab index
            for i in range(tabs_widget.count()):
                if tabs_widget.tabText(i) == "Settings":
                    tabs_widget.setCurrentIndex(i)
                    break
    
    def _upload_logo(self):
        """Show logo upload dialog."""
        dialog = LogoUploadDialog(self)
        dialog.logo_updated.connect(self._on_logo_updated)
        dialog.exec()
    
    def _on_logo_updated(self):
        """Handle logo update event."""
        # Refresh the logo display in the main window
        self._update_logo_display()
        self.status_bar.showMessage("Logo updated successfully", 3000)
    
    def _update_logo_display(self):
        """Update the logo display in the main window."""
        try:
            # Update the window icon
            icon = resource_manager.get_application_icon()
            self.setWindowIcon(icon)
            
            # Update logo in search panel if it exists
            if hasattr(self, 'logo_label'):
                logo_pixmap = resource_manager.get_logo_pixmap(150, 60)
                if logo_pixmap and not logo_pixmap.isNull():
                    self.logo_label.setPixmap(logo_pixmap)
        except Exception as e:
            logger.error(f"Error updating logo display: {e}")
    
    def _show_about(self):
        """Show about dialog."""
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton
        from PyQt6.QtCore import QUrl
        from PyQt6.QtGui import QDesktopServices, QPixmap
        
        dialog = QDialog(self)
        dialog.setWindowTitle("About Sanctions Checker")
        dialog.setFixedSize(400, 300)
        
        layout = QVBoxLayout(dialog)
        
        # Title
        title_label = QLabel("Sanctions Checker v1.0")
        title_font = QFont()
        title_font.setPointSize(16)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)
        
        # Description
        desc_label = QLabel(
            "A comprehensive sanctions screening application that provides "
            "automated data acquisition, advanced fuzzy matching algorithms, "
            "and detailed reporting with cryptographic verification.\n\n"
            "Built with Python and PyQt6."
        )
        desc_label.setWordWrap(True)
        desc_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(desc_label)
        
        # Buy Me a Coffee section
        coffee_frame = QFrame()
        coffee_frame.setFrameStyle(QFrame.Shape.Box)
        coffee_layout = QVBoxLayout(coffee_frame)
        
        coffee_label = QLabel("☕ Support the Developer")
        coffee_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        coffee_font = QFont()
        coffee_font.setPointSize(12)
        coffee_font.setBold(True)
        coffee_label.setFont(coffee_font)
        coffee_layout.addWidget(coffee_label)
        
        support_label = QLabel("If you find this application useful, consider buying me a coffee!")
        support_label.setWordWrap(True)
        support_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        coffee_layout.addWidget(support_label)
        
        coffee_button = QPushButton("☕ Buy Me a Coffee")
        coffee_button.clicked.connect(self._open_coffee_link)
        coffee_button.setStyleSheet("""
            QPushButton {
                background-color: #FFDD00;
                border: 2px solid #FF813F;
                border-radius: 8px;
                padding: 8px 16px;
                font-weight: bold;
                color: #000000;
            }
            QPushButton:hover {
                background-color: #FF813F;
                color: #FFFFFF;
            }
        """)
        coffee_layout.addWidget(coffee_button)
        
        layout.addWidget(coffee_frame)
        
        # Close button
        close_button = QPushButton("Close")
        close_button.clicked.connect(dialog.accept)
        layout.addWidget(close_button)
        
        dialog.exec()
    
    def _show_custom_sanctions(self):
        """Show the custom sanctions management tab."""
        tabs_widget = self.findChild(QTabWidget)
        if tabs_widget:
            # Find the custom sanctions tab index
            for i in range(tabs_widget.count()):
                if "Custom Sanctions" in tabs_widget.tabText(i):
                    tabs_widget.setCurrentIndex(i)
                    break
    
    def _import_custom_sanctions(self):
        """Show custom sanctions import dialog."""
        try:
            if not self.search_service or not self.search_service.db_manager:
                QMessageBox.warning(
                    self, 
                    "Import Unavailable", 
                    "Database connection is not available for custom sanctions import."
                )
                return
            
            from ..services.custom_sanctions_service import CustomSanctionsService
            from .custom_sanctions_import_dialog import CustomSanctionsImportDialog
            
            custom_service = CustomSanctionsService(self.search_service.db_manager)
            dialog = CustomSanctionsImportDialog(custom_service, self)
            
            if dialog.exec():
                # Refresh custom sanctions widget if it exists
                if hasattr(self, 'custom_sanctions_widget'):
                    self.custom_sanctions_widget.refresh_entity_list()
                
                self.status_bar.showMessage("Custom sanctions imported successfully", 3000)
            
        except Exception as e:
            logger.error(f"Error opening custom sanctions import dialog: {e}")
            QMessageBox.critical(
                self,
                "Import Error",
                f"Failed to open custom sanctions import dialog:\n{str(e)}"
            )
    
    def _export_custom_sanctions(self):
        """Show custom sanctions export dialog."""
        try:
            if not self.search_service or not self.search_service.db_manager:
                QMessageBox.warning(
                    self, 
                    "Export Unavailable", 
                    "Database connection is not available for custom sanctions export."
                )
                return
            
            from ..services.custom_sanctions_service import CustomSanctionsService
            from .custom_sanctions_export_dialog import CustomSanctionsExportDialog
            
            custom_service = CustomSanctionsService(self.search_service.db_manager)
            dialog = CustomSanctionsExportDialog(custom_service, self)
            
            if dialog.exec():
                self.status_bar.showMessage("Custom sanctions exported successfully", 3000)
            
        except Exception as e:
            logger.error(f"Error opening custom sanctions export dialog: {e}")
            QMessageBox.critical(
                self,
                "Export Error",
                f"Failed to open custom sanctions export dialog:\n{str(e)}"
            )
    
    def _open_coffee_link(self):
        """Open the Buy Me a Coffee link in the default browser."""
        from PyQt6.QtCore import QUrl
        from PyQt6.QtGui import QDesktopServices
        
        url = QUrl("https://buymeacoffee.com/eliesbazine")
        QDesktopServices.openUrl(url)
    
    def _replay_search(self, query: str, entity_type: str, search_record_id: str):
        """Replay a search from history."""
        # Set the search parameters
        self.name_input.setText(query)
        
        # Set entity type
        if entity_type in ["All", "Individual", "Company"]:
            index = self.entity_type_combo.findText(entity_type)
            if index >= 0:
                self.entity_type_combo.setCurrentIndex(index)
        
        # Switch to results tab
        tabs_widget = self.findChild(QTabWidget)
        if tabs_widget:
            for i in range(tabs_widget.count()):
                if tabs_widget.tabText(i) == "Results":
                    tabs_widget.setCurrentIndex(i)
                    break
        
        # Perform the search
        self._perform_search()
        
        # Show info message
        QMessageBox.information(
            self, 
            "Search Replayed", 
            f"Replaying search for: {query}\n\nThis will use current sanctions data, so results may differ from the original search."
        )
    
    def _compare_searches(self, search_record_ids: List[str]):
        """Launch batch search dialog with selected searches pre-filtered."""
        try:
            # Open batch search dialog
            dialog = BatchSearchDialog(self.search_service, self.db_manager, self)
            
            # If specific searches were selected, we could pre-filter them
            # For now, just open the dialog and let user select
            dialog.exec()
            
        except Exception as e:
            logger.error(f"Error opening batch search dialog: {e}")
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.critical(
                self,
                "Batch Search Error",
                f"Failed to open batch search dialog:\n{str(e)}\n\nPlease check that all required components are installed."
            )
    
    def _batch_search(self):
        """Show batch search dialog."""
        try:
            dialog = BatchSearchDialog(self.search_service, self.db_manager, self)
            dialog.exec()
        except Exception as e:
            logger.error(f"Error opening batch search dialog: {e}")
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.critical(
                self,
                "Batch Search Error",
                f"Failed to open batch search dialog:\n{str(e)}"
            )
    
    def _batch_export(self):
        """Open batch export dialog."""
        try:
            from .export_dialog import ExportDialog
            from ..models.search_record import SearchRecord
            
            if not self.search_service or not self.search_service.db_manager:
                QMessageBox.warning(
                    self, 
                    "Batch Export Unavailable", 
                    "Database connection is not available for batch export."
                )
                return
            
            # Get recent search records
            session = self.search_service.db_manager.get_session()
            try:
                search_records = session.query(SearchRecord).order_by(
                    SearchRecord.search_timestamp.desc()
                ).limit(50).all()
                
                if not search_records:
                    QMessageBox.information(
                        self, 
                        "No Records", 
                        "No search records found for batch export."
                    )
                    return
                
                # Create and show batch export dialog
                export_dialog = ExportDialog(
                    search_records=search_records,
                    parent=self
                )
                
                export_dialog.exec()
                
            finally:
                session.close()
            
        except ImportError as e:
            QMessageBox.warning(
                self, 
                "Batch Export Unavailable", 
                f"Batch export functionality is not available: {e}"
            )
        except Exception as e:
            QMessageBox.critical(
                self, 
                "Batch Export Error", 
                f"An error occurred while opening batch export dialog:\n{str(e)}"
            )
    

    
    def closeEvent(self, event):
        """Handle application close event."""
        # Cancel any running search
        if self.search_worker and self.search_worker.isRunning():
            self.search_worker.cancel()
            self.search_worker.wait(3000)  # Wait up to 3 seconds
        
        # Save configuration
        self.config.save()
        event.accept()