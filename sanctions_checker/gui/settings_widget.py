"""
Settings widget for the Sanctions Checker application.
Provides configuration interface for thresholds, data sources, and user preferences.
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, QSpinBox, QDoubleSpinBox,
    QCheckBox, QGroupBox, QTabWidget, QScrollArea, QFrame, QMessageBox,
    QFileDialog, QTableWidget, QTableWidgetItem, QHeaderView, QSplitter,
    QTextEdit, QSlider, QProgressBar
)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, pyqtSlot
from PyQt6.QtGui import QFont, QValidator, QDoubleValidator, QIntValidator

from sanctions_checker.config import Config
from sanctions_checker.utils.resources import resource_manager
from .logo_upload_dialog import LogoUploadDialog

logger = logging.getLogger(__name__)


class ThresholdValidator(QDoubleValidator):
    """Custom validator for threshold values (0.0 to 1.0)."""
    
    def __init__(self):
        super().__init__(0.0, 1.0, 2)
        self.setNotation(QDoubleValidator.Notation.StandardNotation)


class SettingsWidget(QWidget):
    """Settings configuration widget."""
    
    # Signals
    settings_changed = pyqtSignal(str, object)  # key, value
    settings_saved = pyqtSignal()
    settings_reset = pyqtSignal()
    validation_error = pyqtSignal(str, str)  # field_name, error_message
    
    def __init__(self, config: Config, parent=None):
        """Initialize settings widget.
        
        Args:
            config: Application configuration instance
            parent: Parent widget
        """
        super().__init__(parent)
        self.config = config
        self.original_config = {}  # Store original values for reset
        self.validation_timer = QTimer()
        self.validation_timer.setSingleShot(True)
        self.validation_timer.timeout.connect(self._validate_all_settings)
        
        # Track unsaved changes
        self.has_unsaved_changes = False
        self.field_validators = {}
        
        self.setup_ui()
        self.load_current_settings()
        self.setup_connections()
    
    def setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        # Header
        header_layout = QHBoxLayout()
        
        title = QLabel("Settings")
        title_font = QFont()
        title_font.setPointSize(16)
        title_font.setBold(True)
        title.setFont(title_font)
        header_layout.addWidget(title)
        
        header_layout.addStretch()
        
        # Action buttons
        self.save_button = QPushButton("Save")
        self.save_button.setEnabled(False)
        self.reset_button = QPushButton("Reset")
        self.defaults_button = QPushButton("Restore Defaults")
        
        header_layout.addWidget(self.save_button)
        header_layout.addWidget(self.reset_button)
        header_layout.addWidget(self.defaults_button)
        
        layout.addLayout(header_layout)
        
        # Status/validation message area
        self.status_label = QLabel()
        self.status_label.setStyleSheet("color: green; font-weight: bold;")
        self.status_label.setVisible(False)
        layout.addWidget(self.status_label)
        
        self.error_label = QLabel()
        self.error_label.setStyleSheet("color: red; font-weight: bold;")
        self.error_label.setWordWrap(True)
        self.error_label.setVisible(False)
        layout.addWidget(self.error_label)
        
        # Create tabbed interface for different setting categories
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        
        # Create individual setting tabs
        self._create_matching_tab()
        self._create_data_sources_tab()
        self._create_branding_tab()
        self._create_preferences_tab()
        self._create_advanced_tab()
        self._create_support_tab()
    
    def _create_matching_tab(self):
        """Create matching algorithms configuration tab."""
        tab = QScrollArea()
        tab_widget = QWidget()
        tab_layout = QVBoxLayout(tab_widget)
        
        # Threshold Configuration Group
        threshold_group = QGroupBox("Matching Thresholds")
        threshold_layout = QFormLayout(threshold_group)
        
        # Levenshtein threshold
        self.levenshtein_threshold = QDoubleSpinBox()
        self.levenshtein_threshold.setRange(0.0, 1.0)
        self.levenshtein_threshold.setSingleStep(0.05)
        self.levenshtein_threshold.setDecimals(2)
        self.levenshtein_threshold.setSuffix(" (0.0-1.0)")
        threshold_layout.addRow("Levenshtein Distance Threshold:", self.levenshtein_threshold)
        
        # Jaro-Winkler threshold
        self.jaro_winkler_threshold = QDoubleSpinBox()
        self.jaro_winkler_threshold.setRange(0.0, 1.0)
        self.jaro_winkler_threshold.setSingleStep(0.05)
        self.jaro_winkler_threshold.setDecimals(2)
        self.jaro_winkler_threshold.setSuffix(" (0.0-1.0)")
        threshold_layout.addRow("Jaro-Winkler Threshold:", self.jaro_winkler_threshold)
        
        # Company vs Individual thresholds
        self.company_threshold = QDoubleSpinBox()
        self.company_threshold.setRange(0.0, 1.0)
        self.company_threshold.setSingleStep(0.05)
        self.company_threshold.setDecimals(2)
        self.company_threshold.setSuffix(" (0.0-1.0)")
        threshold_layout.addRow("Company Matching Threshold:", self.company_threshold)
        
        self.individual_threshold = QDoubleSpinBox()
        self.individual_threshold.setRange(0.0, 1.0)
        self.individual_threshold.setSingleStep(0.05)
        self.individual_threshold.setDecimals(2)
        self.individual_threshold.setSuffix(" (0.0-1.0)")
        threshold_layout.addRow("Individual Matching Threshold:", self.individual_threshold)
        
        tab_layout.addWidget(threshold_group)
        
        # Algorithm Configuration Group
        algorithm_group = QGroupBox("Algorithm Settings")
        algorithm_layout = QFormLayout(algorithm_group)
        
        # Soundex enabled
        self.soundex_enabled = QCheckBox("Enable Soundex phonetic matching")
        algorithm_layout.addRow(self.soundex_enabled)
        
        tab_layout.addWidget(algorithm_group)
        
        # Threshold visualization (sliders for better UX)
        visual_group = QGroupBox("Threshold Visualization")
        visual_layout = QGridLayout(visual_group)
        
        # Create sliders that sync with spin boxes
        self.levenshtein_slider = QSlider(Qt.Orientation.Horizontal)
        self.levenshtein_slider.setRange(0, 100)
        self.levenshtein_slider.setTickPosition(QSlider.TickPosition.TicksBelow)
        self.levenshtein_slider.setTickInterval(10)
        visual_layout.addWidget(QLabel("Levenshtein:"), 0, 0)
        visual_layout.addWidget(self.levenshtein_slider, 0, 1)
        
        self.jaro_winkler_slider = QSlider(Qt.Orientation.Horizontal)
        self.jaro_winkler_slider.setRange(0, 100)
        self.jaro_winkler_slider.setTickPosition(QSlider.TickPosition.TicksBelow)
        self.jaro_winkler_slider.setTickInterval(10)
        visual_layout.addWidget(QLabel("Jaro-Winkler:"), 1, 0)
        visual_layout.addWidget(self.jaro_winkler_slider, 1, 1)
        
        tab_layout.addWidget(visual_group)
        
        tab_layout.addStretch()
        tab.setWidget(tab_widget)
        tab.setWidgetResizable(True)
        self.tabs.addTab(tab, "Matching")
    
    def _create_data_sources_tab(self):
        """Create data sources configuration tab."""
        tab = QScrollArea()
        tab_widget = QWidget()
        tab_layout = QVBoxLayout(tab_widget)
        
        # Data Sources Table
        sources_group = QGroupBox("Sanctions Data Sources")
        sources_layout = QVBoxLayout(sources_group)
        
        # Table for data sources
        self.sources_table = QTableWidget()
        self.sources_table.setColumnCount(4)
        self.sources_table.setHorizontalHeaderLabels(["Source", "URL", "Format", "Enabled"])
        
        # Make table responsive
        header = self.sources_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        
        sources_layout.addWidget(self.sources_table)
        
        # Buttons for data source management
        sources_buttons = QHBoxLayout()
        self.add_source_button = QPushButton("Add Source")
        self.edit_source_button = QPushButton("Edit Source")
        self.remove_source_button = QPushButton("Remove Source")
        self.test_source_button = QPushButton("Test Connection")
        
        sources_buttons.addWidget(self.add_source_button)
        sources_buttons.addWidget(self.edit_source_button)
        sources_buttons.addWidget(self.remove_source_button)
        sources_buttons.addWidget(self.test_source_button)
        sources_buttons.addStretch()
        
        sources_layout.addLayout(sources_buttons)
        tab_layout.addWidget(sources_group)
        
        # Update Configuration Group
        update_group = QGroupBox("Automatic Updates")
        update_layout = QFormLayout(update_group)
        
        # Auto update enabled
        self.auto_update_enabled = QCheckBox("Enable automatic data updates")
        update_layout.addRow(self.auto_update_enabled)
        
        # Update interval
        self.update_interval = QSpinBox()
        self.update_interval.setRange(1, 168)  # 1 hour to 1 week
        self.update_interval.setSuffix(" hours")
        update_layout.addRow("Update Interval:", self.update_interval)
        
        # Retry settings
        self.retry_attempts = QSpinBox()
        self.retry_attempts.setRange(1, 10)
        update_layout.addRow("Retry Attempts:", self.retry_attempts)
        
        self.retry_delay = QSpinBox()
        self.retry_delay.setRange(30, 3600)  # 30 seconds to 1 hour
        self.retry_delay.setSuffix(" seconds")
        update_layout.addRow("Retry Delay:", self.retry_delay)
        
        tab_layout.addWidget(update_group)
        
        tab_layout.addStretch()
        tab.setWidget(tab_widget)
        tab.setWidgetResizable(True)
        self.tabs.addTab(tab, "Data Sources")
    
    def _create_branding_tab(self):
        """Create branding and logo configuration tab."""
        tab = QScrollArea()
        tab_widget = QWidget()
        tab_layout = QVBoxLayout(tab_widget)
        tab_layout.setSpacing(15)
        
        # Logo Management Section
        logo_group = QGroupBox("Company Logo")
        logo_layout = QVBoxLayout(logo_group)
        
        # Current logo display
        logo_display_layout = QHBoxLayout()
        
        # Logo preview
        self.logo_preview_label = QLabel()
        self.logo_preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.logo_preview_label.setMinimumHeight(100)
        self.logo_preview_label.setStyleSheet("border: 1px solid #ccc; background: white; padding: 10px;")
        logo_display_layout.addWidget(self.logo_preview_label)
        
        # Logo info and controls
        logo_info_layout = QVBoxLayout()
        
        self.logo_status_label = QLabel()
        self.logo_status_label.setWordWrap(True)
        logo_info_layout.addWidget(self.logo_status_label)
        
        # Logo management buttons
        logo_buttons_layout = QHBoxLayout()
        
        self.upload_logo_btn = QPushButton("📁 Upload Logo")
        self.upload_logo_btn.clicked.connect(self.upload_logo)
        logo_buttons_layout.addWidget(self.upload_logo_btn)
        
        self.reset_logo_btn = QPushButton("🔄 Reset Logo")
        self.reset_logo_btn.clicked.connect(self.reset_logo)
        logo_buttons_layout.addWidget(self.reset_logo_btn)
        
        logo_info_layout.addLayout(logo_buttons_layout)
        logo_info_layout.addStretch()
        
        logo_display_layout.addLayout(logo_info_layout)
        logo_layout.addLayout(logo_display_layout)
        
        # Logo usage information
        usage_info = QLabel(
            "Your logo will appear in:\n"
            "• Application window and title bar\n"
            "• PDF reports and exports\n"
            "• Main application interface\n"
            "• About dialog and documentation"
        )
        usage_info.setStyleSheet("color: #666; font-size: 11px; padding: 10px; background: #f9f9f9; border-radius: 5px;")
        logo_layout.addWidget(usage_info)
        
        tab_layout.addWidget(logo_group)
        
        # Company Information Section
        company_group = QGroupBox("Company Information")
        company_layout = QFormLayout(company_group)
        
        self.company_name_edit = QLineEdit()
        self.company_name_edit.setPlaceholderText("Enter your company name")
        company_layout.addRow("Company Name:", self.company_name_edit)
        
        self.company_address_edit = QTextEdit()
        self.company_address_edit.setMaximumHeight(80)
        self.company_address_edit.setPlaceholderText("Enter company address (optional)")
        company_layout.addRow("Address:", self.company_address_edit)
        
        self.company_contact_edit = QLineEdit()
        self.company_contact_edit.setPlaceholderText("Email or phone (optional)")
        company_layout.addRow("Contact:", self.company_contact_edit)
        
        self.user_name_edit = QLineEdit()
        self.user_name_edit.setPlaceholderText("Enter your name for PDF reports")
        company_layout.addRow("User Name:", self.user_name_edit)
        
        self.user_id_edit = QLineEdit()
        self.user_id_edit.setPlaceholderText("Enter your user ID for search tracking")
        company_layout.addRow("User ID:", self.user_id_edit)
        
        tab_layout.addWidget(company_group)
        
        # PDF Report Branding Section
        pdf_group = QGroupBox("PDF Report Settings")
        pdf_layout = QFormLayout(pdf_group)
        
        self.include_logo_pdf_check = QCheckBox("Include logo in PDF reports")
        self.include_logo_pdf_check.setChecked(True)
        pdf_layout.addRow("Logo in PDFs:", self.include_logo_pdf_check)
        
        self.include_company_info_check = QCheckBox("Include company information in PDF reports")
        self.include_company_info_check.setChecked(True)
        pdf_layout.addRow("Company Info:", self.include_company_info_check)
        
        tab_layout.addWidget(pdf_group)
        
        tab_layout.addStretch()
        
        tab.setWidget(tab_widget)
        tab.setWidgetResizable(True)
        self.tabs.addTab(tab, "🎨 Branding")
        
        # Update logo preview
        self.update_logo_preview()
    
    def _create_preferences_tab(self):
        """Create user preferences configuration tab."""
        tab = QScrollArea()
        tab_widget = QWidget()
        tab_layout = QVBoxLayout(tab_widget)
        
        # Report Settings Group
        report_group = QGroupBox("Report Settings")
        report_layout = QFormLayout(report_group)
        
        # Default export format
        self.default_format = QComboBox()
        self.default_format.addItems(["PDF", "CSV", "JSON", "XML"])
        report_layout.addRow("Default Export Format:", self.default_format)
        
        # Report options
        self.include_algorithm_details = QCheckBox("Include algorithm details in reports")
        report_layout.addRow(self.include_algorithm_details)
        
        self.include_verification_hash = QCheckBox("Include verification hash in reports")
        report_layout.addRow(self.include_verification_hash)
        
        tab_layout.addWidget(report_group)
        
        # Audit Settings Group
        audit_group = QGroupBox("Audit and Retention")
        audit_layout = QFormLayout(audit_group)
        
        # Data retention period
        self.retention_days = QSpinBox()
        self.retention_days.setRange(1, 3650)  # 1 day to 10 years
        self.retention_days.setSuffix(" days")
        audit_layout.addRow("Search History Retention:", self.retention_days)
        
        # Logging settings
        self.log_searches = QCheckBox("Log all searches for audit trail")
        audit_layout.addRow(self.log_searches)
        
        self.log_level = QComboBox()
        self.log_level.addItems(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
        audit_layout.addRow("Log Level:", self.log_level)
        
        tab_layout.addWidget(audit_group)
        
        # GUI Settings Group
        gui_group = QGroupBox("Interface Settings")
        gui_layout = QFormLayout(gui_group)
        
        # Theme selection
        self.theme = QComboBox()
        self.theme.addItems(["Default", "Dark", "Light"])
        gui_layout.addRow("Theme:", self.theme)
        
        # Window size settings
        self.window_width = QSpinBox()
        self.window_width.setRange(800, 3840)
        self.window_width.setSuffix(" px")
        gui_layout.addRow("Default Window Width:", self.window_width)
        
        self.window_height = QSpinBox()
        self.window_height.setRange(600, 2160)
        self.window_height.setSuffix(" px")
        gui_layout.addRow("Default Window Height:", self.window_height)
        
        # Auto-save searches
        self.auto_save_searches = QCheckBox("Automatically save search results")
        gui_layout.addRow(self.auto_save_searches)
        
        tab_layout.addWidget(gui_group)
        
        # Tag Management Group
        tag_group = QGroupBox("Search Tags Management")
        tag_layout = QVBoxLayout(tag_group)
        
        # Tag description
        tag_desc = QLabel(
            "Create tags to categorize your searches by project, team, or purpose. "
            "Tags can be selected during searches and used for filtering search history."
        )
        tag_desc.setWordWrap(True)
        tag_desc.setStyleSheet("color: #666; font-size: 11px; padding: 5px;")
        tag_layout.addWidget(tag_desc)
        
        # Tag input and management
        tag_input_layout = QHBoxLayout()
        
        self.new_tag_input = QLineEdit()
        self.new_tag_input.setPlaceholderText("Enter new tag name (e.g., 'Project Alpha', 'Compliance Team')")
        tag_input_layout.addWidget(self.new_tag_input)
        
        self.add_tag_btn = QPushButton("Add Tag")
        self.add_tag_btn.clicked.connect(self.add_tag)
        tag_input_layout.addWidget(self.add_tag_btn)
        
        tag_layout.addLayout(tag_input_layout)
        
        # Tags list
        self.tags_table = QTableWidget()
        self.tags_table.setColumnCount(2)
        self.tags_table.setHorizontalHeaderLabels(["Tag Name", "Actions"])
        self.tags_table.setMaximumHeight(150)
        
        # Make table responsive
        header = self.tags_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        
        tag_layout.addWidget(self.tags_table)
        
        tab_layout.addWidget(tag_group)
        
        tab_layout.addStretch()
        tab.setWidget(tab_widget)
        tab.setWidgetResizable(True)
        self.tabs.addTab(tab, "Preferences")
    
    def _create_advanced_tab(self):
        """Create advanced configuration tab."""
        tab = QScrollArea()
        tab_widget = QWidget()
        tab_layout = QVBoxLayout(tab_widget)
        
        # Database Settings Group
        db_group = QGroupBox("Database Configuration")
        db_layout = QFormLayout(db_group)
        
        # Database URL
        self.database_url = QLineEdit()
        self.database_url.setPlaceholderText("sqlite:///path/to/database.db")
        db_layout.addRow("Database URL:", self.database_url)
        
        # Database echo (debug SQL)
        self.database_echo = QCheckBox("Enable SQL query logging (debug)")
        db_layout.addRow(self.database_echo)
        
        tab_layout.addWidget(db_group)
        
        # Configuration File Management
        config_group = QGroupBox("Configuration Management")
        config_layout = QVBoxLayout(config_group)
        
        # Current config file path
        config_info_layout = QFormLayout()
        self.config_file_path = QLabel()
        self.config_file_path.setWordWrap(True)
        self.config_file_path.setStyleSheet("font-family: monospace; background-color: #f0f0f0; padding: 5px;")
        config_info_layout.addRow("Config File:", self.config_file_path)
        config_layout.addLayout(config_info_layout)
        
        # Config file actions
        config_buttons = QHBoxLayout()
        self.export_config_button = QPushButton("Export Config")
        self.import_config_button = QPushButton("Import Config")
        self.open_config_folder_button = QPushButton("Open Config Folder")
        
        config_buttons.addWidget(self.export_config_button)
        config_buttons.addWidget(self.import_config_button)
        config_buttons.addWidget(self.open_config_folder_button)
        config_buttons.addStretch()
        
        config_layout.addLayout(config_buttons)
        tab_layout.addWidget(config_group)
        
        # Raw Configuration Editor (for advanced users)
        raw_config_group = QGroupBox("Raw Configuration (Advanced)")
        raw_config_layout = QVBoxLayout(raw_config_group)
        
        warning_label = QLabel("⚠️ Warning: Editing raw configuration can break the application. Use with caution.")
        warning_label.setStyleSheet("color: orange; font-weight: bold;")
        raw_config_layout.addWidget(warning_label)
        
        self.raw_config_editor = QTextEdit()
        self.raw_config_editor.setFont(QFont("Courier", 10))
        self.raw_config_editor.setMaximumHeight(200)
        raw_config_layout.addWidget(self.raw_config_editor)
        
        raw_config_buttons = QHBoxLayout()
        self.load_raw_config_button = QPushButton("Load Current Config")
        self.validate_raw_config_button = QPushButton("Validate JSON")
        self.apply_raw_config_button = QPushButton("Apply Changes")
        
        raw_config_buttons.addWidget(self.load_raw_config_button)
        raw_config_buttons.addWidget(self.validate_raw_config_button)
        raw_config_buttons.addWidget(self.apply_raw_config_button)
        raw_config_buttons.addStretch()
        
        raw_config_layout.addLayout(raw_config_buttons)
        tab_layout.addWidget(raw_config_group)
        
        tab_layout.addStretch()
        tab.setWidget(tab_widget)
        tab.setWidgetResizable(True)
        self.tabs.addTab(tab, "Advanced")
    
    def _create_support_tab(self):
        """Create support and about tab."""
        tab = QScrollArea()
        tab_widget = QWidget()
        tab_layout = QVBoxLayout(tab_widget)
        tab_layout.setSpacing(20)
        
        # Application info section
        app_group = QGroupBox("Application Information")
        app_layout = QVBoxLayout(app_group)
        
        app_title = QLabel("Sanctions Checker v1.0")
        app_title_font = QFont()
        app_title_font.setPointSize(14)
        app_title_font.setBold(True)
        app_title.setFont(app_title_font)
        app_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        app_layout.addWidget(app_title)
        
        app_desc = QLabel(
            "A comprehensive sanctions screening application that provides "
            "automated data acquisition, advanced fuzzy matching algorithms, "
            "and detailed reporting with cryptographic verification."
        )
        app_desc.setWordWrap(True)
        app_desc.setAlignment(Qt.AlignmentFlag.AlignCenter)
        app_layout.addWidget(app_desc)
        
        tab_layout.addWidget(app_group)
        
        # Support section
        support_group = QGroupBox("Support the Developer")
        support_layout = QVBoxLayout(support_group)
        
        support_desc = QLabel(
            "If you find this application useful and it helps with your compliance work, "
            "consider supporting the developer by buying a coffee! Your support helps "
            "maintain and improve this application."
        )
        support_desc.setWordWrap(True)
        support_layout.addWidget(support_desc)
        
        # Coffee button
        coffee_button = QPushButton("☕ Buy Me a Coffee")
        coffee_button.clicked.connect(self._open_coffee_link)
        coffee_button.setStyleSheet("""
            QPushButton {
                background-color: #FFDD00;
                border: 2px solid #FF813F;
                border-radius: 12px;
                padding: 12px 24px;
                font-weight: bold;
                font-size: 14px;
                color: #000000;
                min-height: 20px;
            }
            QPushButton:hover {
                background-color: #FF813F;
                color: #FFFFFF;
                transform: scale(1.05);
            }
            QPushButton:pressed {
                background-color: #E6730F;
            }
        """)
        support_layout.addWidget(coffee_button)
        
        # Benefits of support
        benefits_label = QLabel(
            "Your support helps with:\n"
            "• Maintaining and updating sanctions data sources\n"
            "• Adding new features and improvements\n"
            "• Providing technical support\n"
            "• Keeping the application free and open"
        )
        benefits_label.setStyleSheet("color: #666; font-style: italic;")
        support_layout.addWidget(benefits_label)
        
        tab_layout.addWidget(support_group)
        
        # Technical info section
        tech_group = QGroupBox("Technical Information")
        tech_layout = QFormLayout(tech_group)
        
        tech_layout.addRow("Built with:", QLabel("Python 3.11+ and PyQt6"))
        tech_layout.addRow("Database:", QLabel("SQLite with SQLAlchemy ORM"))
        tech_layout.addRow("Matching Algorithms:", QLabel("Levenshtein, Jaro-Winkler, Soundex"))
        tech_layout.addRow("Report Format:", QLabel("PDF with cryptographic verification"))
        
        tab_layout.addWidget(tech_group)
        
        tab_layout.addStretch()
        tab.setWidget(tab_widget)
        tab.setWidgetResizable(True)
        self.tabs.addTab(tab, "☕ Support")
    
    def _open_coffee_link(self):
        """Open the Buy Me a Coffee link in the default browser."""
        from PyQt6.QtCore import QUrl
        from PyQt6.QtGui import QDesktopServices
        
        url = QUrl("https://buymeacoffee.com/eliesbazine")
        QDesktopServices.openUrl(url)
    
    def add_tag(self):
        """Add a new tag to the list."""
        tag_name = self.new_tag_input.text().strip()
        if not tag_name:
            return
        
        # Get current tags
        current_tags = self.config.get('search.tags', [])
        
        # Check if tag already exists
        if tag_name in current_tags:
            QMessageBox.information(self, "Tag Exists", f"Tag '{tag_name}' already exists.")
            return
        
        # Add new tag
        current_tags.append(tag_name)
        self.config.set('search.tags', current_tags)
        
        # Clear input
        self.new_tag_input.clear()
        
        # Refresh tags table
        self.refresh_tags_table()
        
        # Mark as changed
        self._on_setting_changed()
    
    def remove_tag(self, tag_name: str):
        """Remove a tag from the list."""
        reply = QMessageBox.question(
            self, "Remove Tag", 
            f"Are you sure you want to remove the tag '{tag_name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            current_tags = self.config.get('search.tags', [])
            if tag_name in current_tags:
                current_tags.remove(tag_name)
                self.config.set('search.tags', current_tags)
                self.refresh_tags_table()
                self._on_setting_changed()
    
    def refresh_tags_table(self):
        """Refresh the tags table display."""
        tags = self.config.get('search.tags', [])
        self.tags_table.setRowCount(len(tags))
        
        for row, tag in enumerate(tags):
            # Tag name
            self.tags_table.setItem(row, 0, QTableWidgetItem(tag))
            
            # Remove button
            remove_btn = QPushButton("Remove")
            remove_btn.clicked.connect(lambda checked, t=tag: self.remove_tag(t))
            self.tags_table.setCellWidget(row, 1, remove_btn)
    
    def setup_connections(self):
        """Set up signal-slot connections."""
        # Action buttons
        self.save_button.clicked.connect(self.save_settings)
        self.reset_button.clicked.connect(self.reset_settings)
        self.defaults_button.clicked.connect(self.restore_defaults)
        
        # Threshold controls - sync sliders with spin boxes
        self.levenshtein_threshold.valueChanged.connect(self._sync_levenshtein_slider)
        self.levenshtein_slider.valueChanged.connect(self._sync_levenshtein_spinbox)
        self.jaro_winkler_threshold.valueChanged.connect(self._sync_jaro_winkler_slider)
        self.jaro_winkler_slider.valueChanged.connect(self._sync_jaro_winkler_spinbox)
        
        # Data source management
        self.add_source_button.clicked.connect(self._add_data_source)
        self.edit_source_button.clicked.connect(self._edit_data_source)
        self.remove_source_button.clicked.connect(self._remove_data_source)
        self.test_source_button.clicked.connect(self._test_data_source)
        
        # Configuration management
        self.export_config_button.clicked.connect(self._export_config)
        self.import_config_button.clicked.connect(self._import_config)
        self.open_config_folder_button.clicked.connect(self._open_config_folder)
        
        # Raw config editor
        self.load_raw_config_button.clicked.connect(self._load_raw_config)
        self.validate_raw_config_button.clicked.connect(self._validate_raw_config)
        self.apply_raw_config_button.clicked.connect(self._apply_raw_config)
        
        # Connect all input fields to change detection
        self._connect_change_detection()
        
        # Connect branding fields
        self.company_name_edit.textChanged.connect(self._on_setting_changed)
        self.company_address_edit.textChanged.connect(self._on_setting_changed)
        self.company_contact_edit.textChanged.connect(self._on_setting_changed)
        self.user_name_edit.textChanged.connect(self._on_setting_changed)
        self.user_id_edit.textChanged.connect(self._on_setting_changed)
        
        # Connect tag management
        self.new_tag_input.returnPressed.connect(self.add_tag)
    
    def _connect_change_detection(self):
        """Connect all input fields to change detection."""
        # Matching tab
        self.levenshtein_threshold.valueChanged.connect(self._on_setting_changed)
        self.jaro_winkler_threshold.valueChanged.connect(self._on_setting_changed)
        self.company_threshold.valueChanged.connect(self._on_setting_changed)
        self.individual_threshold.valueChanged.connect(self._on_setting_changed)
        self.soundex_enabled.toggled.connect(self._on_setting_changed)
        
        # Data sources tab
        self.auto_update_enabled.toggled.connect(self._on_setting_changed)
        self.update_interval.valueChanged.connect(self._on_setting_changed)
        self.retry_attempts.valueChanged.connect(self._on_setting_changed)
        self.retry_delay.valueChanged.connect(self._on_setting_changed)
        
        # Preferences tab
        self.default_format.currentTextChanged.connect(self._on_setting_changed)
        self.include_algorithm_details.toggled.connect(self._on_setting_changed)
        self.include_verification_hash.toggled.connect(self._on_setting_changed)
        self.retention_days.valueChanged.connect(self._on_setting_changed)
        self.log_searches.toggled.connect(self._on_setting_changed)
        self.log_level.currentTextChanged.connect(self._on_setting_changed)
        self.theme.currentTextChanged.connect(self._on_setting_changed)
        self.window_width.valueChanged.connect(self._on_setting_changed)
        self.window_height.valueChanged.connect(self._on_setting_changed)
        self.auto_save_searches.toggled.connect(self._on_setting_changed)
        
        # Advanced tab
        self.database_url.textChanged.connect(self._on_setting_changed)
        self.database_echo.toggled.connect(self._on_setting_changed)
    
    def load_current_settings(self):
        """Load current settings from configuration."""
        try:
            # Store original values for reset functionality
            self.original_config = {
                'matching.levenshtein_threshold': self.config.get('matching.levenshtein_threshold'),
                'matching.jaro_winkler_threshold': self.config.get('matching.jaro_winkler_threshold'),
                'matching.company_threshold': self.config.get('matching.company_threshold'),
                'matching.individual_threshold': self.config.get('matching.individual_threshold'),
                'matching.soundex_enabled': self.config.get('matching.soundex_enabled'),
                'updates.auto_update': self.config.get('updates.auto_update'),
                'updates.update_interval_hours': self.config.get('updates.update_interval_hours'),
                'updates.retry_attempts': self.config.get('updates.retry_attempts'),
                'updates.retry_delay_seconds': self.config.get('updates.retry_delay_seconds'),
                'reports.default_format': self.config.get('reports.default_format'),
                'reports.include_algorithm_details': self.config.get('reports.include_algorithm_details'),
                'reports.include_verification_hash': self.config.get('reports.include_verification_hash'),
                'audit.retention_days': self.config.get('audit.retention_days'),
                'audit.log_searches': self.config.get('audit.log_searches'),
                'audit.log_level': self.config.get('audit.log_level'),
                'gui.theme': self.config.get('gui.theme'),
                'gui.window_width': self.config.get('gui.window_width'),
                'gui.window_height': self.config.get('gui.window_height'),
                'gui.auto_save_searches': self.config.get('gui.auto_save_searches'),
                'database.url': self.config.get('database.url'),
                'database.echo': self.config.get('database.echo'),
            }
            
            # Load matching settings
            self.levenshtein_threshold.setValue(self.config.get('matching.levenshtein_threshold', 0.8))
            self.jaro_winkler_threshold.setValue(self.config.get('matching.jaro_winkler_threshold', 0.85))
            self.company_threshold.setValue(self.config.get('matching.company_threshold', 0.75))
            self.individual_threshold.setValue(self.config.get('matching.individual_threshold', 0.8))
            self.soundex_enabled.setChecked(self.config.get('matching.soundex_enabled', True))
            
            # Load data source settings
            self._load_data_sources()
            self.auto_update_enabled.setChecked(self.config.get('updates.auto_update', True))
            self.update_interval.setValue(self.config.get('updates.update_interval_hours', 24))
            self.retry_attempts.setValue(self.config.get('updates.retry_attempts', 3))
            self.retry_delay.setValue(self.config.get('updates.retry_delay_seconds', 300))
            
            # Load preferences
            default_format = self.config.get('reports.default_format', 'pdf').upper()
            index = self.default_format.findText(default_format)
            if index >= 0:
                self.default_format.setCurrentIndex(index)
            
            self.include_algorithm_details.setChecked(self.config.get('reports.include_algorithm_details', True))
            self.include_verification_hash.setChecked(self.config.get('reports.include_verification_hash', True))
            self.retention_days.setValue(self.config.get('audit.retention_days', 365))
            self.log_searches.setChecked(self.config.get('audit.log_searches', True))
            
            log_level = self.config.get('audit.log_level', 'INFO')
            index = self.log_level.findText(log_level)
            if index >= 0:
                self.log_level.setCurrentIndex(index)
            
            theme = self.config.get('gui.theme', 'default').title()
            index = self.theme.findText(theme)
            if index >= 0:
                self.theme.setCurrentIndex(index)
            
            self.window_width.setValue(self.config.get('gui.window_width', 1200))
            self.window_height.setValue(self.config.get('gui.window_height', 800))
            self.auto_save_searches.setChecked(self.config.get('gui.auto_save_searches', True))
            
            # Load advanced settings
            self.database_url.setText(self.config.get('database.url', ''))
            self.database_echo.setChecked(self.config.get('database.echo', False))
            self.config_file_path.setText(str(self.config.config_file))
            
            # Sync sliders with spinboxes
            self._sync_levenshtein_slider()
            self._sync_jaro_winkler_slider()
            
            # Reset change tracking
            self.has_unsaved_changes = False
            self.save_button.setEnabled(False)
            
        except Exception as e:
            logger.error(f"Error loading settings: {e}")
            self._show_error("Settings Load Error", f"Failed to load settings: {str(e)}")
    
    def _load_data_sources(self):
        """Load data sources into the table."""
        try:
            data_sources = self.config.get('data_sources', {})
            self.sources_table.setRowCount(len(data_sources))
            
            for row, (source_name, source_config) in enumerate(data_sources.items()):
                # Source name
                name_item = QTableWidgetItem(source_name)
                name_item.setFlags(name_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.sources_table.setItem(row, 0, name_item)
                
                # URL
                url_item = QTableWidgetItem(source_config.get('url', ''))
                self.sources_table.setItem(row, 1, url_item)
                
                # Format
                format_item = QTableWidgetItem(source_config.get('format', ''))
                self.sources_table.setItem(row, 2, format_item)
                
                # Enabled checkbox
                enabled_checkbox = QCheckBox()
                enabled_checkbox.setChecked(source_config.get('enabled', True))
                enabled_checkbox.toggled.connect(self._on_setting_changed)
                self.sources_table.setCellWidget(row, 3, enabled_checkbox)
            
        except Exception as e:
            logger.error(f"Error loading data sources: {e}")
    
    @pyqtSlot()
    def _on_setting_changed(self):
        """Handle setting change."""
        self.has_unsaved_changes = True
        self.save_button.setEnabled(True)
        self.status_label.setVisible(False)
        
        # Start validation timer (debounce rapid changes)
        self.validation_timer.stop()
        self.validation_timer.start(500)
    
    def _sync_levenshtein_slider(self):
        """Sync Levenshtein slider with spinbox."""
        value = int(self.levenshtein_threshold.value() * 100)
        self.levenshtein_slider.blockSignals(True)
        self.levenshtein_slider.setValue(value)
        self.levenshtein_slider.blockSignals(False)
    
    def _sync_levenshtein_spinbox(self):
        """Sync Levenshtein spinbox with slider."""
        value = self.levenshtein_slider.value() / 100.0
        self.levenshtein_threshold.blockSignals(True)
        self.levenshtein_threshold.setValue(value)
        self.levenshtein_threshold.blockSignals(False)
        self._on_setting_changed()
    
    def _sync_jaro_winkler_slider(self):
        """Sync Jaro-Winkler slider with spinbox."""
        value = int(self.jaro_winkler_threshold.value() * 100)
        self.jaro_winkler_slider.blockSignals(True)
        self.jaro_winkler_slider.setValue(value)
        self.jaro_winkler_slider.blockSignals(False)
    
    def _sync_jaro_winkler_spinbox(self):
        """Sync Jaro-Winkler spinbox with slider."""
        value = self.jaro_winkler_slider.value() / 100.0
        self.jaro_winkler_threshold.blockSignals(True)
        self.jaro_winkler_threshold.setValue(value)
        self.jaro_winkler_threshold.blockSignals(False)
        self._on_setting_changed()
    
    def _validate_all_settings(self):
        """Validate all current settings."""
        errors = []
        
        try:
            # Validate thresholds
            if self.levenshtein_threshold.value() < 0.0 or self.levenshtein_threshold.value() > 1.0:
                errors.append("Levenshtein threshold must be between 0.0 and 1.0")
            
            if self.jaro_winkler_threshold.value() < 0.0 or self.jaro_winkler_threshold.value() > 1.0:
                errors.append("Jaro-Winkler threshold must be between 0.0 and 1.0")
            
            if self.company_threshold.value() < 0.0 or self.company_threshold.value() > 1.0:
                errors.append("Company threshold must be between 0.0 and 1.0")
            
            if self.individual_threshold.value() < 0.0 or self.individual_threshold.value() > 1.0:
                errors.append("Individual threshold must be between 0.0 and 1.0")
            
            # Validate update settings
            if self.update_interval.value() < 1:
                errors.append("Update interval must be at least 1 hour")
            
            if self.retry_attempts.value() < 1:
                errors.append("Retry attempts must be at least 1")
            
            if self.retry_delay.value() < 30:
                errors.append("Retry delay must be at least 30 seconds")
            
            # Validate retention period
            if self.retention_days.value() < 1:
                errors.append("Retention period must be at least 1 day")
            
            # Validate window dimensions
            if self.window_width.value() < 800:
                errors.append("Window width must be at least 800 pixels")
            
            if self.window_height.value() < 600:
                errors.append("Window height must be at least 600 pixels")
            
            # Validate database URL
            db_url = self.database_url.text().strip()
            if db_url and not (db_url.startswith('sqlite://') or db_url.startswith('postgresql://') or db_url.startswith('mysql://')):
                errors.append("Database URL must start with sqlite://, postgresql://, or mysql://")
            
            # Display validation results
            if errors:
                self.error_label.setText("Validation errors:\n• " + "\n• ".join(errors))
                self.error_label.setVisible(True)
                self.save_button.setEnabled(False)
            else:
                self.error_label.setVisible(False)
                if self.has_unsaved_changes:
                    self.save_button.setEnabled(True)
            
        except Exception as e:
            logger.error(f"Error during validation: {e}")
            self.error_label.setText(f"Validation error: {str(e)}")
            self.error_label.setVisible(True)
            self.save_button.setEnabled(False)
    
    def save_settings(self):
        """Save all settings to configuration."""
        try:
            # Validate before saving
            self._validate_all_settings()
            if self.error_label.isVisible():
                return
            
            # Save matching settings
            self.config.set('matching.levenshtein_threshold', self.levenshtein_threshold.value())
            self.config.set('matching.jaro_winkler_threshold', self.jaro_winkler_threshold.value())
            self.config.set('matching.company_threshold', self.company_threshold.value())
            self.config.set('matching.individual_threshold', self.individual_threshold.value())
            self.config.set('matching.soundex_enabled', self.soundex_enabled.isChecked())
            
            # Save data source settings
            self._save_data_sources()
            self.config.set('updates.auto_update', self.auto_update_enabled.isChecked())
            self.config.set('updates.update_interval_hours', self.update_interval.value())
            self.config.set('updates.retry_attempts', self.retry_attempts.value())
            self.config.set('updates.retry_delay_seconds', self.retry_delay.value())
            
            # Save preferences
            self.config.set('reports.default_format', self.default_format.currentText().lower())
            self.config.set('reports.include_algorithm_details', self.include_algorithm_details.isChecked())
            self.config.set('reports.include_verification_hash', self.include_verification_hash.isChecked())
            self.config.set('audit.retention_days', self.retention_days.value())
            self.config.set('audit.log_searches', self.log_searches.isChecked())
            self.config.set('audit.log_level', self.log_level.currentText())
            self.config.set('gui.theme', self.theme.currentText().lower())
            self.config.set('gui.window_width', self.window_width.value())
            self.config.set('gui.window_height', self.window_height.value())
            self.config.set('gui.auto_save_searches', self.auto_save_searches.isChecked())
            
            # Save advanced settings
            self.config.set('database.url', self.database_url.text().strip())
            self.config.set('database.echo', self.database_echo.isChecked())
            
            # Persist to file
            self.config.save()
            
            # Update UI state
            self.has_unsaved_changes = False
            self.save_button.setEnabled(False)
            self.status_label.setText("Settings saved successfully!")
            self.status_label.setVisible(True)
            
            # Hide status message after 3 seconds
            QTimer.singleShot(3000, lambda: self.status_label.setVisible(False))
            
            # Emit signal
            self.settings_saved.emit()
            
        except Exception as e:
            logger.error(f"Error saving settings: {e}")
            self._show_error("Save Error", f"Failed to save settings: {str(e)}")
    
    def _save_data_sources(self):
        """Save data sources from table to configuration."""
        try:
            data_sources = {}
            
            for row in range(self.sources_table.rowCount()):
                name_item = self.sources_table.item(row, 0)
                url_item = self.sources_table.item(row, 1)
                format_item = self.sources_table.item(row, 2)
                enabled_widget = self.sources_table.cellWidget(row, 3)
                
                if name_item and url_item and format_item and enabled_widget:
                    source_name = name_item.text()
                    data_sources[source_name] = {
                        'url': url_item.text(),
                        'format': format_item.text(),
                        'enabled': enabled_widget.isChecked()
                    }
            
            self.config.set('data_sources', data_sources)
            
        except Exception as e:
            logger.error(f"Error saving data sources: {e}")
            raise
    
    def reset_settings(self):
        """Reset settings to previously saved values."""
        try:
            # Reload from original config
            for key, value in self.original_config.items():
                self.config.set(key, value)
            
            # Reload UI
            self.load_current_settings()
            
            self.status_label.setText("Settings reset to last saved values!")
            self.status_label.setVisible(True)
            QTimer.singleShot(3000, lambda: self.status_label.setVisible(False))
            
            self.settings_reset.emit()
            
        except Exception as e:
            logger.error(f"Error resetting settings: {e}")
            self._show_error("Reset Error", f"Failed to reset settings: {str(e)}")
    
    def restore_defaults(self):
        """Restore all settings to default values."""
        reply = QMessageBox.question(
            self,
            "Restore Defaults",
            "Are you sure you want to restore all settings to their default values? This will overwrite all current settings.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                # Reset config to defaults
                self.config._config_data = self.config._defaults.copy()
                
                # Reload UI
                self.load_current_settings()
                
                self.status_label.setText("Settings restored to defaults!")
                self.status_label.setVisible(True)
                QTimer.singleShot(3000, lambda: self.status_label.setVisible(False))
                
                self.settings_reset.emit()
                
            except Exception as e:
                logger.error(f"Error restoring defaults: {e}")
                self._show_error("Restore Error", f"Failed to restore defaults: {str(e)}")
    
    def _add_data_source(self):
        """Add a new data source."""
        # This would open a dialog to add a new data source
        QMessageBox.information(self, "Add Data Source", "Data source management dialog will be implemented.")
    
    def _edit_data_source(self):
        """Edit selected data source."""
        current_row = self.sources_table.currentRow()
        if current_row >= 0:
            QMessageBox.information(self, "Edit Data Source", f"Edit data source at row {current_row + 1}")
        else:
            QMessageBox.information(self, "Edit Data Source", "Please select a data source to edit.")
    
    def _remove_data_source(self):
        """Remove selected data source."""
        current_row = self.sources_table.currentRow()
        if current_row >= 0:
            reply = QMessageBox.question(
                self,
                "Remove Data Source",
                "Are you sure you want to remove this data source?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                self.sources_table.removeRow(current_row)
                self._on_setting_changed()
        else:
            QMessageBox.information(self, "Remove Data Source", "Please select a data source to remove.")
    
    def _test_data_source(self):
        """Test connection to selected data source."""
        current_row = self.sources_table.currentRow()
        if current_row >= 0:
            url_item = self.sources_table.item(current_row, 1)
            if url_item:
                url = url_item.text()
                QMessageBox.information(self, "Test Connection", f"Testing connection to: {url}\n(Test functionality will be implemented)")
        else:
            QMessageBox.information(self, "Test Connection", "Please select a data source to test.")
    
    def _export_config(self):
        """Export configuration to file."""
        try:
            filename, _ = QFileDialog.getSaveFileName(
                self,
                "Export Configuration",
                "sanctions_checker_config.json",
                "JSON Files (*.json)"
            )
            
            if filename:
                import json
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.config._config_data, f, indent=2, ensure_ascii=False)
                
                QMessageBox.information(self, "Export Successful", f"Configuration exported to:\n{filename}")
                
        except Exception as e:
            self._show_error("Export Error", f"Failed to export configuration: {str(e)}")
    
    def _import_config(self):
        """Import configuration from file."""
        try:
            filename, _ = QFileDialog.getOpenFileName(
                self,
                "Import Configuration",
                "",
                "JSON Files (*.json)"
            )
            
            if filename:
                import json
                with open(filename, 'r', encoding='utf-8') as f:
                    imported_config = json.load(f)
                
                # Merge with current config
                self.config._config_data = self.config._merge_config(self.config._defaults, imported_config)
                
                # Reload UI
                self.load_current_settings()
                
                QMessageBox.information(self, "Import Successful", f"Configuration imported from:\n{filename}")
                
        except Exception as e:
            self._show_error("Import Error", f"Failed to import configuration: {str(e)}")
    
    def _open_config_folder(self):
        """Open configuration folder in file explorer."""
        try:
            import os
            import subprocess
            import platform
            
            config_dir = str(self.config.config_dir)
            
            if platform.system() == "Windows":
                os.startfile(config_dir)
            elif platform.system() == "Darwin":  # macOS
                subprocess.run(["open", config_dir])
            else:  # Linux
                subprocess.run(["xdg-open", config_dir])
                
        except Exception as e:
            self._show_error("Open Folder Error", f"Failed to open configuration folder: {str(e)}")
    
    def _load_raw_config(self):
        """Load current configuration into raw editor."""
        try:
            import json
            config_json = json.dumps(self.config._config_data, indent=2, ensure_ascii=False)
            self.raw_config_editor.setPlainText(config_json)
        except Exception as e:
            self._show_error("Load Error", f"Failed to load raw configuration: {str(e)}")
    
    def _validate_raw_config(self):
        """Validate JSON in raw config editor."""
        try:
            import json
            config_text = self.raw_config_editor.toPlainText()
            json.loads(config_text)  # This will raise an exception if invalid
            QMessageBox.information(self, "Validation", "JSON configuration is valid!")
        except json.JSONDecodeError as e:
            self._show_error("Validation Error", f"Invalid JSON: {str(e)}")
        except Exception as e:
            self._show_error("Validation Error", f"Validation failed: {str(e)}")
    
    def _apply_raw_config(self):
        """Apply raw configuration changes."""
        try:
            import json
            config_text = self.raw_config_editor.toPlainText()
            new_config = json.loads(config_text)
            
            # Merge with defaults to ensure all required keys exist
            self.config._config_data = self.config._merge_config(self.config._defaults, new_config)
            
            # Reload UI
            self.load_current_settings()
            
            QMessageBox.information(self, "Apply Successful", "Raw configuration applied successfully!")
            
        except json.JSONDecodeError as e:
            self._show_error("Apply Error", f"Invalid JSON: {str(e)}")
        except Exception as e:
            self._show_error("Apply Error", f"Failed to apply configuration: {str(e)}")
    
    def _show_error(self, title: str, message: str):
        """Show error message dialog."""
        QMessageBox.critical(self, title, message)
    
    def update_logo_preview(self):
        """Update the logo preview in the branding tab."""
        try:
            # Get current logo
            logo_pixmap = resource_manager.get_logo_pixmap(width=200)
            if not logo_pixmap.isNull():
                self.logo_preview_label.setPixmap(logo_pixmap)
                
                if resource_manager.has_logo():
                    self.logo_status_label.setText("✅ Custom logo installed\nLogo will appear in application interface and PDF reports.")
                    self.logo_status_label.setStyleSheet("color: green;")
                else:
                    self.logo_status_label.setText("📝 Using default placeholder\nUpload your company logo for professional branding.")
                    self.logo_status_label.setStyleSheet("color: orange;")
            else:
                self.logo_preview_label.setText("❌ No logo available")
                self.logo_status_label.setText("No logo found. Please upload a logo.")
                self.logo_status_label.setStyleSheet("color: red;")
        except Exception as e:
            self.logo_preview_label.setText("❌ Error loading logo")
            self.logo_status_label.setText(f"Error: {str(e)}")
            self.logo_status_label.setStyleSheet("color: red;")
    
    def upload_logo(self):
        """Open logo upload dialog."""
        try:
            dialog = LogoUploadDialog(self)
            dialog.logo_updated.connect(self.on_logo_updated)
            dialog.exec()
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Could not open logo upload dialog: {str(e)}")
    
    def reset_logo(self):
        """Reset logo to default."""
        reply = QMessageBox.question(
            self, "Reset Logo", 
            "This will remove your custom logo and use the default. Continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                # Remove logo files
                assets_dir = resource_manager.get_assets_directory()
                logo_files = ["logo.png", "logo_small.png", "icon.png", "icon.ico"]
                
                removed_count = 0
                for logo_file in logo_files:
                    logo_path = assets_dir / logo_file
                    if logo_path.exists():
                        logo_path.unlink()
                        removed_count += 1
                
                if removed_count > 0:
                    QMessageBox.information(self, "Reset Complete", 
                                          f"Logo has been reset to default. {removed_count} files removed.")
                else:
                    QMessageBox.information(self, "Reset Complete", "No custom logo files found to remove.")
                
                # Update preview
                self.update_logo_preview()
                
                # Mark as changed
                self._on_setting_changed()
                
            except Exception as e:
                QMessageBox.warning(self, "Reset Failed", f"Could not reset logo: {str(e)}")
    
    def on_logo_updated(self):
        """Handle logo update from upload dialog."""
        # Update the preview
        self.update_logo_preview()
        
        # Mark settings as changed
        self._on_setting_changed()
        
        # Show success message
        self.status_label.setText("Logo updated successfully!")
        self.status_label.setVisible(True)
        
        # Hide message after 3 seconds
        QTimer.singleShot(3000, lambda: self.status_label.setVisible(False))
    
    def closeEvent(self, event):
        """Handle widget close event."""
        if self.has_unsaved_changes:
            reply = QMessageBox.question(
                self,
                "Unsaved Changes",
                "You have unsaved changes. Do you want to save them before closing?",
                QMessageBox.StandardButton.Save | QMessageBox.StandardButton.Discard | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Save
            )
            
            if reply == QMessageBox.StandardButton.Save:
                self.save_settings()
                event.accept()
            elif reply == QMessageBox.StandardButton.Discard:
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()