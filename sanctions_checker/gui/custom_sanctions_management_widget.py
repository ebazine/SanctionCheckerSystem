"""
Custom Sanctions Management Widget for Sanctions Checker Application

This module provides a comprehensive management interface for custom sanctions
with tabbed interface including entity list, create/edit, and data quality tabs.
"""

import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit,
    QComboBox, QTableWidget, QTableWidgetItem, QTabWidget, QGroupBox,
    QFormLayout, QMessageBox, QHeaderView, QAbstractItemView, QCheckBox,
    QDateEdit, QSpinBox, QProgressBar, QTextEdit, QFrame, QSplitter,
    QListWidget, QListWidgetItem, QGridLayout, QSizePolicy, QScrollArea
)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QThread, pyqtSlot, QDate
from PyQt6.QtGui import QFont, QIcon, QColor, QPalette

from ..models.base import SubjectType, NameType, RecordStatus
from ..services.custom_sanctions_service import CustomSanctionsService
from ..services.custom_sanctions_validator import ValidationResult
from .custom_sanction_entry_dialog import CustomSanctionEntryDialog
from .custom_sanctions_import_dialog import CustomSanctionsImportDialog
from .custom_sanctions_export_dialog import CustomSanctionsExportDialog
from .custom_sanctions_data_quality_widget import CustomSanctionsDataQualityWidget

logger = logging.getLogger(__name__)


class EntityListWorker(QThread):
    """Worker thread for loading entity list without blocking UI."""
    
    entities_loaded = pyqtSignal(list, int)  # entities, total_count
    load_error = pyqtSignal(str)  # error message
    
    def __init__(self, service: CustomSanctionsService, filters: Dict[str, Any] = None, 
                 limit: int = 100, offset: int = 0):
        super().__init__()
        self.service = service
        self.filters = filters or {}
        self.limit = limit
        self.offset = offset
    
    def run(self):
        """Load entities in background thread."""
        try:
            entities = self.service.list_sanction_entities(
                filters=self.filters,
                limit=self.limit,
                offset=self.offset
            )
            total_count = self.service.count_sanction_entities(filters=self.filters)
            self.entities_loaded.emit(entities, total_count)
        except Exception as e:
            logger.error(f"Error loading entities: {e}")
            self.load_error.emit(str(e))


class DataQualityWorker(QThread):
    """Worker thread for analyzing data quality without blocking UI."""
    
    quality_analysis_complete = pyqtSignal(dict)  # quality_report
    analysis_error = pyqtSignal(str)  # error message
    
    def __init__(self, service: CustomSanctionsService):
        super().__init__()
        self.service = service
    
    def run(self):
        """Analyze data quality in background thread."""
        try:
            # Get all entities for quality analysis
            entities = self.service.list_sanction_entities(limit=1000)  # Reasonable limit
            
            quality_report = {
                'total_entities': len(entities),
                'by_status': {},
                'by_subject_type': {},
                'incomplete_entities': [],
                'outdated_entities': [],
                'validation_issues': [],
                'recent_updates': [],
                'completeness_stats': {
                    'complete_entities': 0,
                    'incomplete_entities': 0,
                    'critical_issues': 0,
                    'medium_issues': 0,
                    'low_issues': 0,
                    'outdated_entries': 0,
                    'avg_completeness': 0
                }
            }
            
            # Analyze entities
            for entity in entities:
                # Count by status
                status = entity.record_status.value if entity.record_status else 'Unknown'
                quality_report['by_status'][status] = quality_report['by_status'].get(status, 0) + 1
                
                # Count by subject type
                subject_type = entity.subject_type.value if entity.subject_type else 'Unknown'
                quality_report['by_subject_type'][subject_type] = quality_report['by_subject_type'].get(subject_type, 0) + 1
                
                # Check for incomplete data with comprehensive validation
                issues = []
                severity_scores = []
                
                # CRITICAL issues (amplified severity - these make entities unusable)
                if not entity.names or len(entity.names) == 0:
                    issues.append("🚨 NO NAMES DEFINED - Entity cannot be identified")
                    severity_scores.append(10)  # Maximum severity
                elif not any(name.name_type == NameType.PRIMARY for name in entity.names):
                    issues.append("🚨 NO PRIMARY NAME - Entity lacks main identifier")
                    severity_scores.append(9)  # Very high severity
                
                if not entity.sanctioning_authority or entity.sanctioning_authority.strip() == '':
                    issues.append("🚨 MISSING SANCTIONING AUTHORITY - Legal basis unclear")
                    severity_scores.append(10)  # Maximum severity - legal requirement
                
                if not entity.program or entity.program.strip() == '':
                    issues.append("🚨 MISSING PROGRAM - Classification unknown")
                    severity_scores.append(9)  # Very high severity
                
                if not entity.listing_date:
                    issues.append("🚨 MISSING LISTING DATE - Timeline unknown")
                    severity_scores.append(9)  # Very high severity - compliance requirement
                
                # HIGH severity issues (significantly impact usability)
                if not entity.data_source or entity.data_source.strip() == '':
                    issues.append("⚠️ MISSING DATA SOURCE - Verification impossible")
                    severity_scores.append(8)  # Increased from 6 to 8
                
                if not entity.reason_for_listing or entity.reason_for_listing.strip() == '':
                    issues.append("⚠️ MISSING REASON FOR LISTING - Justification unclear")
                    severity_scores.append(7)  # Increased from 5 to 7
                
                # Subject-specific validation (amplified severity)
                if entity.subject_type == SubjectType.INDIVIDUAL:
                    if entity.individual_details:
                        if not entity.individual_details.place_of_birth:
                            issues.append("⚠️ Missing place of birth - Identity verification limited")
                            severity_scores.append(6)  # Increased from 3 to 6
                        if not entity.individual_details.nationalities:
                            issues.append("⚠️ Missing nationalities - Jurisdiction unclear")
                            severity_scores.append(6)  # Increased from 3 to 6
                    else:
                        issues.append("🚨 MISSING INDIVIDUAL DETAILS - Person cannot be properly identified")
                        severity_scores.append(8)  # Increased from 6 to 8
                
                elif entity.subject_type == SubjectType.ENTITY:
                    if entity.entity_details:
                        if not entity.entity_details.registration_number:
                            issues.append("⚠️ Missing registration number - Entity verification limited")
                            severity_scores.append(6)  # Increased from 4 to 6
                        if not entity.entity_details.registration_authority:
                            issues.append("⚠️ Missing registration authority - Legal status unclear")
                            severity_scores.append(6)  # Increased from 4 to 6
                    else:
                        issues.append("🚨 MISSING ENTITY DETAILS - Organization cannot be properly identified")
                        severity_scores.append(8)  # Increased from 6 to 8
                
                # Address validation (amplified severity)
                if not entity.addresses or len(entity.addresses) == 0:
                    issues.append("⚠️ NO ADDRESSES DEFINED - Location tracking impossible")
                    severity_scores.append(6)  # Increased from 4 to 6
                
                # Identifier validation (amplified severity)
                if not entity.identifiers or len(entity.identifiers) == 0:
                    issues.append("⚠️ NO IDENTIFIERS DEFINED - Verification severely limited")
                    severity_scores.append(5)  # Increased from 3 to 5
                
                if issues:
                    # Calculate overall severity score
                    max_severity = max(severity_scores) if severity_scores else 0
                    avg_severity = sum(severity_scores) / len(severity_scores) if severity_scores else 0
                    
                    # Determine completeness percentage
                    total_possible_fields = 15  # Approximate number of important fields
                    missing_critical_fields = sum(1 for score in severity_scores if score >= 8)
                    completeness = max(0, (total_possible_fields - len(issues)) / total_possible_fields * 100)
                    
                    quality_report['incomplete_entities'].append({
                        'entity_id': entity.id,
                        'internal_entry_id': entity.internal_entry_id,
                        'primary_name': entity.names[0].full_name if entity.names else 'Unknown',
                        'issues': issues,
                        'issue_count': len(issues),
                        'max_severity': max_severity,
                        'avg_severity': round(avg_severity, 1),
                        'completeness_percentage': round(completeness, 1),
                        'subject_type': entity.subject_type.value if entity.subject_type else 'Unknown',
                        'record_status': entity.record_status.value if entity.record_status else 'Unknown'
                    })
                
                # Check for recent updates (last 30 days)
                if entity.last_updated and (datetime.utcnow() - entity.last_updated).days <= 30:
                    quality_report['recent_updates'].append({
                        'entity_id': entity.id,
                        'internal_entry_id': entity.internal_entry_id,
                        'primary_name': entity.names[0].full_name if entity.names else 'Unknown',
                        'last_updated': entity.last_updated
                    })
                
                # Check for outdated entries (not updated in 90+ days)
                days_since_update = (datetime.utcnow() - entity.last_updated).days if entity.last_updated else 999
                if days_since_update > 90:
                    issues.append(f"📅 OUTDATED ENTRY - Not updated for {days_since_update} days")
                    severity_scores.append(5)  # Medium severity for outdated entries
                    
                    # Add to outdated entities list
                    quality_report['outdated_entities'].append({
                        'entity_id': entity.id,
                        'internal_entry_id': entity.internal_entry_id,
                        'primary_name': entity.names[0].full_name if entity.names else 'Unknown',
                        'days_since_update': days_since_update,
                        'last_updated': entity.last_updated
                    })
            
            # Calculate completeness statistics
            total_entities = len(entities)
            incomplete_count = len(quality_report['incomplete_entities'])
            complete_count = total_entities - incomplete_count
            
            critical_issues = sum(1 for entity in quality_report['incomplete_entities'] if entity['max_severity'] >= 8)
            high_issues = sum(1 for entity in quality_report['incomplete_entities'] if 6 <= entity['max_severity'] < 8)
            medium_issues = sum(1 for entity in quality_report['incomplete_entities'] if 4 <= entity['max_severity'] < 6)
            low_issues = sum(1 for entity in quality_report['incomplete_entities'] if entity['max_severity'] < 4)
            outdated_count = len(quality_report['outdated_entities'])
            
            avg_completeness = 0
            if quality_report['incomplete_entities']:
                avg_completeness = sum(entity['completeness_percentage'] for entity in quality_report['incomplete_entities']) / len(quality_report['incomplete_entities'])
            
            quality_report['completeness_stats'] = {
                'complete_entities': complete_count,
                'incomplete_entities': incomplete_count,
                'critical_issues': critical_issues,
                'high_issues': high_issues,
                'medium_issues': medium_issues,
                'low_issues': low_issues,
                'outdated_entries': outdated_count,
                'avg_completeness': round(avg_completeness, 1),
                'completeness_rate': round((complete_count / total_entities * 100) if total_entities > 0 else 0, 1)
            }
            
            # Sort incomplete entities by severity (most critical first)
            quality_report['incomplete_entities'].sort(key=lambda x: (-x['max_severity'], -x['issue_count']))
            
            self.quality_analysis_complete.emit(quality_report)
            
        except Exception as e:
            logger.error(f"Error analyzing data quality: {e}")
            self.analysis_error.emit(str(e))


class CustomSanctionsManagementWidget(QWidget):
    """
    Main widget for custom sanctions management with tabbed interface.
    Provides entity list, create/edit, and data quality functionality.
    """
    
    def __init__(self, service: CustomSanctionsService, parent=None):
        """
        Initialize the Custom Sanctions Management Widget.
        
        Args:
            service: CustomSanctionsService instance
            parent: Parent widget
        """
        super().__init__(parent)
        self.service = service
        self.current_entities = []
        self.current_filters = {}
        self.current_page = 0
        self.page_size = 50
        self.total_entities = 0
        
        # Worker threads
        self.entity_list_worker = None
        self.data_quality_worker = None
        
        self.setup_ui()
        self.connect_signals()
        self.load_initial_data()
    
    def setup_ui(self):
        """Set up the user interface with tabbed layout."""
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        # Title
        title_label = QLabel("Custom Sanctions Management")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        title_label.setFont(title_font)
        main_layout.addWidget(title_label)
        
        # Create tab widget
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)
        
        # Entity List Tab
        self.entity_list_tab = self.create_entity_list_tab()
        self.tab_widget.addTab(self.entity_list_tab, "📋 Entity List")
        
        # Create/Edit Tab
        self.create_edit_tab = self.create_create_edit_tab()
        self.tab_widget.addTab(self.create_edit_tab, "➕ Create/Edit")
        
        # Data Quality Tab
        self.data_quality_tab = self.create_data_quality_tab()
        self.tab_widget.addTab(self.data_quality_tab, "🔍 Data Quality")
    
    def create_entity_list_tab(self) -> QWidget:
        """Create the entity list tab with search, filter, and sort functionality."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Search and filter section
        filter_group = QGroupBox("Search & Filter")
        filter_layout = QGridLayout(filter_group)
        
        # Search term
        filter_layout.addWidget(QLabel("Search:"), 0, 0)
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search by name or internal ID...")
        filter_layout.addWidget(self.search_input, 0, 1)
        
        # Subject type filter
        filter_layout.addWidget(QLabel("Subject Type:"), 0, 2)
        self.subject_type_filter = QComboBox()
        self.subject_type_filter.addItem("All", None)
        for subject_type in SubjectType:
            self.subject_type_filter.addItem(subject_type.value, subject_type)
        filter_layout.addWidget(self.subject_type_filter, 0, 3)
        
        # Status filter
        filter_layout.addWidget(QLabel("Status:"), 1, 0)
        self.status_filter = QComboBox()
        self.status_filter.addItem("All", None)
        for status in RecordStatus:
            self.status_filter.addItem(status.value, status)
        filter_layout.addWidget(self.status_filter, 1, 1)
        
        # Authority filter
        filter_layout.addWidget(QLabel("Authority:"), 1, 2)
        self.authority_filter = QLineEdit()
        self.authority_filter.setPlaceholderText("Filter by authority...")
        filter_layout.addWidget(self.authority_filter, 1, 3)
        
        # Filter buttons
        button_layout = QHBoxLayout()
        self.apply_filter_button = QPushButton("Apply Filters")
        self.clear_filter_button = QPushButton("Clear Filters")
        button_layout.addWidget(self.apply_filter_button)
        button_layout.addWidget(self.clear_filter_button)
        button_layout.addStretch()
        filter_layout.addLayout(button_layout, 2, 0, 1, 4)
        
        layout.addWidget(filter_group)
        
        # Entity table
        self.entity_table = QTableWidget()
        self.entity_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.entity_table.setAlternatingRowColors(True)
        self.entity_table.setSortingEnabled(True)
        
        # Set up table columns
        columns = [
            "Internal ID", "Primary Name", "Subject Type", "Status", 
            "Authority", "Program", "Listing Date", "Last Updated"
        ]
        self.entity_table.setColumnCount(len(columns))
        self.entity_table.setHorizontalHeaderLabels(columns)
        
        # Configure table headers
        header = self.entity_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # Internal ID
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)  # Primary Name
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)  # Subject Type
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)  # Status
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)  # Authority
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)  # Program
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)  # Listing Date
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)  # Last Updated
        
        layout.addWidget(self.entity_table)
        
        # Pagination and actions
        bottom_layout = QHBoxLayout()
        
        # Pagination info
        self.pagination_label = QLabel("No entities loaded")
        bottom_layout.addWidget(self.pagination_label)
        
        bottom_layout.addStretch()
        
        # Action buttons
        self.new_entity_button = QPushButton("New Entity")
        self.edit_entity_button = QPushButton("Edit Selected")
        self.edit_entity_button.setEnabled(False)
        self.delete_entity_button = QPushButton("Delete Selected")
        self.delete_entity_button.setEnabled(False)
        self.bulk_status_button = QPushButton("Bulk Status Change")
        self.bulk_status_button.setEnabled(False)
        
        # Import/Export buttons
        self.import_button = QPushButton("Import XML")
        self.export_button = QPushButton("Export XML")
        
        bottom_layout.addWidget(self.new_entity_button)
        bottom_layout.addWidget(self.edit_entity_button)
        bottom_layout.addWidget(self.delete_entity_button)
        bottom_layout.addWidget(self.bulk_status_button)
        bottom_layout.addWidget(QFrame())  # Separator
        bottom_layout.addWidget(self.import_button)
        bottom_layout.addWidget(self.export_button)
        
        layout.addLayout(bottom_layout)
        
        # Progress bar for loading
        self.entity_list_progress = QProgressBar()
        self.entity_list_progress.setVisible(False)
        layout.addWidget(self.entity_list_progress)
        
        return tab
    
    def create_create_edit_tab(self) -> QWidget:
        """Create the create/edit tab that embeds the CustomSanctionEntryDialog."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Instructions
        instructions = QLabel(
            "Use this tab to create new custom sanction entries or edit existing ones. "
            "Click 'New Entity' to create a new entry, or select an entity from the list "
            "and click 'Edit Selected' to modify an existing entry."
        )
        instructions.setWordWrap(True)
        instructions.setStyleSheet("color: #666; font-style: italic; padding: 10px;")
        layout.addWidget(instructions)
        
        # Quick action buttons
        action_layout = QHBoxLayout()
        self.quick_new_button = QPushButton("New Entity")
        self.quick_new_button.setMinimumHeight(40)
        self.quick_edit_button = QPushButton("Edit Selected Entity")
        self.quick_edit_button.setMinimumHeight(40)
        self.quick_edit_button.setEnabled(False)
        
        action_layout.addWidget(self.quick_new_button)
        action_layout.addWidget(self.quick_edit_button)
        action_layout.addStretch()
        
        layout.addLayout(action_layout)
        
        # Recent entities for quick editing
        recent_group = QGroupBox("Recent Entities")
        recent_layout = QVBoxLayout(recent_group)
        
        self.recent_entities_list = QListWidget()
        self.recent_entities_list.setMaximumHeight(200)
        recent_layout.addWidget(self.recent_entities_list)
        
        layout.addWidget(recent_group)
        
        # Add stretch to push content to top
        layout.addStretch()
        
        return tab
    
    def create_data_quality_tab(self) -> QWidget:
        """Create the data quality tab showing validation reports and incomplete entries."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Refresh button
        refresh_layout = QHBoxLayout()
        self.refresh_quality_button = QPushButton("Refresh Analysis")
        refresh_layout.addWidget(self.refresh_quality_button)
        refresh_layout.addStretch()
        layout.addLayout(refresh_layout)
        
        # Create splitter for quality sections
        splitter = QSplitter(Qt.Orientation.Vertical)
        layout.addWidget(splitter)
        
        # Summary statistics
        summary_group = QGroupBox("Summary Statistics")
        summary_layout = QGridLayout(summary_group)
        
        self.total_entities_label = QLabel("Total Entities: 0")
        self.active_entities_label = QLabel("Active: 0")
        self.inactive_entities_label = QLabel("Inactive: 0")
        self.pending_entities_label = QLabel("Pending: 0")
        
        summary_layout.addWidget(self.total_entities_label, 0, 0)
        summary_layout.addWidget(self.active_entities_label, 0, 1)
        summary_layout.addWidget(self.inactive_entities_label, 0, 2)
        summary_layout.addWidget(self.pending_entities_label, 0, 3)
        
        # Subject type breakdown
        self.individual_count_label = QLabel("Individuals: 0")
        self.entity_count_label = QLabel("Entities: 0")
        self.vessel_count_label = QLabel("Vessels: 0")
        self.other_count_label = QLabel("Other: 0")
        
        summary_layout.addWidget(self.individual_count_label, 1, 0)
        summary_layout.addWidget(self.entity_count_label, 1, 1)
        summary_layout.addWidget(self.vessel_count_label, 1, 2)
        summary_layout.addWidget(self.other_count_label, 1, 3)
        
        splitter.addWidget(summary_group)
        
        # Incomplete entities with enhanced display
        incomplete_group = QGroupBox("Incomplete Entities")
        incomplete_layout = QVBoxLayout(incomplete_group)
        
        # Completeness statistics
        completeness_stats_layout = QHBoxLayout()
        self.complete_entities_label = QLabel("Complete: 0")
        self.incomplete_entities_label = QLabel("Incomplete: 0")
        self.critical_issues_label = QLabel("Critical: 0")
        self.outdated_entries_label = QLabel("Outdated: 0")
        self.completeness_rate_label = QLabel("Rate: 0%")
        
        completeness_stats_layout.addWidget(self.complete_entities_label)
        completeness_stats_layout.addWidget(self.incomplete_entities_label)
        completeness_stats_layout.addWidget(self.critical_issues_label)
        completeness_stats_layout.addWidget(self.outdated_entries_label)
        completeness_stats_layout.addWidget(self.completeness_rate_label)
        completeness_stats_layout.addStretch()
        
        incomplete_layout.addLayout(completeness_stats_layout)
        
        # Filter options for incomplete entities
        filter_layout = QHBoxLayout()
        
        self.severity_filter_combo = QComboBox()
        self.severity_filter_combo.addItem("All Issues", "all")
        self.severity_filter_combo.addItem("Critical Only (8-10)", "critical")
        self.severity_filter_combo.addItem("High & Critical (6-10)", "high")
        self.severity_filter_combo.addItem("Medium & Above (4-10)", "medium")
        self.severity_filter_combo.addItem("Low Priority (1-3)", "low")
        
        self.subject_type_filter_combo = QComboBox()
        self.subject_type_filter_combo.addItem("All Types", "all")
        self.subject_type_filter_combo.addItem("Individuals", "Individual")
        self.subject_type_filter_combo.addItem("Entities", "Entity")
        self.subject_type_filter_combo.addItem("Vessels", "Vessel")
        
        filter_layout.addWidget(QLabel("Severity:"))
        filter_layout.addWidget(self.severity_filter_combo)
        filter_layout.addWidget(QLabel("Type:"))
        filter_layout.addWidget(self.subject_type_filter_combo)
        filter_layout.addStretch()
        
        incomplete_layout.addLayout(filter_layout)
        
        # Enhanced incomplete entities table instead of list
        self.incomplete_entities_table = QTableWidget()
        self.incomplete_entities_table.setColumnCount(6)
        self.incomplete_entities_table.setHorizontalHeaderLabels([
            "ID", "Name", "Type", "Completeness", "Issues", "Severity"
        ])
        self.incomplete_entities_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.incomplete_entities_table.setAlternatingRowColors(True)
        self.incomplete_entities_table.setSortingEnabled(True)
        # Make table read-only to prevent users from editing severity/completeness values
        self.incomplete_entities_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        
        # Set column widths
        header = self.incomplete_entities_table.horizontalHeader()
        header.setStretchLastSection(True)
        header.resizeSection(0, 120)  # ID
        header.resizeSection(1, 200)  # Name
        header.resizeSection(2, 80)   # Type
        header.resizeSection(3, 100)  # Completeness
        header.resizeSection(4, 50)   # Issues
        header.resizeSection(5, 80)   # Severity
        
        incomplete_layout.addWidget(self.incomplete_entities_table)
        
        # Bulk operations for data quality
        bulk_operations_layout = QHBoxLayout()
        
        self.bulk_verify_button = QPushButton("🔍 Bulk Verify Selected")
        self.bulk_verify_button.setToolTip("Mark selected entities as verified")
        self.bulk_verify_button.clicked.connect(self.bulk_verify_entities)
        
        self.bulk_assign_reviewer_button = QPushButton("👤 Assign Reviewer")
        self.bulk_assign_reviewer_button.setToolTip("Assign reviewer to selected entities")
        self.bulk_assign_reviewer_button.clicked.connect(self.bulk_assign_reviewer)
        
        self.bulk_update_source_button = QPushButton("📝 Update Data Source")
        self.bulk_update_source_button.setToolTip("Update data source for selected entities")
        self.bulk_update_source_button.clicked.connect(self.bulk_update_data_source)
        
        self.export_quality_report_button = QPushButton("📊 Export Quality Report")
        self.export_quality_report_button.setToolTip("Export data quality analysis report")
        self.export_quality_report_button.clicked.connect(self.export_quality_report)
        
        bulk_operations_layout.addWidget(self.bulk_verify_button)
        bulk_operations_layout.addWidget(self.bulk_assign_reviewer_button)
        bulk_operations_layout.addWidget(self.bulk_update_source_button)
        bulk_operations_layout.addWidget(self.export_quality_report_button)
        bulk_operations_layout.addStretch()
        
        incomplete_layout.addLayout(bulk_operations_layout)
        
        splitter.addWidget(incomplete_group)
        
        # Recent updates
        recent_group = QGroupBox("Recent Updates (Last 30 Days)")
        recent_layout = QVBoxLayout(recent_group)
        
        self.recent_updates_list = QListWidget()
        recent_layout.addWidget(self.recent_updates_list)
        
        splitter.addWidget(recent_group)
        
        # Progress bar for analysis
        self.quality_analysis_progress = QProgressBar()
        self.quality_analysis_progress.setVisible(False)
        layout.addWidget(self.quality_analysis_progress)
        
        return tab
    
    def connect_signals(self):
        """Connect signals and slots."""
        # Entity list tab signals
        self.apply_filter_button.clicked.connect(self.apply_filters)
        self.clear_filter_button.clicked.connect(self.clear_filters)
        self.search_input.returnPressed.connect(self.apply_filters)
        self.entity_table.selectionModel().selectionChanged.connect(self.on_entity_selection_changed)
        self.entity_table.doubleClicked.connect(self.edit_selected_entity)
        
        # Action buttons
        self.new_entity_button.clicked.connect(self.create_new_entity)
        self.edit_entity_button.clicked.connect(self.edit_selected_entity)
        self.delete_entity_button.clicked.connect(self.delete_selected_entities)
        self.bulk_status_button.clicked.connect(self.bulk_status_change)
        
        # Import/Export buttons
        self.import_button.clicked.connect(self.import_xml)
        self.export_button.clicked.connect(self.export_xml)
        
        # Create/edit tab signals
        self.quick_new_button.clicked.connect(self.create_new_entity)
        self.quick_edit_button.clicked.connect(self.edit_selected_entity)
        self.recent_entities_list.doubleClicked.connect(self.edit_recent_entity)
        
        # Data quality tab signals
        self.refresh_quality_button.clicked.connect(self.refresh_data_quality)
        self.incomplete_entities_table.doubleClicked.connect(self.edit_incomplete_entity)
        self.severity_filter_combo.currentTextChanged.connect(self.filter_incomplete_entities)
        self.subject_type_filter_combo.currentTextChanged.connect(self.filter_incomplete_entities)
        self.severity_filter_combo.currentTextChanged.connect(self.filter_incomplete_entities)
        self.subject_type_filter_combo.currentTextChanged.connect(self.filter_incomplete_entities)
        
        # Tab change signal
        self.tab_widget.currentChanged.connect(self.on_tab_changed)
    
    def load_initial_data(self):
        """Load initial data for the widget."""
        self.load_entity_list()
        self.load_recent_entities()
    
    def load_entity_list(self):
        """Load entity list with current filters."""
        if self.entity_list_worker and self.entity_list_worker.isRunning():
            return
        
        self.entity_list_progress.setVisible(True)
        self.entity_list_progress.setRange(0, 0)  # Indeterminate progress
        
        self.entity_list_worker = EntityListWorker(
            service=self.service,
            filters=self.current_filters,
            limit=self.page_size,
            offset=self.current_page * self.page_size
        )
        self.entity_list_worker.entities_loaded.connect(self.on_entities_loaded)
        self.entity_list_worker.load_error.connect(self.on_entity_load_error)
        self.entity_list_worker.start()
    
    @pyqtSlot(list, int)
    def on_entities_loaded(self, entities, total_count):
        """Handle entities loaded signal."""
        self.entity_list_progress.setVisible(False)
        self.current_entities = entities
        self.total_entities = total_count
        
        self.populate_entity_table()
        self.update_pagination_label()
    
    @pyqtSlot(str)
    def on_entity_load_error(self, error_message):
        """Handle entity load error."""
        self.entity_list_progress.setVisible(False)
        QMessageBox.critical(self, "Load Error", f"Failed to load entities:\n{error_message}")
    
    def populate_entity_table(self):
        """Populate the entity table with current entities."""
        self.entity_table.setRowCount(len(self.current_entities))
        
        for row, entity in enumerate(self.current_entities):
            # Internal ID
            self.entity_table.setItem(row, 0, QTableWidgetItem(entity.internal_entry_id or ""))
            
            # Primary Name
            primary_name = ""
            if entity.names:
                primary_names = [n for n in entity.names if n.name_type == NameType.PRIMARY]
                if primary_names:
                    primary_name = primary_names[0].full_name
                elif entity.names:
                    primary_name = entity.names[0].full_name
            self.entity_table.setItem(row, 1, QTableWidgetItem(primary_name))
            
            # Subject Type
            subject_type = entity.subject_type.value if entity.subject_type else ""
            self.entity_table.setItem(row, 2, QTableWidgetItem(subject_type))
            
            # Status
            status = entity.record_status.value if entity.record_status else ""
            status_item = QTableWidgetItem(status)
            if entity.record_status == RecordStatus.ACTIVE:
                status_item.setBackground(QColor(200, 255, 200))  # Light green
            elif entity.record_status == RecordStatus.DELISTED:
                status_item.setBackground(QColor(255, 200, 200))  # Light red
            elif entity.record_status == RecordStatus.PENDING:
                status_item.setBackground(QColor(255, 255, 200))  # Light yellow
            self.entity_table.setItem(row, 3, status_item)
            
            # Authority
            self.entity_table.setItem(row, 4, QTableWidgetItem(entity.sanctioning_authority or ""))
            
            # Program
            self.entity_table.setItem(row, 5, QTableWidgetItem(entity.program or ""))
            
            # Listing Date
            listing_date = entity.listing_date.strftime("%Y-%m-%d") if entity.listing_date else ""
            self.entity_table.setItem(row, 6, QTableWidgetItem(listing_date))
            
            # Last Updated
            last_updated = entity.last_updated.strftime("%Y-%m-%d %H:%M") if entity.last_updated else ""
            self.entity_table.setItem(row, 7, QTableWidgetItem(last_updated))
            
            # Store entity ID in first column for reference
            self.entity_table.item(row, 0).setData(Qt.ItemDataRole.UserRole, entity.id)
    
    def update_pagination_label(self):
        """Update pagination information label."""
        start = self.current_page * self.page_size + 1
        end = min(start + len(self.current_entities) - 1, self.total_entities)
        self.pagination_label.setText(f"Showing {start}-{end} of {self.total_entities} entities")
    
    def apply_filters(self):
        """Apply current filter settings and reload entity list."""
        self.current_filters = {}
        self.current_page = 0
        
        # Search term
        search_term = self.search_input.text().strip()
        if search_term:
            self.current_filters['search_term'] = search_term
        
        # Subject type
        subject_type = self.subject_type_filter.currentData()
        if subject_type:
            self.current_filters['subject_type'] = subject_type
        
        # Status
        status = self.status_filter.currentData()
        if status:
            self.current_filters['record_status'] = status
        
        # Authority
        authority = self.authority_filter.text().strip()
        if authority:
            self.current_filters['sanctioning_authority'] = authority
        
        self.load_entity_list()
    
    def clear_filters(self):
        """Clear all filters and reload entity list."""
        self.search_input.clear()
        self.subject_type_filter.setCurrentIndex(0)
        self.status_filter.setCurrentIndex(0)
        self.authority_filter.clear()
        
        self.current_filters = {}
        self.current_page = 0
        self.load_entity_list()
    
    def on_entity_selection_changed(self):
        """Handle entity selection changes."""
        selected_rows = self.entity_table.selectionModel().selectedRows()
        has_selection = len(selected_rows) > 0
        
        self.edit_entity_button.setEnabled(has_selection)
        self.delete_entity_button.setEnabled(has_selection)
        self.bulk_status_button.setEnabled(has_selection)
        self.quick_edit_button.setEnabled(has_selection)
    
    def create_new_entity(self):
        """Create a new custom sanction entity."""
        dialog = CustomSanctionEntryDialog(self.service, parent=self)
        dialog.entity_saved.connect(self.on_entity_saved)
        dialog.exec()
    
    def edit_selected_entity(self):
        """Edit the selected entity."""
        selected_rows = self.entity_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.information(self, "No Selection", "Please select an entity to edit.")
            return
        
        row = selected_rows[0].row()
        entity_id = self.entity_table.item(row, 0).data(Qt.ItemDataRole.UserRole)
        
        dialog = CustomSanctionEntryDialog(self.service, entity_id=entity_id, parent=self)
        dialog.entity_saved.connect(self.on_entity_saved)
        dialog.exec()
    
    def delete_selected_entities(self):
        """Delete selected entities."""
        selected_rows = self.entity_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.information(self, "No Selection", "Please select entities to delete.")
            return
        
        entity_ids = []
        entity_names = []
        for row in selected_rows:
            entity_id = self.entity_table.item(row.row(), 0).data(Qt.ItemDataRole.UserRole)
            entity_name = self.entity_table.item(row.row(), 1).text()
            entity_ids.append(entity_id)
            entity_names.append(entity_name)
        
        # Confirm deletion
        if len(entity_ids) == 1:
            message = f"Are you sure you want to delete the entity '{entity_names[0]}'?"
        else:
            message = f"Are you sure you want to delete {len(entity_ids)} selected entities?"
        
        reply = QMessageBox.question(
            self, "Confirm Deletion", message,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Delete entities
            deleted_count = 0
            for entity_id in entity_ids:
                try:
                    if self.service.delete_sanction_entity(entity_id):
                        deleted_count += 1
                except Exception as e:
                    logger.error(f"Error deleting entity {entity_id}: {e}")
            
            if deleted_count > 0:
                QMessageBox.information(
                    self, "Deletion Complete", 
                    f"Successfully deleted {deleted_count} of {len(entity_ids)} entities."
                )
                self.load_entity_list()
                self.load_recent_entities()
            else:
                QMessageBox.warning(self, "Deletion Failed", "No entities were deleted.")
    
    def bulk_status_change(self):
        """Change status of selected entities in bulk."""
        selected_rows = self.entity_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.information(self, "No Selection", "Please select entities to update.")
            return
        
        # Show status selection dialog
        from PyQt6.QtWidgets import QInputDialog
        statuses = [status.value for status in RecordStatus]
        status, ok = QInputDialog.getItem(
            self, "Bulk Status Change", "Select new status:", statuses, 0, False
        )
        
        if ok and status:
            new_status = RecordStatus(status)
            entity_ids = []
            for row in selected_rows:
                entity_id = self.entity_table.item(row.row(), 0).data(Qt.ItemDataRole.UserRole)
                entity_ids.append(entity_id)
            
            # Update entities
            updated_count = 0
            for entity_id in entity_ids:
                try:
                    success, _ = self.service.update_sanction_entity(
                        entity_id, {'record_status': new_status}
                    )
                    if success:
                        updated_count += 1
                except Exception as e:
                    logger.error(f"Error updating entity {entity_id}: {e}")
            
            if updated_count > 0:
                QMessageBox.information(
                    self, "Update Complete", 
                    f"Successfully updated {updated_count} of {len(entity_ids)} entities."
                )
                self.load_entity_list()
            else:
                QMessageBox.warning(self, "Update Failed", "No entities were updated.")
    
    def load_recent_entities(self):
        """Load recent entities for quick editing."""
        try:
            # Get recent entities (last 10)
            recent_entities = self.service.list_sanction_entities(
                limit=10, order_by='last_updated', order_desc=True
            )
            
            self.recent_entities_list.clear()
            for entity in recent_entities:
                primary_name = ""
                if entity.names:
                    primary_names = [n for n in entity.names if n.name_type == NameType.PRIMARY]
                    if primary_names:
                        primary_name = primary_names[0].full_name
                    elif entity.names:
                        primary_name = entity.names[0].full_name
                
                item_text = f"{entity.internal_entry_id} - {primary_name}"
                if entity.last_updated:
                    item_text += f" (Updated: {entity.last_updated.strftime('%Y-%m-%d')})"
                
                item = QListWidgetItem(item_text)
                item.setData(Qt.ItemDataRole.UserRole, entity.id)
                self.recent_entities_list.addItem(item)
                
        except Exception as e:
            logger.error(f"Error loading recent entities: {e}")
    
    def edit_recent_entity(self):
        """Edit entity selected from recent entities list."""
        current_item = self.recent_entities_list.currentItem()
        if current_item:
            entity_id = current_item.data(Qt.ItemDataRole.UserRole)
            dialog = CustomSanctionEntryDialog(self.service, entity_id=entity_id, parent=self)
            dialog.entity_saved.connect(self.on_entity_saved)
            dialog.exec()
    
    def refresh_data_quality(self):
        """Refresh data quality analysis."""
        if self.data_quality_worker and self.data_quality_worker.isRunning():
            return
        
        self.quality_analysis_progress.setVisible(True)
        self.quality_analysis_progress.setRange(0, 0)  # Indeterminate progress
        
        self.data_quality_worker = DataQualityWorker(self.service)
        self.data_quality_worker.quality_analysis_complete.connect(self.on_quality_analysis_complete)
        self.data_quality_worker.analysis_error.connect(self.on_quality_analysis_error)
        self.data_quality_worker.start()
    
    @pyqtSlot(dict)
    def on_quality_analysis_complete(self, quality_report):
        """Handle quality analysis completion."""
        self.quality_analysis_progress.setVisible(False)
        
        # Update summary statistics
        self.total_entities_label.setText(f"Total Entities: {quality_report['total_entities']}")
        
        by_status = quality_report['by_status']
        self.active_entities_label.setText(f"Active: {by_status.get('Active', 0)}")
        self.inactive_entities_label.setText(f"Inactive: {by_status.get('Inactive', 0)}")
        self.pending_entities_label.setText(f"Pending: {by_status.get('Pending', 0)}")
        
        by_subject_type = quality_report['by_subject_type']
        self.individual_count_label.setText(f"Individuals: {by_subject_type.get('Individual', 0)}")
        self.entity_count_label.setText(f"Entities: {by_subject_type.get('Entity', 0)}")
        self.vessel_count_label.setText(f"Vessels: {by_subject_type.get('Vessel', 0)}")
        self.other_count_label.setText(f"Other: {by_subject_type.get('Other', 0)}")
        
        # Update completeness statistics
        completeness_stats = quality_report['completeness_stats']
        self.complete_entities_label.setText(f"Complete: {completeness_stats['complete_entities']}")
        self.incomplete_entities_label.setText(f"Incomplete: {completeness_stats['incomplete_entities']}")
        self.critical_issues_label.setText(f"Critical: {completeness_stats['critical_issues']}")
        self.outdated_entries_label.setText(f"Outdated: {completeness_stats['outdated_entries']}")
        self.completeness_rate_label.setText(f"Rate: {completeness_stats['completeness_rate']}%")
        
        # Store incomplete entities data for filtering
        self.all_incomplete_entities = quality_report['incomplete_entities']
        
        # Update incomplete entities table
        self.populate_incomplete_entities_table(self.all_incomplete_entities)
        
        # Update recent updates list
        self.recent_updates_list.clear()
        for recent in quality_report['recent_updates']:
            item_text = f"{recent['internal_entry_id']} - {recent['primary_name']}"
            if recent['last_updated']:
                item_text += f" (Updated: {recent['last_updated'].strftime('%Y-%m-%d %H:%M')})"
            
            item = QListWidgetItem(item_text)
            item.setData(Qt.ItemDataRole.UserRole, recent['entity_id'])
            self.recent_updates_list.addItem(item)
    
    @pyqtSlot(str)
    def on_quality_analysis_error(self, error_message):
        """Handle quality analysis error."""
        self.quality_analysis_progress.setVisible(False)
        QMessageBox.critical(self, "Analysis Error", f"Failed to analyze data quality:\n{error_message}")
    
    def populate_incomplete_entities_table(self, incomplete_entities):
        """Populate the incomplete entities table with data."""
        self.incomplete_entities_table.setRowCount(len(incomplete_entities))
        
        for row, entity in enumerate(incomplete_entities):
            # ID
            id_item = QTableWidgetItem(entity['internal_entry_id'])
            id_item.setData(Qt.ItemDataRole.UserRole, entity['entity_id'])
            self.incomplete_entities_table.setItem(row, 0, id_item)
            
            # Name
            name_item = QTableWidgetItem(entity['primary_name'])
            self.incomplete_entities_table.setItem(row, 1, name_item)
            
            # Type
            type_item = QTableWidgetItem(entity['subject_type'])
            self.incomplete_entities_table.setItem(row, 2, type_item)
            
            # Completeness percentage with improved color coding for readability
            completeness_item = QTableWidgetItem(f"{entity['completeness_percentage']}%")
            if entity['completeness_percentage'] < 50:
                # Dark red background with white text for critical completeness
                completeness_item.setBackground(QColor(220, 53, 69))  # Bootstrap danger red
                completeness_item.setForeground(QColor(255, 255, 255))  # White text
            elif entity['completeness_percentage'] < 75:
                # Orange background with dark text for medium completeness
                completeness_item.setBackground(QColor(255, 193, 7))  # Bootstrap warning orange
                completeness_item.setForeground(QColor(33, 37, 41))  # Dark text
            else:
                # Green background with white text for good completeness
                completeness_item.setBackground(QColor(40, 167, 69))  # Bootstrap success green
                completeness_item.setForeground(QColor(255, 255, 255))  # White text
            self.incomplete_entities_table.setItem(row, 3, completeness_item)
            
            # Issue count
            issues_item = QTableWidgetItem(str(entity['issue_count']))
            self.incomplete_entities_table.setItem(row, 4, issues_item)
            
            # Severity with improved color coding and amplified scale
            severity_text = "CRITICAL" if entity['max_severity'] >= 8 else "HIGH" if entity['max_severity'] >= 6 else "MEDIUM" if entity['max_severity'] >= 4 else "LOW"
            severity_item = QTableWidgetItem(severity_text)
            if entity['max_severity'] >= 8:
                # Critical: Dark red with white text
                severity_item.setBackground(QColor(220, 53, 69))  # Bootstrap danger red
                severity_item.setForeground(QColor(255, 255, 255))  # White text
            elif entity['max_severity'] >= 6:
                # High: Orange-red with white text
                severity_item.setBackground(QColor(253, 126, 20))  # Bootstrap warning-danger
                severity_item.setForeground(QColor(255, 255, 255))  # White text
            elif entity['max_severity'] >= 4:
                # Medium: Orange with dark text
                severity_item.setBackground(QColor(255, 193, 7))  # Bootstrap warning
                severity_item.setForeground(QColor(33, 37, 41))  # Dark text
            else:
                # Low: Blue with white text
                severity_item.setBackground(QColor(13, 110, 253))  # Bootstrap info blue
                severity_item.setForeground(QColor(255, 255, 255))  # White text
            self.incomplete_entities_table.setItem(row, 5, severity_item)
            
            # Set tooltip with detailed issues
            tooltip = f"Issues for {entity['primary_name']}:\n" + "\n".join(entity['issues'])
            for col in range(6):
                if self.incomplete_entities_table.item(row, col):
                    self.incomplete_entities_table.item(row, col).setToolTip(tooltip)
    
    def filter_incomplete_entities(self):
        """Filter incomplete entities based on selected criteria."""
        if not hasattr(self, 'all_incomplete_entities'):
            return
        
        severity_filter = self.severity_filter_combo.currentData()
        subject_type_filter = self.subject_type_filter_combo.currentData()
        
        filtered_entities = []
        
        for entity in self.all_incomplete_entities:
            # Apply severity filter with new amplified scale
            if severity_filter == "critical" and entity['max_severity'] < 8:
                continue
            elif severity_filter == "high" and entity['max_severity'] < 6:
                continue
            elif severity_filter == "medium" and entity['max_severity'] < 4:
                continue
            elif severity_filter == "low" and entity['max_severity'] >= 4:
                continue
            
            # Apply subject type filter
            if subject_type_filter != "all" and entity['subject_type'] != subject_type_filter:
                continue
            
            filtered_entities.append(entity)
        
        self.populate_incomplete_entities_table(filtered_entities)
    
    def edit_incomplete_entity(self):
        """Edit entity selected from incomplete entities table."""
        current_row = self.incomplete_entities_table.currentRow()
        if current_row >= 0:
            entity_id = self.incomplete_entities_table.item(current_row, 0).data(Qt.ItemDataRole.UserRole)
            dialog = CustomSanctionEntryDialog(self.service, entity_id=entity_id, parent=self)
            dialog.entity_saved.connect(self.on_entity_saved)
            dialog.exec()
    
    def on_tab_changed(self, index):
        """Handle tab change events."""
        if index == 2:  # Data Quality tab
            # Auto-refresh data quality when tab is opened
            QTimer.singleShot(100, self.refresh_data_quality)
    
    @pyqtSlot(str)
    def on_entity_saved(self, entity_id):
        """Handle entity saved signal."""
        # Refresh all relevant data
        self.load_entity_list()
        self.load_recent_entities()
        
        # If on data quality tab, refresh that too
        if self.tab_widget.currentIndex() == 2:
            QTimer.singleShot(500, self.refresh_data_quality)
    
    def import_xml(self):
        """Open the XML import dialog."""
        try:
            dialog = CustomSanctionsImportDialog(self.service, parent=self)
            result = dialog.exec()
            
            if result == QDialog.DialogCode.Accepted:
                # Refresh the entity list after successful import
                self.load_entity_list()
                self.load_recent_entities()
                
                # Show success message
                QMessageBox.information(
                    self,
                    "Import Completed",
                    "XML import completed successfully. The entity list has been refreshed."
                )
                
        except Exception as e:
            logger.error(f"Error opening import dialog: {e}")
            QMessageBox.critical(
                self,
                "Import Error",
                f"Failed to open import dialog:\n{str(e)}"
            )
    
    def export_xml(self):
        """Open the XML export dialog."""
        try:
            dialog = CustomSanctionsExportDialog(self.service, parent=self)
            dialog.exec()
            
        except Exception as e:
            logger.error(f"Error opening export dialog: {e}")
            QMessageBox.critical(
                self,
                "Export Error",
                f"Failed to open export dialog:\n{str(e)}"
            )
    
    # ==================== Data Quality Bulk Operations ====================
    
    def bulk_verify_entities(self):
        """Mark selected entities as verified with reviewer information."""
        selected_rows = self.incomplete_entities_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.information(self, "No Selection", "Please select entities to verify.")
            return
        
        # Get reviewer name
        from PyQt6.QtWidgets import QInputDialog
        reviewer, ok = QInputDialog.getText(
            self, "Bulk Verification", "Enter reviewer name:"
        )
        
        if not ok or not reviewer.strip():
            return
        
        # Get entity IDs
        entity_ids = []
        for row in selected_rows:
            entity_id = self.incomplete_entities_table.item(row.row(), 0).data(Qt.ItemDataRole.UserRole)
            entity_ids.append(entity_id)
        
        # Update entities with verification info
        updated_count = 0
        for entity_id in entity_ids:
            try:
                success, _ = self.service.update_sanction_entity(
                    entity_id, {
                        'verified_by': reviewer.strip(),
                        'verified_date': datetime.utcnow().date()
                    }
                )
                if success:
                    updated_count += 1
            except Exception as e:
                logger.error(f"Error verifying entity {entity_id}: {e}")
        
        if updated_count > 0:
            QMessageBox.information(
                self, "Verification Complete", 
                f"Successfully verified {updated_count} of {len(entity_ids)} entities."
            )
            # Refresh data quality analysis
            QTimer.singleShot(100, self.refresh_data_quality)
        else:
            QMessageBox.warning(self, "Verification Failed", "No entities were verified.")
    
    def bulk_assign_reviewer(self):
        """Assign reviewer to selected entities for future verification."""
        selected_rows = self.incomplete_entities_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.information(self, "No Selection", "Please select entities to assign reviewer.")
            return
        
        # Get reviewer name
        from PyQt6.QtWidgets import QInputDialog
        reviewer, ok = QInputDialog.getText(
            self, "Assign Reviewer", "Enter reviewer name for assignment:"
        )
        
        if not ok or not reviewer.strip():
            return
        
        # Get entity IDs
        entity_ids = []
        for row in selected_rows:
            entity_id = self.incomplete_entities_table.item(row.row(), 0).data(Qt.ItemDataRole.UserRole)
            entity_ids.append(entity_id)
        
        # Add note about reviewer assignment
        updated_count = 0
        for entity_id in entity_ids:
            try:
                success = self.service.add_internal_note(
                    entity_id, 
                    f"Assigned to reviewer: {reviewer.strip()} for data quality review"
                )
                if success:
                    updated_count += 1
            except Exception as e:
                logger.error(f"Error assigning reviewer to entity {entity_id}: {e}")
        
        if updated_count > 0:
            QMessageBox.information(
                self, "Assignment Complete", 
                f"Successfully assigned reviewer to {updated_count} of {len(entity_ids)} entities."
            )
            # Refresh data quality analysis
            QTimer.singleShot(100, self.refresh_data_quality)
        else:
            QMessageBox.warning(self, "Assignment Failed", "No entities were assigned.")
    
    def bulk_update_data_source(self):
        """Update data source for selected entities."""
        selected_rows = self.incomplete_entities_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.information(self, "No Selection", "Please select entities to update data source.")
            return
        
        # Get new data source
        from PyQt6.QtWidgets import QInputDialog
        data_source, ok = QInputDialog.getText(
            self, "Update Data Source", "Enter new data source:"
        )
        
        if not ok or not data_source.strip():
            return
        
        # Get entity IDs
        entity_ids = []
        for row in selected_rows:
            entity_id = self.incomplete_entities_table.item(row.row(), 0).data(Qt.ItemDataRole.UserRole)
            entity_ids.append(entity_id)
        
        # Update entities
        updated_count = 0
        for entity_id in entity_ids:
            try:
                success, _ = self.service.update_sanction_entity(
                    entity_id, {'data_source': data_source.strip()}
                )
                if success:
                    updated_count += 1
            except Exception as e:
                logger.error(f"Error updating data source for entity {entity_id}: {e}")
        
        if updated_count > 0:
            QMessageBox.information(
                self, "Update Complete", 
                f"Successfully updated data source for {updated_count} of {len(entity_ids)} entities."
            )
            # Refresh data quality analysis
            QTimer.singleShot(100, self.refresh_data_quality)
        else:
            QMessageBox.warning(self, "Update Failed", "No entities were updated.")
    
    def export_quality_report(self):
        """Export data quality analysis report."""
        if not hasattr(self, 'all_incomplete_entities'):
            QMessageBox.information(self, "No Data", "Please run data quality analysis first.")
            return
        
        from PyQt6.QtWidgets import QFileDialog
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Export Quality Report", "data_quality_report.csv", "CSV Files (*.csv)"
        )
        
        if file_path:
            try:
                import csv
                with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    
                    # Write header
                    writer.writerow([
                        'Internal ID', 'Primary Name', 'Subject Type', 'Record Status',
                        'Completeness %', 'Issue Count', 'Max Severity', 'Issues'
                    ])
                    
                    # Write data
                    for entity in self.all_incomplete_entities:
                        writer.writerow([
                            entity['internal_entry_id'],
                            entity['primary_name'],
                            entity['subject_type'],
                            entity['record_status'],
                            entity['completeness_percentage'],
                            entity['issue_count'],
                            entity['max_severity'],
                            '; '.join(entity['issues'])
                        ])
                
                QMessageBox.information(
                    self, "Export Complete", 
                    f"Quality report exported to:\n{file_path}"
                )
                
            except Exception as e:
                logger.error(f"Error exporting quality report: {e}")
                QMessageBox.critical(
                    self, "Export Error", 
                    f"Failed to export quality report:\n{str(e)}"
                )