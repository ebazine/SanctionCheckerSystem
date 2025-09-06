"""
Custom Sanctions Data Quality Widget

This widget provides a comprehensive dashboard for monitoring and maintaining
data quality in custom sanctions entries.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit,
    QComboBox, QTableWidget, QTableWidgetItem, QTabWidget, QGroupBox,
    QFormLayout, QMessageBox, QHeaderView, QAbstractItemView, QCheckBox,
    QProgressBar, QTextEdit, QFrame, QSplitter, QListWidget, QListWidgetItem,
    QGridLayout, QSizePolicy, QScrollArea, QSpinBox, QDateEdit, QDialog,
    QDialogButtonBox, QTreeWidget, QTreeWidgetItem, QApplication
)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QThread, pyqtSlot, QDate
from PyQt6.QtGui import QFont, QIcon, QColor, QPalette, QPixmap, QPainter
try:
    from PyQt6.QtCharts import QChart, QChartView, QPieSeries, QBarSeries, QBarSet, QBarCategoryAxis, QValueAxis
    CHARTS_AVAILABLE = True
except ImportError:
    # Charts module not available, use placeholder widgets
    CHARTS_AVAILABLE = False
    QChartView = QWidget

from ..models.base import RecordStatus
from ..services.custom_sanctions_data_quality_service import (
    CustomSanctionsDataQualityService, DataQualityReport, DataQualityIssue,
    BulkUpdateResult
)

logger = logging.getLogger(__name__)


class QualityAnalysisWorker(QThread):
    """Worker thread for generating quality reports without blocking UI."""
    
    analysis_complete = pyqtSignal(object)  # DataQualityReport
    analysis_error = pyqtSignal(str)  # error message
    
    def __init__(self, service: CustomSanctionsDataQualityService):
        super().__init__()
        self.service = service
    
    def run(self):
        """Generate quality report in background thread."""
        try:
            report = self.service.generate_quality_report()
            self.analysis_complete.emit(report)
        except Exception as e:
            logger.error(f"Error generating quality report: {e}")
            self.analysis_error.emit(str(e))


class BulkUpdateWorker(QThread):
    """Worker thread for bulk update operations."""
    
    update_complete = pyqtSignal(object)  # BulkUpdateResult
    update_error = pyqtSignal(str)  # error message
    progress_update = pyqtSignal(int, int)  # current, total
    
    def __init__(self, service: CustomSanctionsDataQualityService, operation: str, **kwargs):
        super().__init__()
        self.service = service
        self.operation = operation
        self.kwargs = kwargs
    
    def run(self):
        """Execute bulk update operation in background thread."""
        try:
            if self.operation == 'status_update':
                result = self.service.bulk_update_status(**self.kwargs)
            elif self.operation == 'field_update':
                result = self.service.bulk_update_field(**self.kwargs)
            elif self.operation == 'mark_verified':
                result = self.service.mark_entities_as_verified(**self.kwargs)
            elif self.operation == 'cleanup_outdated':
                result = self.service.cleanup_outdated_entities(**self.kwargs)
            else:
                raise ValueError(f"Unknown operation: {self.operation}")
            
            self.update_complete.emit(result)
        except Exception as e:
            logger.error(f"Error in bulk update operation: {e}")
            self.update_error.emit(str(e))


class BulkUpdateDialog(QDialog):
    """Dialog for configuring bulk update operations."""
    
    def __init__(self, parent=None, operation_type: str = 'status'):
        super().__init__(parent)
        self.operation_type = operation_type
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the dialog UI."""
        self.setWindowTitle(f"Bulk {self.operation_type.title()} Update")
        self.setModal(True)
        self.resize(400, 300)
        
        layout = QVBoxLayout(self)
        
        # Operation-specific controls
        if self.operation_type == 'status':
            self.setup_status_controls(layout)
        elif self.operation_type == 'field':
            self.setup_field_controls(layout)
        elif self.operation_type == 'cleanup':
            self.setup_cleanup_controls(layout)
        
        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def setup_status_controls(self, layout):
        """Set up controls for status update."""
        form_layout = QFormLayout()
        
        self.status_combo = QComboBox()
        for status in RecordStatus:
            self.status_combo.addItem(status.value, status)
        
        form_layout.addRow("New Status:", self.status_combo)
        layout.addLayout(form_layout)
    
    def setup_field_controls(self, layout):
        """Set up controls for field update."""
        form_layout = QFormLayout()
        
        self.field_combo = QComboBox()
        self.field_combo.addItems([
            'sanctioning_authority',
            'program',
            'data_source',
            'internal_notes'
        ])
        
        self.field_value = QLineEdit()
        
        form_layout.addRow("Field:", self.field_combo)
        form_layout.addRow("New Value:", self.field_value)
        layout.addLayout(form_layout)
    
    def setup_cleanup_controls(self, layout):
        """Set up controls for cleanup operation."""
        form_layout = QFormLayout()
        
        self.days_threshold = QSpinBox()
        self.days_threshold.setRange(30, 365)
        self.days_threshold.setValue(180)
        self.days_threshold.setSuffix(" days")
        
        self.dry_run = QCheckBox("Dry run (preview only)")
        self.dry_run.setChecked(True)
        
        form_layout.addRow("Days threshold:", self.days_threshold)
        form_layout.addRow("", self.dry_run)
        layout.addLayout(form_layout)
    
    def get_parameters(self) -> Dict[str, Any]:
        """Get the parameters for the bulk operation."""
        if self.operation_type == 'status':
            return {'new_status': self.status_combo.currentData()}
        elif self.operation_type == 'field':
            return {
                'field_name': self.field_combo.currentText(),
                'field_value': self.field_value.text()
            }
        elif self.operation_type == 'cleanup':
            return {
                'days_threshold': self.days_threshold.value(),
                'dry_run': self.dry_run.isChecked()
            }
        return {}


class CustomSanctionsDataQualityWidget(QWidget):
    """Widget for data quality monitoring and maintenance."""
    
    refresh_requested = pyqtSignal()
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.service = CustomSanctionsDataQualityService()
        self.current_report = None
        self.selected_entity_ids = []
        
        self.setup_ui()
        self.setup_connections()
        
        # Auto-refresh timer
        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self.refresh_data)
        self.refresh_timer.start(300000)  # Refresh every 5 minutes
    
    def setup_ui(self):
        """Set up the widget UI."""
        layout = QVBoxLayout(self)
        
        # Header with refresh button
        header_layout = QHBoxLayout()
        
        title_label = QLabel("Data Quality Dashboard")
        title_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        header_layout.addWidget(title_label)
        
        header_layout.addStretch()
        
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh_data)
        header_layout.addWidget(self.refresh_btn)
        
        layout.addLayout(header_layout)
        
        # Main content in tabs
        self.tab_widget = QTabWidget()
        
        # Overview tab
        self.overview_tab = self.create_overview_tab()
        self.tab_widget.addTab(self.overview_tab, "Overview")
        
        # Issues tab
        self.issues_tab = self.create_issues_tab()
        self.tab_widget.addTab(self.issues_tab, "Issues")
        
        # Maintenance tab
        self.maintenance_tab = self.create_maintenance_tab()
        self.tab_widget.addTab(self.maintenance_tab, "Maintenance")
        
        layout.addWidget(self.tab_widget)
        
        # Status bar
        self.status_label = QLabel("Ready")
        layout.addWidget(self.status_label)
    
    def create_overview_tab(self) -> QWidget:
        """Create the overview tab with statistics and charts."""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Statistics cards
        stats_layout = QGridLayout()
        
        self.total_entities_card = self.create_stat_card("Total Entities", "0", QColor(52, 152, 219))
        self.complete_entities_card = self.create_stat_card("Complete", "0", QColor(46, 204, 113))
        self.critical_issues_card = self.create_stat_card("Critical Issues", "0", QColor(231, 76, 60))
        self.outdated_entities_card = self.create_stat_card("Outdated", "0", QColor(241, 196, 15))
        
        stats_layout.addWidget(self.total_entities_card, 0, 0)
        stats_layout.addWidget(self.complete_entities_card, 0, 1)
        stats_layout.addWidget(self.critical_issues_card, 0, 2)
        stats_layout.addWidget(self.outdated_entities_card, 0, 3)
        
        layout.addLayout(stats_layout)
        
        # Charts
        charts_layout = QHBoxLayout()
        
        # Status distribution chart
        self.status_chart_view = QChartView()
        self.status_chart_view.setMinimumHeight(300)
        charts_layout.addWidget(self.status_chart_view)
        
        # Issues severity chart
        self.severity_chart_view = QChartView()
        self.severity_chart_view.setMinimumHeight(300)
        charts_layout.addWidget(self.severity_chart_view)
        
        layout.addLayout(charts_layout)
        
        # Recent activity
        activity_group = QGroupBox("Recent Activity")
        activity_layout = QVBoxLayout(activity_group)
        
        self.activity_list = QListWidget()
        self.activity_list.setMaximumHeight(150)
        activity_layout.addWidget(self.activity_list)
        
        layout.addWidget(activity_group)
        
        return widget
    
    def create_issues_tab(self) -> QWidget:
        """Create the issues tab with detailed issue listing."""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Filters
        filter_layout = QHBoxLayout()
        
        filter_layout.addWidget(QLabel("Severity:"))
        self.severity_filter = QComboBox()
        self.severity_filter.addItems(["All", "Critical", "Medium", "Low"])
        self.severity_filter.currentTextChanged.connect(self.filter_issues)
        filter_layout.addWidget(self.severity_filter)
        
        filter_layout.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All")
        self.type_filter.currentTextChanged.connect(self.filter_issues)
        filter_layout.addWidget(self.type_filter)
        
        filter_layout.addStretch()
        
        layout.addLayout(filter_layout)
        
        # Issues table
        self.issues_table = QTableWidget()
        self.issues_table.setColumnCount(6)
        self.issues_table.setHorizontalHeaderLabels([
            "Entity ID", "Severity", "Type", "Field", "Description", "Suggested Action"
        ])
        self.issues_table.horizontalHeader().setStretchLastSection(True)
        self.issues_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.issues_table.setAlternatingRowColors(True)
        
        layout.addWidget(self.issues_table)
        
        return widget
    
    def create_maintenance_tab(self) -> QWidget:
        """Create the maintenance tab with bulk operations."""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Entity selection
        selection_group = QGroupBox("Entity Selection")
        selection_layout = QVBoxLayout(selection_group)
        
        selection_buttons_layout = QHBoxLayout()
        
        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.clicked.connect(self.select_all_entities)
        selection_buttons_layout.addWidget(self.select_all_btn)
        
        self.select_none_btn = QPushButton("Select None")
        self.select_none_btn.clicked.connect(self.select_no_entities)
        selection_buttons_layout.addWidget(self.select_none_btn)
        
        self.select_issues_btn = QPushButton("Select Entities with Issues")
        self.select_issues_btn.clicked.connect(self.select_entities_with_issues)
        selection_buttons_layout.addWidget(self.select_issues_btn)
        
        selection_buttons_layout.addStretch()
        
        selection_layout.addLayout(selection_buttons_layout)
        
        self.selected_count_label = QLabel("0 entities selected")
        selection_layout.addWidget(self.selected_count_label)
        
        layout.addWidget(selection_group)
        
        # Bulk operations
        operations_group = QGroupBox("Bulk Operations")
        operations_layout = QGridLayout(operations_group)
        
        self.bulk_status_btn = QPushButton("Update Status")
        self.bulk_status_btn.clicked.connect(lambda: self.show_bulk_update_dialog('status'))
        operations_layout.addWidget(self.bulk_status_btn, 0, 0)
        
        self.bulk_field_btn = QPushButton("Update Field")
        self.bulk_field_btn.clicked.connect(lambda: self.show_bulk_update_dialog('field'))
        operations_layout.addWidget(self.bulk_field_btn, 0, 1)
        
        self.bulk_verify_btn = QPushButton("Mark as Verified")
        self.bulk_verify_btn.clicked.connect(self.bulk_mark_verified)
        operations_layout.addWidget(self.bulk_verify_btn, 1, 0)
        
        self.cleanup_btn = QPushButton("Cleanup Outdated")
        self.cleanup_btn.clicked.connect(lambda: self.show_bulk_update_dialog('cleanup'))
        operations_layout.addWidget(self.cleanup_btn, 1, 1)
        
        layout.addWidget(operations_group)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Results area
        results_group = QGroupBox("Operation Results")
        results_layout = QVBoxLayout(results_group)
        
        self.results_text = QTextEdit()
        self.results_text.setMaximumHeight(200)
        self.results_text.setReadOnly(True)
        results_layout.addWidget(self.results_text)
        
        layout.addWidget(results_group)
        
        layout.addStretch()
        
        return widget
    
    def create_stat_card(self, title: str, value: str, color: QColor) -> QWidget:
        """Create a statistics card widget."""
        card = QFrame()
        card.setFrameStyle(QFrame.Shape.Box)
        card.setStyleSheet(f"""
            QFrame {{
                border: 2px solid {color.name()};
                border-radius: 8px;
                background-color: white;
                padding: 10px;
            }}
        """)
        
        layout = QVBoxLayout(card)
        
        title_label = QLabel(title)
        title_label.setFont(QFont("Arial", 10))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)
        
        value_label = QLabel(value)
        value_label.setFont(QFont("Arial", 18, QFont.Weight.Bold))
        value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        value_label.setStyleSheet(f"color: {color.name()};")
        layout.addWidget(value_label)
        
        # Store references for updating
        card.title_label = title_label
        card.value_label = value_label
        
        return card
    
    def setup_connections(self):
        """Set up signal connections."""
        pass
    
    def refresh_data(self):
        """Refresh the data quality report."""
        self.status_label.setText("Generating quality report...")
        self.refresh_btn.setEnabled(False)
        
        # Start analysis in background thread
        self.analysis_worker = QualityAnalysisWorker(self.service)
        self.analysis_worker.analysis_complete.connect(self.on_analysis_complete)
        self.analysis_worker.analysis_error.connect(self.on_analysis_error)
        self.analysis_worker.start()
    
    @pyqtSlot(object)
    def on_analysis_complete(self, report: DataQualityReport):
        """Handle completion of quality analysis."""
        self.current_report = report
        self.update_overview_tab(report)
        self.update_issues_tab(report)
        
        self.status_label.setText(f"Report generated at {report.generated_at.strftime('%H:%M:%S')}")
        self.refresh_btn.setEnabled(True)
    
    @pyqtSlot(str)
    def on_analysis_error(self, error_message: str):
        """Handle analysis error."""
        self.status_label.setText(f"Error: {error_message}")
        self.refresh_btn.setEnabled(True)
        
        QMessageBox.warning(self, "Analysis Error", f"Failed to generate quality report:\n{error_message}")
    
    def update_overview_tab(self, report: DataQualityReport):
        """Update the overview tab with report data."""
        # Update stat cards
        self.total_entities_card.value_label.setText(str(report.total_entities))
        self.complete_entities_card.value_label.setText(str(report.completeness_stats.complete_entities))
        self.critical_issues_card.value_label.setText(str(report.completeness_stats.critical_issues))
        self.outdated_entities_card.value_label.setText(str(report.completeness_stats.outdated_entries))
        
        # Update status chart
        self.update_status_chart(report.entities_by_status)
        
        # Update severity chart
        self.update_severity_chart(report.issues_by_severity)
        
        # Update activity list
        self.update_activity_list(report.recent_activity)
    
    def update_status_chart(self, status_data: Dict[str, int]):
        """Update the status distribution pie chart."""
        if not CHARTS_AVAILABLE:
            # Create a simple text display instead of chart
            text_widget = QLabel()
            text_content = "Status Distribution:\n"
            for status, count in status_data.items():
                text_content += f"• {status}: {count}\n"
            text_widget.setText(text_content)
            text_widget.setAlignment(Qt.AlignmentFlag.AlignCenter)
            
            # Replace chart view content
            layout = self.status_chart_view.layout()
            if layout:
                for i in reversed(range(layout.count())):
                    layout.itemAt(i).widget().setParent(None)
            else:
                layout = QVBoxLayout(self.status_chart_view)
            layout.addWidget(text_widget)
            return
        
        series = QPieSeries()
        
        colors = {
            'Active': QColor(46, 204, 113),
            'Inactive': QColor(149, 165, 166),
            'Delisted': QColor(231, 76, 60),
            'Pending': QColor(241, 196, 15)
        }
        
        for status, count in status_data.items():
            if count > 0:
                slice = series.append(f"{status} ({count})", count)
                if status in colors:
                    slice.setBrush(colors[status])
        
        chart = QChart()
        chart.addSeries(series)
        chart.setTitle("Entities by Status")
        chart.legend().setAlignment(Qt.AlignmentFlag.AlignBottom)
        
        self.status_chart_view.setChart(chart)
    
    def update_severity_chart(self, severity_data: Dict[str, int]):
        """Update the issues severity bar chart."""
        if not CHARTS_AVAILABLE:
            # Create a simple text display instead of chart
            text_widget = QLabel()
            text_content = "Issues by Severity:\n"
            categories = ["Critical", "Medium", "Low"]
            for category in categories:
                count = severity_data.get(category.lower(), 0)
                text_content += f"• {category}: {count}\n"
            text_widget.setText(text_content)
            text_widget.setAlignment(Qt.AlignmentFlag.AlignCenter)
            
            # Replace chart view content
            layout = self.severity_chart_view.layout()
            if layout:
                for i in reversed(range(layout.count())):
                    layout.itemAt(i).widget().setParent(None)
            else:
                layout = QVBoxLayout(self.severity_chart_view)
            layout.addWidget(text_widget)
            return
        
        series = QBarSeries()
        bar_set = QBarSet("Issues")
        
        categories = ["Critical", "Medium", "Low"]
        colors = [QColor(231, 76, 60), QColor(241, 196, 15), QColor(52, 152, 219)]
        
        for category in categories:
            count = severity_data.get(category.lower(), 0)
            bar_set.append(count)
        
        series.append(bar_set)
        
        chart = QChart()
        chart.addSeries(series)
        chart.setTitle("Issues by Severity")
        
        axis_x = QBarCategoryAxis()
        axis_x.append(categories)
        chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        series.attachAxis(axis_x)
        
        axis_y = QValueAxis()
        chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        series.attachAxis(axis_y)
        
        self.severity_chart_view.setChart(chart)
    
    def update_activity_list(self, activity_data: Dict[str, int]):
        """Update the recent activity list."""
        self.activity_list.clear()
        
        for key, count in activity_data.items():
            description = key.replace('_', ' ').title()
            item = QListWidgetItem(f"{description}: {count}")
            self.activity_list.addItem(item)
    
    def update_issues_tab(self, report: DataQualityReport):
        """Update the issues tab with report data."""
        # Update type filter
        self.type_filter.clear()
        self.type_filter.addItem("All")
        for issue_type in report.issues_by_type.keys():
            self.type_filter.addItem(issue_type.replace('_', ' ').title())
        
        # Update issues table
        self.populate_issues_table(report.top_issues)
    
    def populate_issues_table(self, issues: List[DataQualityIssue]):
        """Populate the issues table with data."""
        self.issues_table.setRowCount(len(issues))
        
        for row, issue in enumerate(issues):
            self.issues_table.setItem(row, 0, QTableWidgetItem(issue.internal_entry_id))
            
            severity_item = QTableWidgetItem(issue.severity.title())
            if issue.severity == 'critical':
                severity_item.setBackground(QColor(231, 76, 60, 50))
            elif issue.severity == 'medium':
                severity_item.setBackground(QColor(241, 196, 15, 50))
            else:
                severity_item.setBackground(QColor(52, 152, 219, 50))
            self.issues_table.setItem(row, 1, severity_item)
            
            self.issues_table.setItem(row, 2, QTableWidgetItem(issue.issue_type.replace('_', ' ').title()))
            self.issues_table.setItem(row, 3, QTableWidgetItem(issue.field_name))
            self.issues_table.setItem(row, 4, QTableWidgetItem(issue.description))
            self.issues_table.setItem(row, 5, QTableWidgetItem(issue.suggested_action))
        
        self.issues_table.resizeColumnsToContents()
    
    def filter_issues(self):
        """Filter the issues table based on selected criteria."""
        if not self.current_report:
            return
        
        severity_filter = self.severity_filter.currentText().lower()
        type_filter = self.type_filter.currentText().lower().replace(' ', '_')
        
        filtered_issues = []
        for issue in self.current_report.top_issues:
            if severity_filter != "all" and issue.severity != severity_filter:
                continue
            if type_filter != "all" and issue.issue_type != type_filter:
                continue
            filtered_issues.append(issue)
        
        self.populate_issues_table(filtered_issues)
    
    def select_all_entities(self):
        """Select all entities for bulk operations."""
        if not self.current_report:
            return
        
        # This is a simplified implementation
        # In a real implementation, you'd need to get all entity IDs
        self.selected_entity_ids = []  # Would be populated with actual IDs
        self.update_selection_count()
    
    def select_no_entities(self):
        """Clear entity selection."""
        self.selected_entity_ids = []
        self.update_selection_count()
    
    def select_entities_with_issues(self):
        """Select entities that have data quality issues."""
        if not self.current_report:
            return
        
        # Get unique entity IDs from issues
        entity_ids = set()
        for issue in self.current_report.top_issues:
            entity_ids.add(issue.entity_id)
        
        self.selected_entity_ids = list(entity_ids)
        self.update_selection_count()
    
    def update_selection_count(self):
        """Update the selection count label."""
        count = len(self.selected_entity_ids)
        self.selected_count_label.setText(f"{count} entities selected")
        
        # Enable/disable bulk operation buttons
        has_selection = count > 0
        self.bulk_status_btn.setEnabled(has_selection)
        self.bulk_field_btn.setEnabled(has_selection)
        self.bulk_verify_btn.setEnabled(has_selection)
    
    def show_bulk_update_dialog(self, operation_type: str):
        """Show dialog for configuring bulk update operation."""
        if not self.selected_entity_ids and operation_type != 'cleanup':
            QMessageBox.warning(self, "No Selection", "Please select entities first.")
            return
        
        dialog = BulkUpdateDialog(self, operation_type)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            parameters = dialog.get_parameters()
            self.execute_bulk_operation(operation_type, parameters)
    
    def bulk_mark_verified(self):
        """Mark selected entities as verified."""
        if not self.selected_entity_ids:
            QMessageBox.warning(self, "No Selection", "Please select entities first.")
            return
        
        reply = QMessageBox.question(
            self, "Confirm Verification",
            f"Mark {len(self.selected_entity_ids)} entities as verified?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.execute_bulk_operation('mark_verified', {})
    
    def execute_bulk_operation(self, operation: str, parameters: Dict[str, Any]):
        """Execute a bulk operation."""
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        
        # Prepare parameters
        if operation != 'cleanup_outdated':
            parameters['entity_ids'] = self.selected_entity_ids
        parameters['user_id'] = 'current_user'  # Would be actual user ID
        
        # Start bulk operation in background thread
        self.bulk_worker = BulkUpdateWorker(self.service, operation, **parameters)
        self.bulk_worker.update_complete.connect(self.on_bulk_update_complete)
        self.bulk_worker.update_error.connect(self.on_bulk_update_error)
        self.bulk_worker.start()
    
    @pyqtSlot(object)
    def on_bulk_update_complete(self, result: BulkUpdateResult):
        """Handle completion of bulk update operation."""
        self.progress_bar.setVisible(False)
        
        # Display results
        results_text = f"""
Bulk Operation Results:
- Total Processed: {result.total_processed}
- Successful Updates: {result.successful_updates}
- Failed Updates: {result.failed_updates}

"""
        
        if result.errors:
            results_text += "Errors:\n"
            for error in result.errors[:10]:  # Show first 10 errors
                results_text += f"- {error}\n"
            
            if len(result.errors) > 10:
                results_text += f"... and {len(result.errors) - 10} more errors\n"
        
        self.results_text.setText(results_text)
        
        # Refresh data if updates were successful
        if result.successful_updates > 0:
            QTimer.singleShot(1000, self.refresh_data)  # Refresh after 1 second
    
    @pyqtSlot(str)
    def on_bulk_update_error(self, error_message: str):
        """Handle bulk update error."""
        self.progress_bar.setVisible(False)
        self.results_text.setText(f"Bulk operation failed: {error_message}")
        
        QMessageBox.warning(self, "Bulk Operation Error", f"Operation failed:\n{error_message}")