#!/usr/bin/env python3
"""
Statistics Widget for displaying detailed analytics about sanctions data.
"""

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, 
                            QTableWidgetItem, QLabel, QGroupBox, QHeaderView,
                            QTabWidget, QPushButton, QTextEdit, QSplitter,
                            QTreeWidget, QTreeWidgetItem, QProgressBar)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QColor
from datetime import datetime
from typing import Dict, Optional
import json

from ..services.data_status_service import DataStatusService, DataStatistics


class StatisticsCalculationWorker(QThread):
    """Worker thread for calculating statistics."""
    
    progress = pyqtSignal(str)  # progress message
    statistics_ready = pyqtSignal(dict)  # statistics dict
    finished = pyqtSignal()
    
    def __init__(self, data_service: DataStatusService):
        super().__init__()
        self.data_service = data_service
    
    def run(self):
        """Calculate statistics for all data sources."""
        try:
            self.progress.emit("Calculating statistics...")
            statistics = self.data_service.get_all_statistics()
            self.statistics_ready.emit(statistics)
        except Exception as e:
            print(f"Error calculating statistics: {e}")
            self.statistics_ready.emit({})
        finally:
            self.finished.emit()


class StatisticsWidget(QWidget):
    """Widget for displaying detailed statistics about sanctions data."""
    
    def __init__(self, config, data_service: DataStatusService):
        super().__init__()
        self.config = config
        self.data_service = data_service
        self.statistics = {}
        self.calculation_worker = None
        
        self.init_ui()
        self.setup_timer()
        self.refresh_statistics()
    
    def init_ui(self):
        """Initialize the user interface."""
        layout = QVBoxLayout(self)
        
        # Title and controls
        header_layout = QHBoxLayout()
        
        title = QLabel("Sanctions Data Statistics")
        title.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        header_layout.addWidget(title)
        
        header_layout.addStretch()
        
        self.refresh_btn = QPushButton("🔄 Refresh Statistics")
        self.refresh_btn.clicked.connect(self.refresh_statistics)
        header_layout.addWidget(self.refresh_btn)
        
        self.export_btn = QPushButton("📊 Export Statistics")
        self.export_btn.clicked.connect(self.export_statistics)
        header_layout.addWidget(self.export_btn)
        
        layout.addLayout(header_layout)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Main content area
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left side - Overview and source selection
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        
        # Overview section
        self.create_overview_section()
        left_layout.addWidget(self.overview_group)
        
        # Source list
        self.create_source_list()
        left_layout.addWidget(self.source_list)
        
        splitter.addWidget(left_widget)
        
        # Right side - Detailed statistics
        self.create_details_tabs()
        splitter.addWidget(self.details_tabs)
        
        # Set splitter proportions
        splitter.setSizes([300, 700])
        
        layout.addWidget(splitter)
        
        # Status bar
        self.status_label = QLabel("Ready")
        layout.addWidget(self.status_label)
    
    def create_overview_section(self):
        """Create the overview statistics section."""
        self.overview_group = QGroupBox("Overview")
        layout = QVBoxLayout(self.overview_group)
        
        # Summary metrics
        metrics_layout = QHBoxLayout()
        
        self.total_entities_label = QLabel("Total Entities: 0")
        self.total_entities_label.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        metrics_layout.addWidget(self.total_entities_label)
        
        self.total_individuals_label = QLabel("Individuals: 0")
        metrics_layout.addWidget(self.total_individuals_label)
        
        self.total_organizations_label = QLabel("Organizations: 0")
        metrics_layout.addWidget(self.total_organizations_label)
        
        layout.addLayout(metrics_layout)
        
        # Sources summary
        self.sources_summary_label = QLabel("Sources: 0 loaded")
        layout.addWidget(self.sources_summary_label)
        
        # Last updated
        self.last_updated_label = QLabel("Last Updated: Never")
        layout.addWidget(self.last_updated_label)
    
    def create_source_list(self):
        """Create the source selection list."""
        self.source_list = QTreeWidget()
        self.source_list.setHeaderLabel("Data Sources")
        self.source_list.itemClicked.connect(self.on_source_selected)
        
        # Add "All Sources" item
        all_item = QTreeWidgetItem(["All Sources"])
        all_item.setData(0, Qt.ItemDataRole.UserRole, "ALL")
        self.source_list.addTopLevelItem(all_item)
        
        # Will be populated when statistics are loaded
    
    def create_details_tabs(self):
        """Create the detailed statistics tabs."""
        self.details_tabs = QTabWidget()
        
        # Entity Types tab
        self.entity_types_table = QTableWidget()
        self.entity_types_table.setColumnCount(3)
        self.entity_types_table.setHorizontalHeaderLabels(["Entity Type", "Count", "Percentage"])
        self.details_tabs.addTab(self.entity_types_table, "Entity Types")
        
        # Countries tab
        self.countries_table = QTableWidget()
        self.countries_table.setColumnCount(3)
        self.countries_table.setHorizontalHeaderLabels(["Country", "Count", "Percentage"])
        self.details_tabs.addTab(self.countries_table, "Countries")
        
        # Timeline tab
        self.timeline_table = QTableWidget()
        self.timeline_table.setColumnCount(3)
        self.timeline_table.setHorizontalHeaderLabels(["Year", "Count", "Percentage"])
        self.details_tabs.addTab(self.timeline_table, "Timeline")
        
        # Raw Data tab
        self.raw_data_text = QTextEdit()
        self.raw_data_text.setReadOnly(True)
        self.raw_data_text.setFont(QFont("Courier", 9))
        self.details_tabs.addTab(self.raw_data_text, "Raw Data")
        
        # Configure table headers
        for table in [self.entity_types_table, self.countries_table, self.timeline_table]:
            header = table.horizontalHeader()
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
            # Remove alternating row colors for better readability
            table.setAlternatingRowColors(False)
            table.setSortingEnabled(True)
    
    def setup_timer(self):
        """Setup automatic refresh timer."""
        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_statistics)
        self.timer.start(600000)  # Refresh every 10 minutes
    
    def refresh_statistics(self):
        """Refresh statistics data."""
        if self.calculation_worker and self.calculation_worker.isRunning():
            return
        
        # Disable refresh button and show progress
        self.refresh_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        self.status_label.setText("Calculating statistics...")
        
        # Start calculation worker
        self.calculation_worker = StatisticsCalculationWorker(self.data_service)
        self.calculation_worker.progress.connect(self.status_label.setText)
        self.calculation_worker.statistics_ready.connect(self.on_statistics_ready)
        self.calculation_worker.finished.connect(self.on_calculation_finished)
        self.calculation_worker.start()
    
    def on_statistics_ready(self, statistics: Dict[str, DataStatistics]):
        """Handle new statistics data."""
        self.statistics = statistics
        self.update_overview()
        self.update_source_list()
        self.update_details("ALL")  # Show combined statistics initially
    
    def on_calculation_finished(self):
        """Handle calculation completion."""
        self.refresh_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.status_label.setText(f"Statistics updated at {datetime.now().strftime('%H:%M:%S')}")
    
    def update_overview(self):
        """Update the overview section."""
        if not self.statistics:
            return
        
        # Calculate totals
        total_entities = sum(stat.total_entities for stat in self.statistics.values())
        total_individuals = sum(stat.individuals for stat in self.statistics.values())
        total_organizations = sum(stat.organizations for stat in self.statistics.values())
        
        # Update labels
        self.total_entities_label.setText(f"Total Entities: {total_entities:,}")
        self.total_individuals_label.setText(f"Individuals: {total_individuals:,}")
        self.total_organizations_label.setText(f"Organizations: {total_organizations:,}")
        
        # Sources summary
        loaded_sources = len([s for s in self.statistics.values() if s.total_entities > 0])
        total_sources = len(self.statistics)
        self.sources_summary_label.setText(f"Sources: {loaded_sources}/{total_sources} loaded")
        
        # Last updated
        last_updates = [s.last_updated for s in self.statistics.values() if s.last_updated]
        if last_updates:
            latest = max(last_updates)
            self.last_updated_label.setText(f"Last Updated: {latest.strftime('%Y-%m-%d %H:%M')}")
    
    def update_source_list(self):
        """Update the source list."""
        # Clear existing items (except "All Sources")
        root = self.source_list.invisibleRootItem()
        while root.childCount() > 1:
            root.removeChild(root.child(1))
        
        # Add source items
        for source_id, stat in self.statistics.items():
            item = QTreeWidgetItem([f"{source_id} ({stat.total_entities:,} entities)"])
            item.setData(0, Qt.ItemDataRole.UserRole, source_id)
            
            # Use icons instead of background colors for better readability
            if stat.total_entities > 0:
                item.setText(0, f"✅ {source_id} ({stat.total_entities:,} entities)")
            else:
                item.setText(0, f"❌ {source_id} (0 entities)")
            
            self.source_list.addTopLevelItem(item)
        
        # Expand all items
        self.source_list.expandAll()
    
    def on_source_selected(self, item, column):
        """Handle source selection."""
        source_id = item.data(0, Qt.ItemDataRole.UserRole)
        self.update_details(source_id)
    
    def update_details(self, source_id: str):
        """Update the detailed statistics tabs."""
        if source_id == "ALL":
            self.update_combined_details()
        else:
            self.update_source_details(source_id)
    
    def update_combined_details(self):
        """Update details for all sources combined."""
        if not self.statistics:
            return
        
        # Combine all statistics
        combined_countries = {}
        combined_entity_types = {}
        combined_date_ranges = {}
        
        for stat in self.statistics.values():
            # Combine countries
            for country, count in stat.countries.items():
                combined_countries[country] = combined_countries.get(country, 0) + count
            
            # Combine entity types
            for entity_type, count in stat.entity_types.items():
                combined_entity_types[entity_type] = combined_entity_types.get(entity_type, 0) + count
            
            # Combine date ranges
            for year, count in stat.date_ranges.items():
                combined_date_ranges[year] = combined_date_ranges.get(year, 0) + count
        
        # Update tables
        self.populate_table(self.entity_types_table, combined_entity_types)
        self.populate_table(self.countries_table, combined_countries)
        self.populate_table(self.timeline_table, combined_date_ranges)
        
        # Update raw data
        raw_data = {
            "source": "All Sources Combined",
            "entity_types": combined_entity_types,
            "countries": combined_countries,
            "timeline": combined_date_ranges,
            "generated_at": datetime.now().isoformat()
        }
        self.raw_data_text.setPlainText(json.dumps(raw_data, indent=2))
    
    def update_source_details(self, source_id: str):
        """Update details for a specific source."""
        if source_id not in self.statistics:
            return
        
        stat = self.statistics[source_id]
        
        # Update tables
        self.populate_table(self.entity_types_table, stat.entity_types)
        self.populate_table(self.countries_table, stat.countries)
        self.populate_table(self.timeline_table, stat.date_ranges)
        
        # Update raw data
        raw_data = {
            "source": stat.source_name,
            "total_entities": stat.total_entities,
            "individuals": stat.individuals,
            "organizations": stat.organizations,
            "entity_types": stat.entity_types,
            "countries": stat.countries,
            "timeline": stat.date_ranges,
            "last_updated": stat.last_updated.isoformat() if stat.last_updated else None,
            "generated_at": datetime.now().isoformat()
        }
        self.raw_data_text.setPlainText(json.dumps(raw_data, indent=2))
    
    def populate_table(self, table: QTableWidget, data: Dict[str, int]):
        """Populate a table with data."""
        if not data:
            table.setRowCount(0)
            return
        
        # Sort data by count (descending)
        sorted_data = sorted(data.items(), key=lambda x: x[1], reverse=True)
        total = sum(data.values())
        
        table.setRowCount(len(sorted_data))
        
        for row, (key, count) in enumerate(sorted_data):
            # Key (country, entity type, year, etc.)
            table.setItem(row, 0, QTableWidgetItem(str(key)))
            
            # Count
            count_item = QTableWidgetItem(f"{count:,}")
            count_item.setData(Qt.ItemDataRole.UserRole, count)  # For sorting
            table.setItem(row, 1, count_item)
            
            # Percentage
            percentage = (count / total * 100) if total > 0 else 0
            percentage_item = QTableWidgetItem(f"{percentage:.1f}%")
            percentage_item.setData(Qt.ItemDataRole.UserRole, percentage)  # For sorting
            table.setItem(row, 2, percentage_item)
            
            # Use text indicators instead of background colors for high percentages
            if percentage > 10:
                key_item = table.item(row, 0)
                key_item.setText(f"⭐ {key}")  # Add star for high percentages
    
    def export_statistics(self):
        """Export statistics to a file."""
        try:
            from PyQt6.QtWidgets import QFileDialog
            
            filename, _ = QFileDialog.getSaveFileName(
                self, "Export Statistics", 
                f"sanctions_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                "JSON Files (*.json);;All Files (*)"
            )
            
            if filename:
                # Prepare export data
                export_data = {
                    "export_info": {
                        "generated_at": datetime.now().isoformat(),
                        "total_sources": len(self.statistics),
                        "application": "Sanctions Checker"
                    },
                    "summary": {
                        "total_entities": sum(s.total_entities for s in self.statistics.values()),
                        "total_individuals": sum(s.individuals for s in self.statistics.values()),
                        "total_organizations": sum(s.organizations for s in self.statistics.values())
                    },
                    "sources": {}
                }
                
                # Add detailed statistics for each source
                for source_id, stat in self.statistics.items():
                    export_data["sources"][source_id] = {
                        "name": stat.source_name,
                        "total_entities": stat.total_entities,
                        "individuals": stat.individuals,
                        "organizations": stat.organizations,
                        "countries": stat.countries,
                        "entity_types": stat.entity_types,
                        "timeline": stat.date_ranges,
                        "last_updated": stat.last_updated.isoformat() if stat.last_updated else None
                    }
                
                # Write to file
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, indent=2, ensure_ascii=False)
                
                self.status_label.setText(f"Statistics exported to {filename}")
                
        except Exception as e:
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.warning(self, "Export Error", f"Failed to export statistics: {str(e)}")
    
    def closeEvent(self, event):
        """Handle widget close event."""
        if self.calculation_worker and self.calculation_worker.isRunning():
            self.calculation_worker.terminate()
            self.calculation_worker.wait()
        
        if hasattr(self, 'timer'):
            self.timer.stop()
        
        event.accept()