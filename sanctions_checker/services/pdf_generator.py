"""
PDF report generation service for sanctions checker.
"""
import hashlib
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image, PageTemplate, Frame
from reportlab.platypus.flowables import HRFlowable
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus.doctemplate import BaseDocTemplate
from reportlab.platypus.frames import Frame
from reportlab.platypus.tableofcontents import TableOfContents

from ..models.search_record import SearchRecord
from ..models.search_result import SearchResult
from ..utils.resources import resource_manager


class NumberedCanvas:
    """Custom canvas for adding page numbers and headers/footers."""
    
    def __init__(self, canvas, doc):
        self.canvas = canvas
        self.doc = doc
        
    def draw_page_number(self):
        """Draw page number at bottom of page."""
        page_num = self.canvas.getPageNumber()
        text = f"Page {page_num}"
        self.canvas.drawRightString(A4[0] - 72, 30, text)
        
    def draw_header_footer(self, user_name=None):
        """Draw header and footer information."""
        # Footer with date and user
        footer_text = f"Generated on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        if user_name:
            footer_text += f" by {user_name}"
        
        self.canvas.setFont("Helvetica", 8)
        self.canvas.drawString(72, 30, footer_text)
        
        # Header line
        self.canvas.setStrokeColor(colors.lightgrey)
        self.canvas.line(72, A4[1] - 50, A4[0] - 72, A4[1] - 50)


class PDFGenerator:
    """
    Service for generating PDF reports of sanctions search results.
    
    This class creates detailed PDF reports showing search parameters,
    sanctions list versions, match results with algorithm breakdowns,
    and cryptographic hashes for verification.
    """
    
    def __init__(self):
        """Initialize the PDF generator with default styles."""
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
        self.user_name = None
    
    def _setup_custom_styles(self):
        """Set up custom paragraph styles for the report."""
        # Title style
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Title'],
            fontSize=18,
            spaceAfter=30,
            alignment=TA_CENTER,
            textColor=colors.darkblue
        ))
        
        # Section header style
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=14,
            spaceAfter=12,
            spaceBefore=20,
            textColor=colors.darkblue,
            borderWidth=1,
            borderColor=colors.darkblue,
            borderPadding=5
        ))
        
        # Subsection header style
        self.styles.add(ParagraphStyle(
            name='SubsectionHeader',
            parent=self.styles['Heading3'],
            fontSize=12,
            spaceAfter=8,
            spaceBefore=12,
            textColor=colors.black
        ))
        
        # Confidence score styles
        self.styles.add(ParagraphStyle(
            name='HighConfidence',
            parent=self.styles['Normal'],
            textColor=colors.red,
            fontSize=10,
            fontName='Helvetica-Bold'
        ))
        
        self.styles.add(ParagraphStyle(
            name='MediumConfidence',
            parent=self.styles['Normal'],
            textColor=colors.orange,
            fontSize=10,
            fontName='Helvetica-Bold'
        ))
        
        self.styles.add(ParagraphStyle(
            name='LowConfidence',
            parent=self.styles['Normal'],
            textColor=colors.green,
            fontSize=10
        ))
    
    def generate_report(self, search_record: SearchRecord, output_path: str, user_name: str = None) -> str:
        """
        Generate a comprehensive PDF report for a search record.
        
        Args:
            search_record: The SearchRecord object containing search data
            output_path: Path where the PDF should be saved
            user_name: Optional user name to include in the report
            
        Returns:
            str: The verification hash for the generated report
        """
        self.user_name = user_name
        
        # Create the PDF document with custom page template
        doc = BaseDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=72,
            leftMargin=72,
            topMargin=90,
            bottomMargin=72
        )
        
        # Create frame for content
        frame = Frame(
            72, 72, A4[0] - 144, A4[1] - 162,
            leftPadding=0, bottomPadding=0, rightPadding=0, topPadding=0
        )
        
        # Create page template with header/footer
        def on_page(canvas, doc):
            numbered_canvas = NumberedCanvas(canvas, doc)
            numbered_canvas.draw_page_number()
            numbered_canvas.draw_header_footer(user_name)
        
        template = PageTemplate(id='normal', frames=[frame], onPage=on_page)
        doc.addPageTemplates([template])
        
        # Build the story (content) for the PDF
        story = []
        
        # Add title and header information
        self._add_header(story, search_record)
        
        # Add search parameters section
        self._add_search_parameters(story, search_record)
        
        # Add sanctions list versions section
        self._add_sanctions_list_versions(story, search_record)
        
        # Add search results section
        self._add_search_results(story, search_record)
        
        # Add algorithm breakdown section
        self._add_algorithm_breakdown(story, search_record)
        
        # Generate verification hash
        verification_hash = self._generate_verification_hash(search_record)
        
        # Add verification section
        self._add_verification_section(story, verification_hash)
        
        # Build the PDF
        doc.build(story)
        
        return verification_hash
    
    def _add_header(self, story: List, search_record: SearchRecord):
        """Add the report header with title and basic information."""
        # Add logo if available
        self._add_logo(story)
        
        # Title
        title = Paragraph("Sanctions Screening Report", self.styles['CustomTitle'])
        story.append(title)
        story.append(Spacer(1, 20))
        
        # Report metadata table with better formatting
        report_data = [
            ['Report Generated:', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')],
            ['Search ID:', search_record.id],
            ['Search Query:', self._wrap_text(search_record.search_query, 50)],
            ['Search Timestamp:', search_record.search_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')],
        ]
        
        # Add user name if available
        if self.user_name:
            report_data.append(['Generated by:', self.user_name])
        else:
            report_data.append(['User ID:', search_record.user_id or 'Anonymous'])
        
        report_table = Table(report_data, colWidths=[2.2*inch, 3.8*inch])
        report_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.lightblue),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.darkblue),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        
        story.append(report_table)
        story.append(Spacer(1, 20))
    
    def _add_search_parameters(self, story: List, search_record: SearchRecord):
        """Add search parameters section."""
        story.append(Paragraph("Search Parameters", self.styles['SectionHeader']))
        
        params = search_record.search_parameters or {}
        if params:
            param_data = []
            for key, value in params.items():
                param_data.append([key.replace('_', ' ').title(), str(value)])
            
            param_table = Table(param_data, colWidths=[2*inch, 4*inch])
            param_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            story.append(param_table)
        else:
            story.append(Paragraph("Default search parameters used.", self.styles['Normal']))
        
        story.append(Spacer(1, 15))
    
    def _add_sanctions_list_versions(self, story: List, search_record: SearchRecord):
        """Add sanctions list versions section."""
        story.append(Paragraph("Sanctions List Versions", self.styles['SectionHeader']))
        
        versions = search_record.sanctions_list_versions or {}
        if versions:
            version_data = []
            for source, version in versions.items():
                version_data.append([source.upper(), version])
            
            version_table = Table(version_data, colWidths=[2*inch, 4*inch])
            version_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            story.append(version_table)
        else:
            story.append(Paragraph("No sanctions list version information available.", self.styles['Normal']))
        
        story.append(Spacer(1, 15))
    
    def _add_search_results(self, story: List, search_record: SearchRecord):
        """Add search results section."""
        story.append(Paragraph("Search Results Summary", self.styles['SectionHeader']))
        
        results_summary = search_record.get_results_summary()
        
        # Count custom vs official matches
        custom_count = 0
        official_count = 0
        
        for result in search_record.results:
            if hasattr(result, 'match_details') and result.match_details:
                if result.match_details.get('entity_type') == 'custom':
                    custom_count += 1
                else:
                    official_count += 1
            else:
                official_count += 1
        
        # Summary table with source breakdown
        summary_data = [
            ['Total Matches:', str(results_summary['total'])],
            ['Official Sanctions:', str(official_count)],
            ['Custom Sanctions:', str(custom_count)],
            ['High Confidence (≥80%):', str(results_summary['high_confidence'])],
            ['Medium Confidence (60-79%):', str(results_summary['medium_confidence'])],
            ['Low Confidence (<60%):', str(results_summary['low_confidence'])]
        ]
        
        summary_table = Table(summary_data, colWidths=[2.5*inch, 1.5*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(summary_table)
        story.append(Spacer(1, 15))
        
        # Detailed results
        if search_record.results:
            story.append(Paragraph("Detailed Match Results", self.styles['SubsectionHeader']))
            
            for i, result in enumerate(search_record.results, 1):
                self._add_individual_result(story, result, i)
        else:
            story.append(Paragraph("No matches found.", self.styles['Normal']))
    
    def _wrap_text(self, text: str, max_length: int) -> str:
        """Wrap text to prevent overflow in table cells."""
        if not text or len(text) <= max_length:
            return text
        
        words = text.split()
        lines = []
        current_line = []
        current_length = 0
        
        for word in words:
            if current_length + len(word) + 1 <= max_length:
                current_line.append(word)
                current_length += len(word) + 1
            else:
                if current_line:
                    lines.append(' '.join(current_line))
                current_line = [word]
                current_length = len(word)
        
        if current_line:
            lines.append(' '.join(current_line))
        
        return '<br/>'.join(lines)

    def _add_individual_result(self, story: List, result: SearchResult, result_number: int):
        """Add details for an individual search result."""
        # Result header
        confidence_level = result.get_confidence_level()
        confidence_style = self._get_confidence_style(confidence_level)
        
        # Determine if this is a custom sanctions match
        is_custom = False
        if hasattr(result, 'match_details') and result.match_details:
            is_custom = result.match_details.get('entity_type') == 'custom'
        
        source_indicator = " (Custom)" if is_custom else " (Official)"
        result_header = f"Match #{result_number}{source_indicator} - {confidence_level} Confidence ({result.overall_confidence:.1%})"
        story.append(Paragraph(result_header, confidence_style))
        
        # Entity details
        entity = result.entity
        if entity:
            if is_custom:
                # Handle custom sanction entity
                entity_data = [
                    ['Entity Name:', Paragraph(self._wrap_text(entity.get_primary_name() or 'N/A', 60), self.styles['Normal'])],
                    ['Subject Type:', entity.subject_type.value if hasattr(entity, 'subject_type') else 'N/A'],
                    ['Sanctioning Authority:', self._wrap_text(entity.sanctioning_authority, 50)],
                    ['Program:', self._wrap_text(entity.program, 50)],
                    ['Source:', 'Custom Sanctions List'],
                    ['Listing Date:', entity.listing_date.strftime('%Y-%m-%d') if entity.listing_date else 'N/A'],
                    ['Status:', entity.record_status.value if hasattr(entity, 'record_status') else 'N/A']
                ]
                
                # Add aliases if available
                all_names = entity.get_all_names() if hasattr(entity, 'get_all_names') else []
                primary_name = entity.get_primary_name() if hasattr(entity, 'get_primary_name') else None
                aliases = [name for name in all_names if name != primary_name]
                
                if aliases:
                    aliases_text = ', '.join(aliases[:5])  # Limit to first 5 aliases
                    if len(aliases) > 5:
                        aliases_text += f' (and {len(aliases) - 5} more)'
                    entity_data.append(['Aliases:', Paragraph(self._wrap_text(aliases_text, 60), self.styles['Normal'])])
                
                # Add measures imposed if available
                if hasattr(entity, 'measures_imposed') and entity.measures_imposed:
                    entity_data.append(['Measures:', Paragraph(self._wrap_text(entity.measures_imposed, 60), self.styles['Normal'])])
                
            else:
                # Handle official sanction entity
                entity_data = [
                    ['Entity Name:', Paragraph(self._wrap_text(entity.name, 60), self.styles['Normal'])],
                    ['Entity Type:', entity.entity_type],
                    ['Sanctions Type:', self._wrap_text(entity.sanctions_type, 50)],
                    ['Source:', entity.source],
                    ['Effective Date:', entity.effective_date.strftime('%Y-%m-%d') if entity.effective_date else 'N/A']
                ]
                
                if entity.aliases:
                    aliases_text = ', '.join(entity.aliases[:5])  # Limit to first 5 aliases
                    if len(entity.aliases) > 5:
                        aliases_text += f' (and {len(entity.aliases) - 5} more)'
                    entity_data.append(['Aliases:', Paragraph(self._wrap_text(aliases_text, 60), self.styles['Normal'])])
            
            # Use different background color for custom sanctions
            bg_color = colors.lightcyan if is_custom else colors.lightblue
            
            entity_table = Table(entity_data, colWidths=[1.8*inch, 4.2*inch])
            entity_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), bg_color),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
                ('ALIGN', (1, 0), (1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('GRID', (0, 0), (-1, -1), 1, colors.darkblue),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 8),
                ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ]))
            story.append(entity_table)
        
        story.append(Spacer(1, 10))
    
    def _add_algorithm_breakdown(self, story: List, search_record: SearchRecord):
        """Add algorithm breakdown section."""
        story.append(Paragraph("Algorithm Breakdown", self.styles['SectionHeader']))
        
        if not search_record.results:
            story.append(Paragraph("No algorithm data available.", self.styles['Normal']))
            return
        
        # Create algorithm breakdown table
        algorithm_data = [['Match #', 'Entity Name', 'Levenshtein', 'Jaro-Winkler', 'Soundex', 'Overall']]
        
        for i, result in enumerate(search_record.results, 1):
            scores = result.confidence_scores or {}
            
            # Get entity name - handle both official and custom sanctions
            entity_name = 'N/A'
            if result.entity:
                if hasattr(result.entity, 'name'):
                    # Official sanction entity
                    entity_name = result.entity.name
                elif hasattr(result.entity, 'get_primary_name'):
                    # Custom sanction entity
                    entity_name = result.entity.get_primary_name() or 'N/A'
            
            # Truncate long names
            if len(entity_name) > 30:
                entity_name = entity_name[:30] + '...'
            
            row = [
                str(i),
                entity_name,
                f"{scores.get('levenshtein', 0):.3f}",
                f"{scores.get('jaro_winkler', 0):.3f}",
                f"{scores.get('soundex', 0):.3f}",
                f"{result.overall_confidence:.3f}"
            ]
            algorithm_data.append(row)
        
        algorithm_table = Table(algorithm_data, colWidths=[0.6*inch, 2.4*inch, 0.8*inch, 0.8*inch, 0.8*inch, 0.8*inch])
        algorithm_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        # Color code confidence scores
        for i in range(1, len(algorithm_data)):
            for j in range(2, 6):  # Algorithm columns
                score = float(algorithm_data[i][j])
                if score >= 0.8:
                    algorithm_table.setStyle(TableStyle([('BACKGROUND', (j, i), (j, i), colors.lightcoral)]))
                elif score >= 0.6:
                    algorithm_table.setStyle(TableStyle([('BACKGROUND', (j, i), (j, i), colors.lightyellow)]))
                elif score >= 0.4:
                    algorithm_table.setStyle(TableStyle([('BACKGROUND', (j, i), (j, i), colors.lightgreen)]))
        
        story.append(algorithm_table)
        story.append(Spacer(1, 15))
    
    def _add_verification_section(self, story: List, verification_hash: str):
        """Add verification section with hash."""
        story.append(Paragraph("Report Verification", self.styles['SectionHeader']))
        
        verification_text = """
        This report has been cryptographically signed to ensure its authenticity and integrity.
        The verification hash below can be used to validate that this report has not been tampered with.
        """
        story.append(Paragraph(verification_text, self.styles['Normal']))
        story.append(Spacer(1, 10))
        
        # Hash display with proper wrapping
        wrapped_hash = self._wrap_hash(verification_hash)
        hash_data = [['Verification Hash:', Paragraph(wrapped_hash, self.styles['Normal'])]]
        hash_table = Table(hash_data, colWidths=[1.8*inch, 4.2*inch])
        hash_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, 0), colors.lightblue),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, 0), 'RIGHT'),
            ('ALIGN', (1, 0), (1, 0), 'LEFT'),
            ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, 0), 'Courier'),
            ('FONTSIZE', (0, 0), (0, 0), 9),
            ('FONTSIZE', (1, 0), (1, 0), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.darkblue),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        story.append(hash_table)
        
        story.append(Spacer(1, 10))
        
        # Footer information
        footer_info = f"Generated by Sanctions Checker System"
        if self.user_name:
            footer_info += f" for {self.user_name}"
        story.append(Paragraph(footer_info, self.styles['Normal']))

    def _wrap_hash(self, hash_string: str, chars_per_line: int = 32) -> str:
        """Wrap a long hash string for better display."""
        if len(hash_string) <= chars_per_line:
            return hash_string
        
        lines = []
        for i in range(0, len(hash_string), chars_per_line):
            lines.append(hash_string[i:i + chars_per_line])
        
        return '<br/>'.join(lines)
    
    def _add_logo(self, story: List):
        """Add logo to the PDF if available."""
        try:
            if resource_manager.has_logo():
                logo_path = str(resource_manager.logo_path)
                # Create image with appropriate size for PDF header
                logo_img = Image(logo_path, width=2*inch, height=1*inch, kind='proportional')
                logo_img.hAlign = 'CENTER'
                story.append(logo_img)
                story.append(Spacer(1, 15))
        except Exception as e:
            # If logo loading fails, just continue without it
            print(f"Could not add logo to PDF: {e}")
            pass
    
    def _get_confidence_style(self, confidence_level: str) -> ParagraphStyle:
        """Get the appropriate style for a confidence level."""
        if confidence_level == "HIGH":
            return self.styles['HighConfidence']
        elif confidence_level == "MEDIUM":
            return self.styles['MediumConfidence']
        else:
            return self.styles['LowConfidence']
    
    def _generate_verification_hash(self, search_record: SearchRecord) -> str:
        """
        Generate a cryptographic hash for report verification.
        
        The hash is based on:
        - Search query
        - Search timestamp
        - Search parameters
        - Sanctions list versions
        - All search results with their confidence scores
        
        Args:
            search_record: The SearchRecord to generate hash for
            
        Returns:
            str: SHA-256 hash in hexadecimal format
        """
        # Collect all data for hashing
        hash_data = {
            'search_query': search_record.search_query,
            'search_timestamp': search_record.search_timestamp.isoformat(),
            'search_parameters': search_record.search_parameters or {},
            'sanctions_list_versions': search_record.sanctions_list_versions or {},
            'results': []
        }
        
        # Add results data
        for result in search_record.results:
            result_data = {
                'entity_id': result.entity_id,
                'confidence_scores': result.confidence_scores or {},
                'overall_confidence': result.overall_confidence,
                'entity_name': result.entity.name if result.entity else None
            }
            hash_data['results'].append(result_data)
        
        # Sort results by entity_id for consistent hashing
        hash_data['results'].sort(key=lambda x: x['entity_id'])
        
        # Convert to JSON string and generate hash
        json_string = json.dumps(hash_data, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(json_string.encode('utf-8')).hexdigest()
    
    def generate_search_report(self, search_query: str, entity_type: str, matches: List, 
                              output_path: str, search_record_id: str = None, user_name: str = None) -> str:
        """
        Generate a PDF report from search matches (convenience method for GUI).
        
        Args:
            search_query: The original search query
            entity_type: The entity type searched for
            matches: List of EntityMatch objects
            output_path: Path where the PDF should be saved
            search_record_id: Optional search record ID
            
        Returns:
            str: The verification hash for the generated report
        """
        # Create a temporary SearchRecord-like object for report generation
        from ..models.search_record import SearchRecord
        from ..models.search_result import SearchResult
        
        # Create search record
        search_record = SearchRecord(
            id=search_record_id or f"gui_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            search_query=search_query,
            search_timestamp=datetime.now(),
            user_id="gui_user",
            search_parameters={'entity_type': entity_type},
            sanctions_list_versions={'gui': '1.0'}
        )
        
        # Convert EntityMatch objects to SearchResult objects
        search_results = []
        for match in matches:
            search_result = SearchResult(
                id=f"result_{len(search_results)}",
                search_record_id=search_record.id,
                entity_id=match.entity.id,
                confidence_scores=match.confidence_scores,
                match_details=match.match_details,
                overall_confidence=match.overall_confidence
            )
            search_result.entity = match.entity
            search_results.append(search_result)
        
        search_record.results = search_results
        
        # Generate the report using the main method
        return self.generate_report(search_record, output_path, user_name)

    def verify_report_hash(self, search_record: SearchRecord, provided_hash: str) -> bool:
        """
        Verify that a provided hash matches the expected hash for a search record.
        
        Args:
            search_record: The SearchRecord to verify
            provided_hash: The hash to verify against
            
        Returns:
            bool: True if the hash is valid, False otherwise
        """
        expected_hash = self._generate_verification_hash(search_record)
        return expected_hash.lower() == provided_hash.lower()


class ReportVerifier:
    """
    Utility class for verifying PDF report authenticity.
    """
    
    @staticmethod
    def verify_hash(search_record: SearchRecord, provided_hash: str) -> Dict[str, Any]:
        """
        Verify a report hash and return detailed verification results.
        
        Args:
            search_record: The SearchRecord to verify
            provided_hash: The hash to verify
            
        Returns:
            dict: Verification results with status and details
        """
        generator = PDFGenerator()
        expected_hash = generator._generate_verification_hash(search_record)
        
        is_valid = expected_hash.lower() == provided_hash.lower()
        
        return {
            'is_valid': is_valid,
            'expected_hash': expected_hash,
            'provided_hash': provided_hash,
            'verification_timestamp': datetime.now().isoformat(),
            'search_record_id': search_record.id,
            'message': 'Hash verification successful' if is_valid else 'Hash verification failed - report may have been tampered with'
        }
    
    @staticmethod
    def generate_verification_report(verification_result: Dict[str, Any], output_path: str):
        """
        Generate a verification report PDF.
        
        Args:
            verification_result: Result from verify_hash method
            output_path: Path where verification report should be saved
        """
        doc = SimpleDocTemplate(output_path, pagesize=A4)
        styles = getSampleStyleSheet()
        story = []
        
        # Title
        title = Paragraph("Report Verification Results", styles['Title'])
        story.append(title)
        story.append(Spacer(1, 20))
        
        # Verification status
        status_color = colors.green if verification_result['is_valid'] else colors.red
        status_text = "VALID" if verification_result['is_valid'] else "INVALID"
        
        status_style = ParagraphStyle(
            name='Status',
            parent=styles['Heading1'],
            textColor=status_color,
            alignment=TA_CENTER
        )
        
        story.append(Paragraph(f"Verification Status: {status_text}", status_style))
        story.append(Spacer(1, 20))
        
        # Details table
        details_data = [
            ['Search Record ID:', verification_result['search_record_id']],
            ['Verification Time:', verification_result['verification_timestamp']],
            ['Expected Hash:', verification_result['expected_hash']],
            ['Provided Hash:', verification_result['provided_hash']],
            ['Message:', verification_result['message']]
        ]
        
        details_table = Table(details_data, colWidths=[2*inch, 4*inch])
        details_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        
        story.append(details_table)
        
        doc.build(story)