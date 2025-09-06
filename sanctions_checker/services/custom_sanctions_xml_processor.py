"""
Custom Sanctions XML Processing Service

This service handles XML import/export operations for custom sanctions data,
including schema validation, transformation between database models and XML,
and error handling for malformed data.
"""

import os
import json
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import xml.etree.ElementTree as ET
from xml.dom import minidom

try:
    from lxml import etree
    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False

from ..models.custom_sanction_entity import CustomSanctionEntity
from ..models.custom_sanction_name import CustomSanctionName
from ..models.custom_sanction_individual import CustomSanctionIndividual
from ..models.custom_sanction_entity_details import CustomSanctionEntityDetails
from ..models.custom_sanction_address import CustomSanctionAddress
from ..models.custom_sanction_identifier import CustomSanctionIdentifier
from ..utils.logger import get_logger


@dataclass
class ValidationResult:
    """Result of XML validation operation."""
    is_valid: bool
    error_message: str = ""
    errors: List[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []


@dataclass
class ImportResult:
    """Result of XML import operation."""
    total_processed: int
    imported_count: int
    skipped_count: int
    error_count: int
    errors: List[str]
    
    @property
    def success(self) -> bool:
        """Check if import was successful (no errors)."""
        return self.error_count == 0


class CustomSanctionsXMLProcessor:
    """Handles XML import/export operations for custom sanctions."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.namespace = "https://www.yourcompany.com/sanctions"
        self.schema_path = os.path.join(os.path.dirname(__file__), "..", "..", "schemas", "custom_sanctions.xsd")
        self._schema = None
        
        # Initialize schema if lxml is available
        if LXML_AVAILABLE and os.path.exists(self.schema_path):
            try:
                with open(self.schema_path, 'r', encoding='utf-8') as f:
                    schema_doc = etree.parse(f)
                self._schema = etree.XMLSchema(schema_doc)
                self.logger.info("XML schema loaded successfully")
            except Exception as e:
                self.logger.error(f"Failed to load XML schema: {e}")
                self._schema = None
        else:
            if not LXML_AVAILABLE:
                self.logger.warning("lxml not available, schema validation disabled")
            if not os.path.exists(self.schema_path):
                self.logger.warning(f"Schema file not found: {self.schema_path}")
    
    def validate_against_schema(self, xml_content: str) -> ValidationResult:
        """
        Validate XML content against the custom sanctions schema.
        
        Args:
            xml_content: XML content as string
            
        Returns:
            ValidationResult with validation status and any errors
        """
        errors = []
        warnings = []
        
        if not LXML_AVAILABLE:
            warnings.append("lxml not available, schema validation skipped")
            return ValidationResult(is_valid=True, errors=errors, warnings=warnings)
        
        if not self._schema:
            warnings.append("Schema not loaded, validation skipped")
            return ValidationResult(is_valid=True, errors=errors, warnings=warnings)
        
        try:
            # Parse XML content
            xml_doc = etree.fromstring(xml_content.encode('utf-8'))
            
            # Validate against schema
            if not self._schema.validate(xml_doc):
                for error in self._schema.error_log:
                    errors.append(f"Line {error.line}: {error.message}")
                
                return ValidationResult(is_valid=False, errors=errors, warnings=warnings)
            
            self.logger.info("XML validation successful")
            return ValidationResult(is_valid=True, errors=errors, warnings=warnings)
            
        except etree.XMLSyntaxError as e:
            errors.append(f"XML syntax error: {e}")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)
        except Exception as e:
            errors.append(f"Validation error: {e}")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)
    
    def export_entities_to_xml(self, entities: List[CustomSanctionEntity], 
                              exported_by: str = "System") -> str:
        """
        Export custom sanction entities to XML format.
        
        Args:
            entities: List of CustomSanctionEntity objects to export
            exported_by: Name of user/system performing export
            
        Returns:
            XML content as string
        """
        try:
            # Create root element with namespace
            root = ET.Element("CustomSanctionsList")
            root.set("xmlns", self.namespace)
            root.set("version", "1.0")
            root.set("exportDate", datetime.utcnow().isoformat())
            
            # Add metadata
            metadata = ET.SubElement(root, "Metadata")
            ET.SubElement(metadata, "ExportedBy").text = exported_by
            ET.SubElement(metadata, "TotalEntries").text = str(len(entities))
            ET.SubElement(metadata, "Description").text = f"Custom sanctions export - {len(entities)} entries"
            
            # Add each entity
            for entity in entities:
                entry_element = self._transform_entity_to_xml_element(entity)
                root.append(entry_element)
            
            # Convert to pretty-printed string
            xml_str = ET.tostring(root, encoding='unicode')
            dom = minidom.parseString(xml_str)
            pretty_xml = dom.toprettyxml(indent="  ")
            
            # Remove empty lines and fix encoding declaration
            lines = [line for line in pretty_xml.split('\n') if line.strip()]
            if lines and lines[0].startswith('<?xml'):
                lines[0] = '<?xml version="1.0" encoding="UTF-8"?>'
            
            result = '\n'.join(lines)
            self.logger.info(f"Successfully exported {len(entities)} entities to XML")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to export entities to XML: {e}")
            raise
    
    def import_entities_from_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        Import custom sanction entities from XML content.
        
        Args:
            xml_content: XML content as string
            
        Returns:
            List of entity data dictionaries
        """
        try:
            # Parse XML
            root = ET.fromstring(xml_content)
            
            # Handle namespace
            if root.tag.startswith('{'):
                namespace = root.tag.split('}')[0] + '}'
            else:
                namespace = ''
            
            entities_data = []
            
            # Find all SanctionEntry elements
            entry_elements = root.findall(f"{namespace}SanctionEntry")
            if not entry_elements:
                # Try without namespace
                entry_elements = root.findall("SanctionEntry")
            
            for entry_element in entry_elements:
                entity_data = self._transform_xml_element_to_entity_data(entry_element, namespace)
                entities_data.append(entity_data)
            
            self.logger.info(f"Successfully parsed {len(entities_data)} entities from XML")
            return entities_data
            
        except ET.ParseError as e:
            self.logger.error(f"XML parsing error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to import entities from XML: {e}")
            raise
    
    def _transform_entity_to_xml_element(self, entity: CustomSanctionEntity) -> ET.Element:
        """Transform a CustomSanctionEntity to XML element."""
        entry = ET.Element("SanctionEntry")
        entry.set("id", entity.id)
        
        # Basic information
        ET.SubElement(entry, "InternalEntryId").text = entity.internal_entry_id
        ET.SubElement(entry, "SubjectType").text = entity.subject_type.value
        
        # Names
        names_elem = ET.SubElement(entry, "Names")
        for name in entity.names:
            name_info = ET.SubElement(names_elem, "NameInfo")
            ET.SubElement(name_info, "FullName").text = name.full_name
            ET.SubElement(name_info, "NameType").text = name.name_type.value
        
        # Individual details
        if entity.individual_details:
            individual = entity.individual_details[0]  # Should only be one
            ind_elem = ET.SubElement(entry, "IndividualDetails")
            
            # Birth info
            if any([individual.birth_year, individual.birth_month, individual.birth_day, 
                   individual.birth_full_date, individual.birth_note]):
                birth_info = ET.SubElement(ind_elem, "BirthInfo")
                if individual.birth_year:
                    ET.SubElement(birth_info, "BirthYear").text = str(individual.birth_year)
                if individual.birth_month:
                    ET.SubElement(birth_info, "BirthMonth").text = str(individual.birth_month)
                if individual.birth_day:
                    ET.SubElement(birth_info, "BirthDay").text = str(individual.birth_day)
                if individual.birth_full_date:
                    ET.SubElement(birth_info, "BirthFullDate").text = individual.birth_full_date.isoformat()
                if individual.birth_note:
                    ET.SubElement(birth_info, "BirthNote").text = individual.birth_note
            
            if individual.place_of_birth:
                ET.SubElement(ind_elem, "PlaceOfBirth").text = individual.place_of_birth
            
            # Nationalities
            if individual.nationalities:
                nationalities_elem = ET.SubElement(ind_elem, "Nationalities")
                for nationality in individual.nationalities:
                    ET.SubElement(nationalities_elem, "Nationality").text = nationality
        
        # Entity details
        if entity.entity_details:
            entity_detail = entity.entity_details[0]  # Should only be one
            ent_elem = ET.SubElement(entry, "EntityDetails")
            
            if entity_detail.registration_number:
                ET.SubElement(ent_elem, "RegistrationNumber").text = entity_detail.registration_number
            if entity_detail.registration_authority:
                ET.SubElement(ent_elem, "RegistrationAuthority").text = entity_detail.registration_authority
            if entity_detail.incorporation_date:
                ET.SubElement(ent_elem, "IncorporationDate").text = entity_detail.incorporation_date.isoformat()
            if entity_detail.company_type:
                ET.SubElement(ent_elem, "CompanyType").text = entity_detail.company_type
            if entity_detail.tax_id:
                ET.SubElement(ent_elem, "TaxId").text = entity_detail.tax_id
        
        # Addresses
        if entity.addresses:
            addresses_elem = ET.SubElement(entry, "Addresses")
            for address in entity.addresses:
                addr_info = ET.SubElement(addresses_elem, "AddressInfo")
                if address.street:
                    ET.SubElement(addr_info, "Street").text = address.street
                if address.city:
                    ET.SubElement(addr_info, "City").text = address.city
                if address.postal_code:
                    ET.SubElement(addr_info, "PostalCode").text = address.postal_code
                if address.country:
                    ET.SubElement(addr_info, "Country").text = address.country
                if address.full_address:
                    ET.SubElement(addr_info, "FullAddress").text = address.full_address
        
        # Identifiers
        if entity.identifiers:
            identifiers_elem = ET.SubElement(entry, "Identifiers")
            for identifier in entity.identifiers:
                id_info = ET.SubElement(identifiers_elem, "IdentifierInfo")
                ET.SubElement(id_info, "IdType").text = identifier.id_type
                ET.SubElement(id_info, "IdValue").text = identifier.id_value
                if identifier.issuing_country:
                    ET.SubElement(id_info, "IssuingCountry").text = identifier.issuing_country
                if identifier.notes:
                    ET.SubElement(id_info, "Notes").text = identifier.notes
        
        # Sanction details
        sanction_details = ET.SubElement(entry, "SanctionDetails")
        ET.SubElement(sanction_details, "SanctioningAuthority").text = entity.sanctioning_authority
        ET.SubElement(sanction_details, "Program").text = entity.program
        if entity.legal_basis:
            ET.SubElement(sanction_details, "LegalBasis").text = entity.legal_basis
        ET.SubElement(sanction_details, "ListingDate").text = entity.listing_date.isoformat()
        if entity.measures_imposed:
            ET.SubElement(sanction_details, "MeasuresImposed").text = entity.measures_imposed
        if entity.reason_for_listing:
            ET.SubElement(sanction_details, "ReasonForListing").text = entity.reason_for_listing
        
        # Internal metadata
        metadata = ET.SubElement(entry, "InternalMetadata")
        ET.SubElement(metadata, "DataSource").text = entity.data_source
        ET.SubElement(metadata, "RecordStatus").text = entity.record_status.value
        ET.SubElement(metadata, "LastUpdated").text = entity.last_updated.isoformat()
        if entity.internal_notes:
            ET.SubElement(metadata, "InternalNotes").text = entity.internal_notes
        if entity.created_by:
            ET.SubElement(metadata, "CreatedBy").text = entity.created_by
        if entity.verified_by:
            ET.SubElement(metadata, "VerifiedBy").text = entity.verified_by
        if entity.verified_date:
            ET.SubElement(metadata, "VerifiedDate").text = entity.verified_date.isoformat()
        
        return entry
    
    def _transform_xml_element_to_entity_data(self, element: ET.Element, namespace: str = '') -> Dict[str, Any]:
        """Transform XML element to entity data dictionary."""
        data = {}
        
        # Helper function to get text content
        def get_text(parent, tag_name):
            elem = parent.find(f"{namespace}{tag_name}")
            return elem.text if elem is not None and elem.text else None
        
        # Helper function to get date
        def get_date(parent, tag_name):
            text = get_text(parent, tag_name)
            if text:
                try:
                    return datetime.fromisoformat(text.replace('Z', '+00:00')).date()
                except:
                    return None
            return None
        
        # Helper function to get datetime
        def get_datetime(parent, tag_name):
            text = get_text(parent, tag_name)
            if text:
                try:
                    return datetime.fromisoformat(text.replace('Z', '+00:00'))
                except:
                    return None
            return None
        
        # Basic information
        data['id'] = element.get('id')
        data['internal_entry_id'] = get_text(element, 'InternalEntryId')
        data['subject_type'] = get_text(element, 'SubjectType')
        
        # Names
        names_elem = element.find(f"{namespace}Names")
        if names_elem is not None:
            data['names'] = []
            for name_info in names_elem.findall(f"{namespace}NameInfo"):
                name_data = {
                    'full_name': get_text(name_info, 'FullName'),
                    'name_type': get_text(name_info, 'NameType')
                }
                data['names'].append(name_data)
        
        # Individual details
        ind_elem = element.find(f"{namespace}IndividualDetails")
        if ind_elem is not None:
            individual_data = {}
            
            # Birth info
            birth_info = ind_elem.find(f"{namespace}BirthInfo")
            if birth_info is not None:
                birth_year_text = get_text(birth_info, 'BirthYear')
                if birth_year_text:
                    individual_data['birth_year'] = int(birth_year_text)
                
                birth_month_text = get_text(birth_info, 'BirthMonth')
                if birth_month_text:
                    individual_data['birth_month'] = int(birth_month_text)
                
                birth_day_text = get_text(birth_info, 'BirthDay')
                if birth_day_text:
                    individual_data['birth_day'] = int(birth_day_text)
                
                individual_data['birth_full_date'] = get_date(birth_info, 'BirthFullDate')
                individual_data['birth_note'] = get_text(birth_info, 'BirthNote')
            
            individual_data['place_of_birth'] = get_text(ind_elem, 'PlaceOfBirth')
            
            # Nationalities
            nationalities_elem = ind_elem.find(f"{namespace}Nationalities")
            if nationalities_elem is not None:
                nationalities = []
                for nat_elem in nationalities_elem.findall(f"{namespace}Nationality"):
                    if nat_elem.text:
                        nationalities.append(nat_elem.text)
                individual_data['nationalities'] = nationalities
            
            data['individual_details'] = individual_data
        
        # Entity details
        ent_elem = element.find(f"{namespace}EntityDetails")
        if ent_elem is not None:
            entity_data = {
                'registration_number': get_text(ent_elem, 'RegistrationNumber'),
                'registration_authority': get_text(ent_elem, 'RegistrationAuthority'),
                'incorporation_date': get_date(ent_elem, 'IncorporationDate'),
                'company_type': get_text(ent_elem, 'CompanyType'),
                'tax_id': get_text(ent_elem, 'TaxId')
            }
            data['entity_details'] = entity_data
        
        # Addresses
        addresses_elem = element.find(f"{namespace}Addresses")
        if addresses_elem is not None:
            data['addresses'] = []
            for addr_info in addresses_elem.findall(f"{namespace}AddressInfo"):
                address_data = {
                    'street': get_text(addr_info, 'Street'),
                    'city': get_text(addr_info, 'City'),
                    'postal_code': get_text(addr_info, 'PostalCode'),
                    'country': get_text(addr_info, 'Country'),
                    'full_address': get_text(addr_info, 'FullAddress')
                }
                data['addresses'].append(address_data)
        
        # Identifiers
        identifiers_elem = element.find(f"{namespace}Identifiers")
        if identifiers_elem is not None:
            data['identifiers'] = []
            for id_info in identifiers_elem.findall(f"{namespace}IdentifierInfo"):
                identifier_data = {
                    'id_type': get_text(id_info, 'IdType'),
                    'id_value': get_text(id_info, 'IdValue'),
                    'issuing_country': get_text(id_info, 'IssuingCountry'),
                    'notes': get_text(id_info, 'Notes')
                }
                data['identifiers'].append(identifier_data)
        
        # Sanction details
        sanction_elem = element.find(f"{namespace}SanctionDetails")
        if sanction_elem is not None:
            data['sanctioning_authority'] = get_text(sanction_elem, 'SanctioningAuthority')
            data['program'] = get_text(sanction_elem, 'Program')
            data['legal_basis'] = get_text(sanction_elem, 'LegalBasis')
            data['listing_date'] = get_date(sanction_elem, 'ListingDate')
            data['measures_imposed'] = get_text(sanction_elem, 'MeasuresImposed')
            data['reason_for_listing'] = get_text(sanction_elem, 'ReasonForListing')
        
        # Internal metadata
        metadata_elem = element.find(f"{namespace}InternalMetadata")
        if metadata_elem is not None:
            data['data_source'] = get_text(metadata_elem, 'DataSource')
            data['record_status'] = get_text(metadata_elem, 'RecordStatus')
            data['last_updated'] = get_datetime(metadata_elem, 'LastUpdated')
            data['internal_notes'] = get_text(metadata_elem, 'InternalNotes')
            data['created_by'] = get_text(metadata_elem, 'CreatedBy')
            data['verified_by'] = get_text(metadata_elem, 'VerifiedBy')
            data['verified_date'] = get_datetime(metadata_elem, 'VerifiedDate')
        
        return data
    
    def validate_xml_schema(self, xml_content: str) -> bool:
        """
        Simple validation check for XML schema compliance.
        
        Args:
            xml_content: XML content to validate
            
        Returns:
            True if valid, False otherwise
        """
        result = self.validate_against_schema(xml_content)
        return result.is_valid