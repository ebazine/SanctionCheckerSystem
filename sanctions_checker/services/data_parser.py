"""
Data parser module for parsing sanctions lists in various formats.
"""
import xml.etree.ElementTree as ET
import csv
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import re

logger = logging.getLogger(__name__)


class DataParser:
    """Parses sanctions data from various file formats."""
    
    def __init__(self):
        """Initialize the data parser."""
        pass
    
    def parse_file(self, file_path: str, source_name: str, file_format: str) -> List[Dict[str, Any]]:
        """
        Parse a sanctions file based on its format and source.
        
        Args:
            file_path: Path to the file to parse
            source_name: Name of the data source (EU, UN, OFAC)
            file_format: File format (xml, csv, json)
            
        Returns:
            List of parsed entity dictionaries
            
        Raises:
            ValueError: If format or source is not supported
            FileNotFoundError: If file doesn't exist
        """
        if not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Parsing {source_name} {file_format.upper()} file: {file_path}")
        
        if file_format.lower() == 'xml':
            return self._parse_xml(file_path, source_name)
        elif file_format.lower() == 'csv':
            return self._parse_csv(file_path, source_name)
        elif file_format.lower() == 'json':
            return self._parse_json(file_path, source_name)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    
    def _parse_xml(self, file_path: str, source_name: str) -> List[Dict[str, Any]]:
        """
        Parse XML sanctions file.
        
        Args:
            file_path: Path to XML file
            source_name: Name of the data source
            
        Returns:
            List of parsed entity dictionaries
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            if source_name.upper() == 'EU':
                return self._parse_eu_xml(root)
            elif source_name.upper() == 'UN':
                return self._parse_un_xml(root)
            elif source_name.upper() == 'OFAC':
                return self._parse_ofac_xml(root)
            else:
                # Generic XML parser
                return self._parse_generic_xml(root, source_name)
                
        except ET.ParseError as e:
            logger.error(f"XML parsing error in {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error parsing XML file {file_path}: {e}")
            raise
    
    def _parse_eu_xml(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Parse EU consolidated sanctions list XML format."""
        entities = []
        
        # Handle namespace if present
        namespace = ''
        if '}' in root.tag:
            namespace = root.tag.split('}')[0] + '}'
        
        # EU XML structure: <export><sanctionEntity>
        search_pattern = f'.//{namespace}sanctionEntity' if namespace else './/sanctionEntity'
        for entity_elem in root.findall(search_pattern):
            try:
                # Get the primary name from the first nameAlias with strong="true"
                primary_name = None
                aliases = []
                
                name_alias_pattern = f'{namespace}nameAlias' if namespace else 'nameAlias'
                for name_alias in entity_elem.findall(name_alias_pattern):
                    whole_name = name_alias.get('wholeName', '').strip()
                    if whole_name:
                        if name_alias.get('strong') == 'true' and not primary_name:
                            primary_name = whole_name
                        else:
                            if whole_name != primary_name:
                                aliases.append(whole_name)
                
                if not primary_name:
                    # If no strong name found, use the first available name
                    first_alias = entity_elem.find(name_alias_pattern)
                    if first_alias is not None:
                        primary_name = first_alias.get('wholeName', '').strip()
                
                if not primary_name:
                    continue  # Skip entities without names
                
                # Get entity type from subjectType
                subject_type_pattern = f'{namespace}subjectType' if namespace else 'subjectType'
                subject_type_elem = entity_elem.find(subject_type_pattern)
                entity_type = 'UNKNOWN'
                if subject_type_elem is not None:
                    entity_type = subject_type_elem.get('code', 'UNKNOWN').upper()
                
                # Get regulation information
                regulation_pattern = f'{namespace}regulation' if namespace else 'regulation'
                regulation_elem = entity_elem.find(regulation_pattern)
                sanctions_type = 'EU_SANCTIONS'
                effective_date = None
                programme = None
                
                if regulation_elem is not None:
                    programme = regulation_elem.get('programme', '')
                    if programme:
                        sanctions_type = f'EU_{programme}'
                    
                    entry_date = regulation_elem.get('entryIntoForceDate')
                    if entry_date:
                        effective_date = self._parse_date(entry_date)
                
                entity = {
                    'source': 'EU',
                    'entity_type': entity_type,
                    'name': primary_name,
                    'aliases': aliases,
                    'sanctions_type': sanctions_type,
                    'effective_date': effective_date,
                    'additional_info': {}
                }
                
                # Add EU reference number
                eu_ref = entity_elem.get('euReferenceNumber')
                if eu_ref:
                    entity['additional_info']['eu_reference_number'] = eu_ref
                
                # Add UN ID if available
                un_id = entity_elem.get('unitedNationId')
                if un_id:
                    entity['additional_info']['united_nation_id'] = un_id
                
                # Add programme
                if programme:
                    entity['additional_info']['programme'] = programme
                
                # Add remark
                remark_pattern = f'{namespace}remark' if namespace else 'remark'
                remark_elem = entity_elem.find(remark_pattern)
                if remark_elem is not None and remark_elem.text:
                    entity['additional_info']['remark'] = remark_elem.text.strip()
                
                # Extract birth information
                birthdate_pattern = f'{namespace}birthdate' if namespace else 'birthdate'
                birthdate_elem = entity_elem.find(birthdate_pattern)
                if birthdate_elem is not None:
                    birth_info = {}
                    
                    birthdate = birthdate_elem.get('birthdate')
                    if birthdate:
                        birth_info['date'] = birthdate
                        entity['additional_info']['birth_date'] = birthdate
                    
                    city = birthdate_elem.get('city')
                    if city:
                        birth_info['city'] = city
                    
                    country = birthdate_elem.get('countryDescription')
                    if country:
                        birth_info['country'] = country
                    
                    if birth_info:
                        entity['additional_info']['birth_info'] = birth_info
                
                # Extract citizenship
                citizenship_pattern = f'{namespace}citizenship' if namespace else 'citizenship'
                citizenship_elem = entity_elem.find(citizenship_pattern)
                if citizenship_elem is not None:
                    country = citizenship_elem.get('countryDescription')
                    if country:
                        entity['additional_info']['citizenship'] = country
                
                entities.append(entity)
                    
            except Exception as e:
                logger.warning(f"Error parsing EU entity: {e}")
                continue
        
        logger.info(f"Parsed {len(entities)} entities from EU XML")
        return entities
    
    def _parse_un_xml(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Parse UN consolidated sanctions list XML format."""
        entities = []
        
        # UN XML structure - this is a generic implementation
        # In production, this would be tailored to the actual UN XML schema
        for individual in root.findall('.//INDIVIDUAL'):
            try:
                entity = {
                    'source': 'UN',
                    'entity_type': 'INDIVIDUAL',
                    'name': '',
                    'aliases': [],
                    'sanctions_type': 'UN_SANCTIONS',
                    'effective_date': None,
                    'additional_info': {}
                }
                
                # Extract names
                first_name = self._get_text_or_default(individual.find('FIRST_NAME'))
                second_name = self._get_text_or_default(individual.find('SECOND_NAME'))
                third_name = self._get_text_or_default(individual.find('THIRD_NAME'))
                fourth_name = self._get_text_or_default(individual.find('FOURTH_NAME'))
                
                name_parts = [n for n in [first_name, second_name, third_name, fourth_name] if n]
                entity['name'] = ' '.join(name_parts)
                
                # Extract UN number
                un_list_type = self._get_text_or_default(individual.find('UN_LIST_TYPE'))
                reference_number = self._get_text_or_default(individual.find('REFERENCE_NUMBER'))
                entity['additional_info']['un_list_type'] = un_list_type
                entity['additional_info']['reference_number'] = reference_number
                
                # Extract date of birth
                date_of_birth = self._get_text_or_default(individual.find('INDIVIDUAL_DATE_OF_BIRTH/DATE'))
                if date_of_birth:
                    entity['additional_info']['date_of_birth'] = date_of_birth
                
                # Extract place of birth
                place_of_birth = self._get_text_or_default(individual.find('INDIVIDUAL_PLACE_OF_BIRTH/VALUE'))
                if place_of_birth:
                    entity['additional_info']['place_of_birth'] = place_of_birth
                
                if entity['name']:
                    entities.append(entity)
                    
            except Exception as e:
                logger.warning(f"Error parsing UN individual: {e}")
                continue
        
        # Parse entities (organizations)
        for entity_elem in root.findall('.//ENTITY'):
            try:
                entity = {
                    'source': 'UN',
                    'entity_type': 'ENTITY',
                    'name': self._get_text_or_default(entity_elem.find('FIRST_NAME')),
                    'aliases': [],
                    'sanctions_type': 'UN_SANCTIONS',
                    'effective_date': None,
                    'additional_info': {}
                }
                
                # Extract UN information
                un_list_type = self._get_text_or_default(entity_elem.find('UN_LIST_TYPE'))
                reference_number = self._get_text_or_default(entity_elem.find('REFERENCE_NUMBER'))
                entity['additional_info']['un_list_type'] = un_list_type
                entity['additional_info']['reference_number'] = reference_number
                
                if entity['name']:
                    entities.append(entity)
                    
            except Exception as e:
                logger.warning(f"Error parsing UN entity: {e}")
                continue
        
        logger.info(f"Parsed {len(entities)} entities from UN XML")
        return entities
    
    def _parse_ofac_xml(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Parse OFAC SDN XML format."""
        entities = []
        
        # Handle namespace if present
        namespace = ''
        if '}' in root.tag:
            namespace = root.tag.split('}')[0] + '}'
        
        # OFAC XML structure: <sdnList><sdnEntry>
        search_pattern = f'.//{namespace}sdnEntry' if namespace else './/sdnEntry'
        for entry_elem in root.findall(search_pattern):
            try:
                # Extract basic information
                uid = self._get_text_or_default(entry_elem.find(f'{namespace}uid' if namespace else 'uid'))
                first_name = self._get_text_or_default(entry_elem.find(f'{namespace}firstName' if namespace else 'firstName'))
                last_name = self._get_text_or_default(entry_elem.find(f'{namespace}lastName' if namespace else 'lastName'))
                sdn_type = self._get_text_or_default(entry_elem.find(f'{namespace}sdnType' if namespace else 'sdnType'))
                
                # Construct name
                name_parts = [n for n in [first_name, last_name] if n]
                name = ' '.join(name_parts) if name_parts else last_name
                
                if not name:
                    continue  # Skip entries without names
                
                entity = {
                    'source': 'OFAC',
                    'entity_type': sdn_type.upper() if sdn_type else 'UNKNOWN',
                    'name': name,
                    'aliases': [],
                    'sanctions_type': 'OFAC_SDN',
                    'effective_date': None,
                    'additional_info': {}
                }
                
                # Add UID
                if uid:
                    entity['additional_info']['uid'] = uid
                
                # Extract title
                title = self._get_text_or_default(entry_elem.find(f'{namespace}title' if namespace else 'title'))
                if title:
                    entity['additional_info']['title'] = title
                
                # Extract remarks
                remarks = self._get_text_or_default(entry_elem.find(f'{namespace}remarks' if namespace else 'remarks'))
                if remarks:
                    entity['additional_info']['remarks'] = remarks
                
                # Extract programs
                program_list = []
                program_pattern = f'.//{namespace}program' if namespace else './/program'
                for program_elem in entry_elem.findall(program_pattern):
                    program_text = self._get_text_or_default(program_elem)
                    if program_text:
                        program_list.append(program_text)
                
                if program_list:
                    entity['additional_info']['programs'] = program_list
                
                # Extract aliases
                alias_pattern = f'.//{namespace}aka' if namespace else './/aka'
                for aka_elem in entry_elem.findall(alias_pattern):
                    aka_type = aka_elem.get('type', '')
                    aka_category = aka_elem.get('category', '')
                    
                    first_name_aka = self._get_text_or_default(aka_elem.find(f'{namespace}firstName' if namespace else 'firstName'))
                    last_name_aka = self._get_text_or_default(aka_elem.find(f'{namespace}lastName' if namespace else 'lastName'))
                    
                    aka_name_parts = [n for n in [first_name_aka, last_name_aka] if n]
                    aka_name = ' '.join(aka_name_parts) if aka_name_parts else last_name_aka
                    
                    if aka_name and aka_name != name:
                        entity['aliases'].append(aka_name)
                
                # Extract addresses
                addresses = []
                address_pattern = f'.//{namespace}address' if namespace else './/address'
                for addr_elem in entry_elem.findall(address_pattern):
                    address_parts = []
                    
                    address1 = self._get_text_or_default(addr_elem.find(f'{namespace}address1' if namespace else 'address1'))
                    if address1:
                        address_parts.append(address1)
                    
                    city = self._get_text_or_default(addr_elem.find(f'{namespace}city' if namespace else 'city'))
                    if city:
                        address_parts.append(city)
                    
                    country = self._get_text_or_default(addr_elem.find(f'{namespace}country' if namespace else 'country'))
                    if country:
                        address_parts.append(country)
                    
                    if address_parts:
                        addresses.append(', '.join(address_parts))
                
                if addresses:
                    entity['additional_info']['addresses'] = addresses
                
                # Extract dates of birth
                dob_pattern = f'.//{namespace}dateOfBirth' if namespace else './/dateOfBirth'
                for dob_elem in entry_elem.findall(dob_pattern):
                    dob_text = self._get_text_or_default(dob_elem)
                    if dob_text:
                        entity['additional_info']['date_of_birth'] = dob_text
                        break  # Use first date of birth found
                
                # Extract places of birth
                pob_pattern = f'.//{namespace}placeOfBirth' if namespace else './/placeOfBirth'
                for pob_elem in entry_elem.findall(pob_pattern):
                    pob_text = self._get_text_or_default(pob_elem)
                    if pob_text:
                        entity['additional_info']['place_of_birth'] = pob_text
                        break  # Use first place of birth found
                
                entities.append(entity)
                    
            except Exception as e:
                logger.warning(f"Error parsing OFAC entity: {e}")
                continue
        
        logger.info(f"Parsed {len(entities)} entities from OFAC XML")
        return entities
    
    def _parse_csv(self, file_path: str, source_name: str) -> List[Dict[str, Any]]:
        """
        Parse CSV sanctions file.
        
        Args:
            file_path: Path to CSV file
            source_name: Name of the data source
            
        Returns:
            List of parsed entity dictionaries
        """
        entities = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', newline='') as csvfile:
                # Try to detect delimiter
                sample = csvfile.read(1024)
                csvfile.seek(0)
                
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                
                if source_name.upper() == 'OFAC':
                    entities = self._parse_ofac_csv(reader)
                else:
                    entities = self._parse_generic_csv(reader, source_name)
                    
        except Exception as e:
            logger.error(f"Error parsing CSV file {file_path}: {e}")
            raise
        
        logger.info(f"Parsed {len(entities)} entities from {source_name} CSV")
        return entities
    
    def _parse_ofac_csv(self, reader: csv.DictReader) -> List[Dict[str, Any]]:
        """Parse OFAC SDN CSV format."""
        entities = []
        
        for row in reader:
            try:
                entity = {
                    'source': 'OFAC',
                    'entity_type': row.get('SDN_Type', 'UNKNOWN'),
                    'name': row.get('SDN_Name', '').strip(),
                    'aliases': [],
                    'sanctions_type': 'OFAC_SDN',
                    'effective_date': None,
                    'additional_info': {}
                }
                
                # Extract additional OFAC-specific information
                if 'Program' in row:
                    entity['additional_info']['program'] = row['Program']
                
                if 'Title' in row:
                    entity['additional_info']['title'] = row['Title']
                
                if 'Call_Sign' in row:
                    entity['additional_info']['call_sign'] = row['Call_Sign']
                
                if 'Vess_type' in row:
                    entity['additional_info']['vessel_type'] = row['Vess_type']
                
                if 'Tonnage' in row:
                    entity['additional_info']['tonnage'] = row['Tonnage']
                
                if 'GRT' in row:
                    entity['additional_info']['grt'] = row['GRT']
                
                if 'Vess_flag' in row:
                    entity['additional_info']['vessel_flag'] = row['Vess_flag']
                
                if 'Vess_owner' in row:
                    entity['additional_info']['vessel_owner'] = row['Vess_owner']
                
                if 'Remarks' in row:
                    entity['additional_info']['remarks'] = row['Remarks']
                
                if entity['name']:
                    entities.append(entity)
                    
            except Exception as e:
                logger.warning(f"Error parsing OFAC CSV row: {e}")
                continue
        
        return entities
    
    def _parse_json(self, file_path: str, source_name: str) -> List[Dict[str, Any]]:
        """
        Parse JSON sanctions file.
        
        Args:
            file_path: Path to JSON file
            source_name: Name of the data source
            
        Returns:
            List of parsed entity dictionaries
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as jsonfile:
                data = json.load(jsonfile)
                
            return self._parse_generic_json(data, source_name)
            
        except Exception as e:
            logger.error(f"Error parsing JSON file {file_path}: {e}")
            raise
    
    def _parse_generic_xml(self, root: ET.Element, source_name: str) -> List[Dict[str, Any]]:
        """Generic XML parser for unknown formats."""
        entities = []
        
        # Try to find entity-like elements
        possible_entity_tags = ['entity', 'person', 'individual', 'organization', 'record']
        
        for tag in possible_entity_tags:
            for elem in root.findall(f'.//{tag}'):
                entity = {
                    'source': source_name,
                    'entity_type': 'UNKNOWN',
                    'name': '',
                    'aliases': [],
                    'sanctions_type': f'{source_name}_SANCTIONS',
                    'effective_date': None,
                    'additional_info': {}
                }
                
                # Try to extract name from common name fields
                name_fields = ['name', 'fullname', 'full_name', 'entity_name', 'person_name']
                for field in name_fields:
                    name_elem = elem.find(field)
                    if name_elem is not None and name_elem.text:
                        entity['name'] = name_elem.text.strip()
                        break
                
                if entity['name']:
                    entities.append(entity)
        
        return entities
    
    def _parse_generic_csv(self, reader: csv.DictReader, source_name: str) -> List[Dict[str, Any]]:
        """Generic CSV parser for unknown formats."""
        entities = []
        
        # Try to identify name column
        fieldnames = reader.fieldnames or []
        name_column = None
        
        for field in fieldnames:
            if any(keyword in field.lower() for keyword in ['name', 'entity', 'person']):
                name_column = field
                break
        
        if not name_column and fieldnames:
            name_column = fieldnames[0]  # Use first column as fallback
        
        for row in reader:
            if name_column and row.get(name_column):
                entity = {
                    'source': source_name,
                    'entity_type': 'UNKNOWN',
                    'name': row[name_column].strip(),
                    'aliases': [],
                    'sanctions_type': f'{source_name}_SANCTIONS',
                    'effective_date': None,
                    'additional_info': {k: v for k, v in row.items() if k != name_column and v}
                }
                entities.append(entity)
        
        return entities
    
    def _parse_generic_json(self, data: Any, source_name: str) -> List[Dict[str, Any]]:
        """Generic JSON parser for unknown formats."""
        entities = []
        
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    entity = self._extract_entity_from_dict(item, source_name)
                    if entity:
                        entities.append(entity)
        elif isinstance(data, dict):
            # Check if it's a single entity or contains a list of entities
            for key, value in data.items():
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            entity = self._extract_entity_from_dict(item, source_name)
                            if entity:
                                entities.append(entity)
                    break
            else:
                # Treat the whole dict as a single entity
                entity = self._extract_entity_from_dict(data, source_name)
                if entity:
                    entities.append(entity)
        
        return entities
    
    def _extract_entity_from_dict(self, item: Dict, source_name: str) -> Optional[Dict[str, Any]]:
        """Extract entity information from a dictionary."""
        # Try to find name field
        name = None
        name_fields = ['name', 'fullname', 'full_name', 'entity_name', 'person_name']
        
        for field in name_fields:
            if field in item and item[field]:
                name = str(item[field]).strip()
                break
        
        if not name:
            return None
        
        entity = {
            'source': source_name,
            'entity_type': item.get('type', item.get('entity_type', 'UNKNOWN')),
            'name': name,
            'aliases': item.get('aliases', []),
            'sanctions_type': item.get('sanctions_type', f'{source_name}_SANCTIONS'),
            'effective_date': self._parse_date(item.get('effective_date')),
            'additional_info': {k: v for k, v in item.items() 
                             if k not in ['name', 'fullname', 'full_name', 'entity_name', 'person_name', 
                                        'type', 'entity_type', 'aliases', 'sanctions_type', 'effective_date']}
        }
        
        return entity
    
    def _get_text_or_default(self, element: Optional[ET.Element], default: str = '') -> str:
        """Get text from XML element or return default."""
        if element is not None and element.text:
            return element.text.strip()
        return default
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string into datetime object."""
        if not date_str:
            return None
        
        # Try common date formats
        date_formats = [
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%d-%m-%Y',
            '%Y%m%d'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def validate_parsed_data(self, entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate parsed entity data and return validation report.
        
        Args:
            entities: List of parsed entities
            
        Returns:
            Validation report dictionary
        """
        report = {
            'total_entities': len(entities),
            'valid_entities': 0,
            'invalid_entities': 0,
            'warnings': [],
            'errors': []
        }
        
        required_fields = ['source', 'entity_type', 'name', 'sanctions_type']
        
        for i, entity in enumerate(entities):
            is_valid = True
            
            # Check required fields
            for field in required_fields:
                if field not in entity or not entity[field]:
                    report['errors'].append(f"Entity {i}: Missing required field '{field}'")
                    is_valid = False
            
            # Check name length
            if entity.get('name') and len(entity['name']) < 2:
                report['warnings'].append(f"Entity {i}: Name too short: '{entity['name']}'")
            
            # Check for suspicious characters
            if entity.get('name'):
                if re.search(r'[<>{}]', entity['name']):
                    report['warnings'].append(f"Entity {i}: Name contains suspicious characters: '{entity['name']}'")
            
            if is_valid:
                report['valid_entities'] += 1
            else:
                report['invalid_entities'] += 1
        
        return report