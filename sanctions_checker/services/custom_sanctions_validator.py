"""
Custom sanctions validation service for data quality and duplicate detection.

This module provides comprehensive validation for custom sanctions data including
field validation, format checking, duplicate detection, and data quality assessment.
"""
import re
import logging
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime, date
from dataclasses import dataclass, field
from enum import Enum

from ..models.base import SubjectType, NameType, RecordStatus

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """Represents a single validation issue."""
    field: str
    message: str
    severity: ValidationSeverity
    code: str
    value: Any = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'field': self.field,
            'message': self.message,
            'severity': self.severity.value,
            'code': self.code,
            'value': self.value
        }


@dataclass
class ValidationResult:
    """Result of validation operation."""
    is_valid: bool = True
    issues: List[ValidationIssue] = field(default_factory=list)
    warnings_count: int = 0
    errors_count: int = 0
    info_count: int = 0
    
    def add_issue(self, issue: ValidationIssue):
        """Add a validation issue."""
        self.issues.append(issue)
        if issue.severity == ValidationSeverity.ERROR:
            self.errors_count += 1
            self.is_valid = False
        elif issue.severity == ValidationSeverity.WARNING:
            self.warnings_count += 1
        elif issue.severity == ValidationSeverity.INFO:
            self.info_count += 1
    
    def add_error(self, field: str, message: str, code: str, value: Any = None):
        """Add an error issue."""
        self.add_issue(ValidationIssue(field, message, ValidationSeverity.ERROR, code, value))
    
    def add_warning(self, field: str, message: str, code: str, value: Any = None):
        """Add a warning issue."""
        self.add_issue(ValidationIssue(field, message, ValidationSeverity.WARNING, code, value))
    
    def add_info(self, field: str, message: str, code: str, value: Any = None):
        """Add an info issue."""
        self.add_issue(ValidationIssue(field, message, ValidationSeverity.INFO, code, value))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'is_valid': self.is_valid,
            'errors_count': self.errors_count,
            'warnings_count': self.warnings_count,
            'info_count': self.info_count,
            'issues': [issue.to_dict() for issue in self.issues]
        }


@dataclass
class DuplicateMatch:
    """Represents a potential duplicate match."""
    entity_id: str
    internal_entry_id: str
    match_type: str  # 'name', 'identifier', 'registration_number'
    match_value: str
    confidence: float  # 0.0 to 1.0
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'entity_id': self.entity_id,
            'internal_entry_id': self.internal_entry_id,
            'match_type': self.match_type,
            'match_value': self.match_value,
            'confidence': self.confidence,
            'details': self.details
        }


@dataclass
class DuplicateDetectionResult:
    """Result of duplicate detection operation."""
    has_duplicates: bool = False
    matches: List[DuplicateMatch] = field(default_factory=list)
    
    def add_match(self, match: DuplicateMatch):
        """Add a duplicate match."""
        self.matches.append(match)
        self.has_duplicates = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'has_duplicates': self.has_duplicates,
            'matches': [match.to_dict() for match in self.matches]
        }


class CustomSanctionsValidator:
    """
    Validator for custom sanctions data with comprehensive validation rules.
    
    Provides field validation, format checking, duplicate detection,
    and data quality assessment for custom sanctions entities.
    """
    
    # Validation patterns
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    PHONE_PATTERN = re.compile(r'^[\+]?[1-9][\d]{0,15}$')
    POSTAL_CODE_PATTERNS = {
        'US': re.compile(r'^\d{5}(-\d{4})?$'),
        'UK': re.compile(r'^[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][A-Z]{2}$'),
        'CA': re.compile(r'^[A-Z]\d[A-Z] \d[A-Z]\d$'),
        'DE': re.compile(r'^\d{5}$'),
        'FR': re.compile(r'^\d{5}$'),
    }
    
    # Country codes (ISO 3166-1 alpha-2)
    VALID_COUNTRY_CODES = {
        'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AQ', 'AR', 'AS', 'AT',
        'AU', 'AW', 'AX', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI',
        'BJ', 'BL', 'BM', 'BN', 'BO', 'BQ', 'BR', 'BS', 'BT', 'BV', 'BW', 'BY',
        'BZ', 'CA', 'CC', 'CD', 'CF', 'CG', 'CH', 'CI', 'CK', 'CL', 'CM', 'CN',
        'CO', 'CR', 'CU', 'CV', 'CW', 'CX', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM',
        'DO', 'DZ', 'EC', 'EE', 'EG', 'EH', 'ER', 'ES', 'ET', 'FI', 'FJ', 'FK',
        'FM', 'FO', 'FR', 'GA', 'GB', 'GD', 'GE', 'GF', 'GG', 'GH', 'GI', 'GL',
        'GM', 'GN', 'GP', 'GQ', 'GR', 'GS', 'GT', 'GU', 'GW', 'GY', 'HK', 'HM',
        'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IM', 'IN', 'IO', 'IQ', 'IR',
        'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KM', 'KN',
        'KP', 'KR', 'KW', 'KY', 'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS',
        'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MF', 'MG', 'MH', 'MK',
        'ML', 'MM', 'MN', 'MO', 'MP', 'MQ', 'MR', 'MS', 'MT', 'MU', 'MV', 'MW',
        'MX', 'MY', 'MZ', 'NA', 'NC', 'NE', 'NF', 'NG', 'NI', 'NL', 'NO', 'NP',
        'NR', 'NU', 'NZ', 'OM', 'PA', 'PE', 'PF', 'PG', 'PH', 'PK', 'PL', 'PM',
        'PN', 'PR', 'PS', 'PT', 'PW', 'PY', 'QA', 'RE', 'RO', 'RS', 'RU', 'RW',
        'SA', 'SB', 'SC', 'SD', 'SE', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL', 'SM',
        'SN', 'SO', 'SR', 'SS', 'ST', 'SV', 'SX', 'SY', 'SZ', 'TC', 'TD', 'TF',
        'TG', 'TH', 'TJ', 'TK', 'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TV', 'TW',
        'TZ', 'UA', 'UG', 'UM', 'US', 'UY', 'UZ', 'VA', 'VC', 'VE', 'VG', 'VI',
        'VN', 'VU', 'WF', 'WS', 'YE', 'YT', 'ZA', 'ZM', 'ZW'
    }
    
    def __init__(self):
        """Initialize the validator."""
        self.logger = logging.getLogger(__name__)
    
    def validate_entity_data(self, entity_data: Dict[str, Any]) -> ValidationResult:
        """
        Validate complete entity data including all related information.
        
        Args:
            entity_data: Dictionary containing entity data to validate
            
        Returns:
            ValidationResult with all validation issues
        """
        result = ValidationResult()
        
        try:
            # Validate core entity fields
            self._validate_core_entity_fields(entity_data, result)
            
            # Validate subject type specific fields
            subject_type = entity_data.get('subject_type')
            if subject_type:
                if subject_type == SubjectType.INDIVIDUAL.value:
                    self._validate_individual_fields(entity_data.get('individual_details', {}), result)
                elif subject_type == SubjectType.ENTITY.value:
                    self._validate_entity_fields(entity_data.get('entity_details', {}), result)
            
            # Validate names
            names = entity_data.get('names', [])
            self._validate_names(names, result)
            
            # Validate addresses
            addresses = entity_data.get('addresses', [])
            self._validate_addresses(addresses, result)
            
            # Validate identifiers
            identifiers = entity_data.get('identifiers', [])
            self._validate_identifiers(identifiers, result)
            
            # Validate sanction details
            self._validate_sanction_details(entity_data, result)
            
            # Validate metadata
            self._validate_metadata(entity_data, result)
            
        except Exception as e:
            self.logger.error(f"Error during validation: {e}")
            result.add_error('validation', f'Validation error: {str(e)}', 'VALIDATION_ERROR')
        
        return result
    
    def _validate_core_entity_fields(self, entity_data: Dict[str, Any], result: ValidationResult):
        """Validate core entity fields."""
        # Internal entry ID
        internal_entry_id = entity_data.get('internal_entry_id')
        if not internal_entry_id:
            result.add_error('internal_entry_id', 'Internal entry ID is required', 'REQUIRED_FIELD')
        elif not isinstance(internal_entry_id, str):
            result.add_error('internal_entry_id', 'Internal entry ID must be a string', 'INVALID_TYPE')
        elif len(internal_entry_id.strip()) == 0:
            result.add_error('internal_entry_id', 'Internal entry ID cannot be empty', 'EMPTY_FIELD')
        elif len(internal_entry_id) > 50:
            result.add_error('internal_entry_id', 'Internal entry ID cannot exceed 50 characters', 'MAX_LENGTH')
        
        # Subject type
        subject_type = entity_data.get('subject_type')
        if not subject_type:
            result.add_error('subject_type', 'Subject type is required', 'REQUIRED_FIELD')
        elif subject_type not in [st.value for st in SubjectType]:
            result.add_error('subject_type', f'Invalid subject type: {subject_type}', 'INVALID_VALUE', subject_type)
        
        # Sanctioning authority
        sanctioning_authority = entity_data.get('sanctioning_authority')
        if sanctioning_authority is None:
            result.add_error('sanctioning_authority', 'Sanctioning authority is required', 'REQUIRED_FIELD')
        elif not isinstance(sanctioning_authority, str):
            result.add_error('sanctioning_authority', 'Sanctioning authority must be a string', 'INVALID_TYPE')
        elif len(sanctioning_authority.strip()) == 0:
            result.add_error('sanctioning_authority', 'Sanctioning authority cannot be empty', 'EMPTY_FIELD')
        elif len(sanctioning_authority) > 200:
            result.add_error('sanctioning_authority', 'Sanctioning authority cannot exceed 200 characters', 'MAX_LENGTH')
        
        # Program
        program = entity_data.get('program')
        if not program:
            result.add_error('program', 'Program is required', 'REQUIRED_FIELD')
        elif not isinstance(program, str):
            result.add_error('program', 'Program must be a string', 'INVALID_TYPE')
        elif len(program.strip()) == 0:
            result.add_error('program', 'Program cannot be empty', 'EMPTY_FIELD')
        elif len(program) > 200:
            result.add_error('program', 'Program cannot exceed 200 characters', 'MAX_LENGTH')
        
        # Listing date
        listing_date = entity_data.get('listing_date')
        if not listing_date:
            result.add_error('listing_date', 'Listing date is required', 'REQUIRED_FIELD')
        else:
            self._validate_date_field(listing_date, 'listing_date', result)
            
            # Check if listing date is in the future
            if isinstance(listing_date, (date, datetime)):
                if listing_date > date.today():
                    result.add_warning('listing_date', 'Listing date is in the future', 'FUTURE_DATE', listing_date)
            elif isinstance(listing_date, str):
                try:
                    parsed_date = datetime.fromisoformat(listing_date.replace('Z', '+00:00')).date()
                    if parsed_date > date.today():
                        result.add_warning('listing_date', 'Listing date is in the future', 'FUTURE_DATE', listing_date)
                except ValueError:
                    pass  # Already handled by _validate_date_field
        
        # Data source
        data_source = entity_data.get('data_source')
        if not data_source:
            result.add_error('data_source', 'Data source is required', 'REQUIRED_FIELD')
        elif not isinstance(data_source, str):
            result.add_error('data_source', 'Data source must be a string', 'INVALID_TYPE')
        elif len(data_source.strip()) == 0:
            result.add_error('data_source', 'Data source cannot be empty', 'EMPTY_FIELD')
        elif len(data_source) > 500:
            result.add_error('data_source', 'Data source cannot exceed 500 characters', 'MAX_LENGTH')
        
        # Record status
        record_status = entity_data.get('record_status')
        if record_status and record_status not in [rs.value for rs in RecordStatus]:
            result.add_error('record_status', f'Invalid record status: {record_status}', 'INVALID_VALUE', record_status)
    
    def _validate_individual_fields(self, individual_data: Dict[str, Any], result: ValidationResult):
        """Validate individual-specific fields."""
        if not individual_data:
            return
        
        # Birth year validation
        birth_year = individual_data.get('birth_year')
        if birth_year is not None:
            if not isinstance(birth_year, int):
                result.add_error('individual_details.birth_year', 'Birth year must be an integer', 'INVALID_TYPE')
            elif birth_year < 1900 or birth_year > datetime.now().year:
                result.add_error('individual_details.birth_year', f'Birth year must be between 1900 and {datetime.now().year}', 'INVALID_RANGE', birth_year)
        
        # Birth month validation
        birth_month = individual_data.get('birth_month')
        if birth_month is not None:
            if not isinstance(birth_month, int):
                result.add_error('individual_details.birth_month', 'Birth month must be an integer', 'INVALID_TYPE')
            elif birth_month < 1 or birth_month > 12:
                result.add_error('individual_details.birth_month', 'Birth month must be between 1 and 12', 'INVALID_RANGE', birth_month)
        
        # Birth day validation
        birth_day = individual_data.get('birth_day')
        if birth_day is not None:
            if not isinstance(birth_day, int):
                result.add_error('individual_details.birth_day', 'Birth day must be an integer', 'INVALID_TYPE')
            elif birth_day < 1 or birth_day > 31:
                result.add_error('individual_details.birth_day', 'Birth day must be between 1 and 31', 'INVALID_RANGE', birth_day)
        
        # Birth full date validation
        birth_full_date = individual_data.get('birth_full_date')
        if birth_full_date:
            self._validate_date_field(birth_full_date, 'individual_details.birth_full_date', result)
        
        # Place of birth validation
        place_of_birth = individual_data.get('place_of_birth')
        if place_of_birth:
            if not isinstance(place_of_birth, str):
                result.add_error('individual_details.place_of_birth', 'Place of birth must be a string', 'INVALID_TYPE')
            elif len(place_of_birth) > 200:
                result.add_error('individual_details.place_of_birth', 'Place of birth cannot exceed 200 characters', 'MAX_LENGTH')
        
        # Nationalities validation
        nationalities = individual_data.get('nationalities', [])
        if nationalities:
            if not isinstance(nationalities, list):
                result.add_error('individual_details.nationalities', 'Nationalities must be a list', 'INVALID_TYPE')
            else:
                for i, nationality in enumerate(nationalities):
                    if not isinstance(nationality, str):
                        result.add_error(f'individual_details.nationalities[{i}]', 'Nationality must be a string', 'INVALID_TYPE')
                    elif len(nationality.strip()) == 0:
                        result.add_error(f'individual_details.nationalities[{i}]', 'Nationality cannot be empty', 'EMPTY_FIELD')
                    elif len(nationality) > 100:
                        result.add_error(f'individual_details.nationalities[{i}]', 'Nationality cannot exceed 100 characters', 'MAX_LENGTH')
    
    def _validate_entity_fields(self, entity_details: Dict[str, Any], result: ValidationResult):
        """Validate entity-specific fields."""
        if not entity_details:
            return
        
        # Registration number validation
        registration_number = entity_details.get('registration_number')
        if registration_number:
            if not isinstance(registration_number, str):
                result.add_error('entity_details.registration_number', 'Registration number must be a string', 'INVALID_TYPE')
            elif len(registration_number.strip()) == 0:
                result.add_error('entity_details.registration_number', 'Registration number cannot be empty', 'EMPTY_FIELD')
            elif len(registration_number) > 100:
                result.add_error('entity_details.registration_number', 'Registration number cannot exceed 100 characters', 'MAX_LENGTH')
        
        # Registration authority validation
        registration_authority = entity_details.get('registration_authority')
        if registration_authority:
            if not isinstance(registration_authority, str):
                result.add_error('entity_details.registration_authority', 'Registration authority must be a string', 'INVALID_TYPE')
            elif len(registration_authority) > 200:
                result.add_error('entity_details.registration_authority', 'Registration authority cannot exceed 200 characters', 'MAX_LENGTH')
        
        # Incorporation date validation
        incorporation_date = entity_details.get('incorporation_date')
        if incorporation_date:
            self._validate_date_field(incorporation_date, 'entity_details.incorporation_date', result)
        
        # Company type validation
        company_type = entity_details.get('company_type')
        if company_type:
            if not isinstance(company_type, str):
                result.add_error('entity_details.company_type', 'Company type must be a string', 'INVALID_TYPE')
            elif len(company_type) > 100:
                result.add_error('entity_details.company_type', 'Company type cannot exceed 100 characters', 'MAX_LENGTH')
        
        # Tax ID validation
        tax_id = entity_details.get('tax_id')
        if tax_id:
            if not isinstance(tax_id, str):
                result.add_error('entity_details.tax_id', 'Tax ID must be a string', 'INVALID_TYPE')
            elif len(tax_id.strip()) == 0:
                result.add_error('entity_details.tax_id', 'Tax ID cannot be empty', 'EMPTY_FIELD')
            elif len(tax_id) > 100:
                result.add_error('entity_details.tax_id', 'Tax ID cannot exceed 100 characters', 'MAX_LENGTH')
    
    def _validate_names(self, names: List[Dict[str, Any]], result: ValidationResult):
        """Validate names and aliases."""
        if not names:
            result.add_error('names', 'At least one name is required', 'REQUIRED_FIELD')
            return
        
        primary_names = []
        seen_names = set()
        
        for i, name_data in enumerate(names):
            if not isinstance(name_data, dict):
                result.add_error(f'names[{i}]', 'Name must be a dictionary', 'INVALID_TYPE')
                continue
            
            # Full name validation
            full_name = name_data.get('full_name')
            if not full_name:
                result.add_error(f'names[{i}].full_name', 'Full name is required', 'REQUIRED_FIELD')
            elif not isinstance(full_name, str):
                result.add_error(f'names[{i}].full_name', 'Full name must be a string', 'INVALID_TYPE')
            elif len(full_name.strip()) == 0:
                result.add_error(f'names[{i}].full_name', 'Full name cannot be empty', 'EMPTY_FIELD')
            elif len(full_name) < 2:
                result.add_warning(f'names[{i}].full_name', 'Full name is very short', 'SHORT_NAME', full_name)
            elif len(full_name) > 1000:
                result.add_error(f'names[{i}].full_name', 'Full name cannot exceed 1000 characters', 'MAX_LENGTH')
            else:
                # Check for duplicate names
                name_lower = full_name.lower().strip()
                if name_lower in seen_names:
                    result.add_warning(f'names[{i}].full_name', f'Duplicate name: {full_name}', 'DUPLICATE_NAME', full_name)
                else:
                    seen_names.add(name_lower)
            
            # Name type validation
            name_type = name_data.get('name_type')
            if not name_type:
                result.add_error(f'names[{i}].name_type', 'Name type is required', 'REQUIRED_FIELD')
            elif name_type not in [nt.value for nt in NameType]:
                result.add_error(f'names[{i}].name_type', f'Invalid name type: {name_type}', 'INVALID_VALUE', name_type)
            elif name_type == NameType.PRIMARY.value:
                primary_names.append(i)
        
        # Check for primary name requirements
        if len(primary_names) == 0:
            result.add_error('names', 'At least one primary name is required', 'MISSING_PRIMARY_NAME')
        elif len(primary_names) > 1:
            result.add_warning('names', f'Multiple primary names found (indices: {primary_names})', 'MULTIPLE_PRIMARY_NAMES')
    
    def _validate_addresses(self, addresses: List[Dict[str, Any]], result: ValidationResult):
        """Validate addresses."""
        for i, address_data in enumerate(addresses):
            if not isinstance(address_data, dict):
                result.add_error(f'addresses[{i}]', 'Address must be a dictionary', 'INVALID_TYPE')
                continue
            
            # Check if at least one address field is provided
            address_fields = ['street', 'city', 'postal_code', 'country', 'full_address']
            has_address_data = any(address_data.get(field) for field in address_fields)
            
            if not has_address_data:
                result.add_warning(f'addresses[{i}]', 'Address has no data in any field', 'EMPTY_ADDRESS')
                continue
            
            # Validate individual fields
            for field in ['street', 'full_address']:
                value = address_data.get(field)
                if value and len(value) > 1000:
                    result.add_error(f'addresses[{i}].{field}', f'{field.title()} cannot exceed 1000 characters', 'MAX_LENGTH')
            
            for field in ['city', 'country']:
                value = address_data.get(field)
                if value and len(value) > 100:
                    result.add_error(f'addresses[{i}].{field}', f'{field.title()} cannot exceed 100 characters', 'MAX_LENGTH')
            
            postal_code = address_data.get('postal_code')
            if postal_code and len(postal_code) > 20:
                result.add_error(f'addresses[{i}].postal_code', 'Postal code cannot exceed 20 characters', 'MAX_LENGTH')
            
            # Validate postal code format if country is known
            country = address_data.get('country')
            if postal_code and country and country.upper() in self.POSTAL_CODE_PATTERNS:
                pattern = self.POSTAL_CODE_PATTERNS[country.upper()]
                if not pattern.match(postal_code):
                    result.add_warning(f'addresses[{i}].postal_code', f'Postal code format may be invalid for {country}', 'INVALID_POSTAL_FORMAT')
    
    def _validate_identifiers(self, identifiers: List[Dict[str, Any]], result: ValidationResult):
        """Validate identifiers."""
        seen_identifiers = set()
        
        for i, identifier_data in enumerate(identifiers):
            if not isinstance(identifier_data, dict):
                result.add_error(f'identifiers[{i}]', 'Identifier must be a dictionary', 'INVALID_TYPE')
                continue
            
            # ID type validation
            id_type = identifier_data.get('id_type')
            if not id_type:
                result.add_error(f'identifiers[{i}].id_type', 'ID type is required', 'REQUIRED_FIELD')
            elif not isinstance(id_type, str):
                result.add_error(f'identifiers[{i}].id_type', 'ID type must be a string', 'INVALID_TYPE')
            elif len(id_type.strip()) == 0:
                result.add_error(f'identifiers[{i}].id_type', 'ID type cannot be empty', 'EMPTY_FIELD')
            elif len(id_type) > 100:
                result.add_error(f'identifiers[{i}].id_type', 'ID type cannot exceed 100 characters', 'MAX_LENGTH')
            
            # ID value validation
            id_value = identifier_data.get('id_value')
            if not id_value:
                result.add_error(f'identifiers[{i}].id_value', 'ID value is required', 'REQUIRED_FIELD')
            elif not isinstance(id_value, str):
                result.add_error(f'identifiers[{i}].id_value', 'ID value must be a string', 'INVALID_TYPE')
            elif len(id_value.strip()) == 0:
                result.add_error(f'identifiers[{i}].id_value', 'ID value cannot be empty', 'EMPTY_FIELD')
            elif len(id_value) > 200:
                result.add_error(f'identifiers[{i}].id_value', 'ID value cannot exceed 200 characters', 'MAX_LENGTH')
            else:
                # Check for duplicate identifiers
                if id_type and id_value:
                    identifier_key = f"{id_type}:{id_value}".lower()
                    if identifier_key in seen_identifiers:
                        result.add_warning(f'identifiers[{i}]', f'Duplicate identifier: {id_type} - {id_value}', 'DUPLICATE_IDENTIFIER')
                    else:
                        seen_identifiers.add(identifier_key)
            
            # Issuing country validation
            issuing_country = identifier_data.get('issuing_country')
            if issuing_country:
                if not isinstance(issuing_country, str):
                    result.add_error(f'identifiers[{i}].issuing_country', 'Issuing country must be a string', 'INVALID_TYPE')
                elif len(issuing_country) > 100:
                    result.add_error(f'identifiers[{i}].issuing_country', 'Issuing country cannot exceed 100 characters', 'MAX_LENGTH')
                elif len(issuing_country) == 2 and issuing_country.upper() not in self.VALID_COUNTRY_CODES:
                    result.add_warning(f'identifiers[{i}].issuing_country', f'Unknown country code: {issuing_country}', 'UNKNOWN_COUNTRY_CODE')
            
            # Notes validation
            notes = identifier_data.get('notes')
            if notes and len(notes) > 1000:
                result.add_error(f'identifiers[{i}].notes', 'Notes cannot exceed 1000 characters', 'MAX_LENGTH')
    
    def _validate_sanction_details(self, entity_data: Dict[str, Any], result: ValidationResult):
        """Validate sanction-specific details."""
        # Legal basis validation
        legal_basis = entity_data.get('legal_basis')
        if legal_basis and len(legal_basis) > 5000:
            result.add_error('legal_basis', 'Legal basis cannot exceed 5000 characters', 'MAX_LENGTH')
        
        # Measures imposed validation
        measures_imposed = entity_data.get('measures_imposed')
        if measures_imposed and len(measures_imposed) > 5000:
            result.add_error('measures_imposed', 'Measures imposed cannot exceed 5000 characters', 'MAX_LENGTH')
        
        # Reason for listing validation
        reason_for_listing = entity_data.get('reason_for_listing')
        if reason_for_listing and len(reason_for_listing) > 5000:
            result.add_error('reason_for_listing', 'Reason for listing cannot exceed 5000 characters', 'MAX_LENGTH')
    
    def _validate_metadata(self, entity_data: Dict[str, Any], result: ValidationResult):
        """Validate metadata fields."""
        # Internal notes validation
        internal_notes = entity_data.get('internal_notes')
        if internal_notes and len(internal_notes) > 5000:
            result.add_error('internal_notes', 'Internal notes cannot exceed 5000 characters', 'MAX_LENGTH')
        
        # Created by validation
        created_by = entity_data.get('created_by')
        if created_by:
            if not isinstance(created_by, str):
                result.add_error('created_by', 'Created by must be a string', 'INVALID_TYPE')
            elif len(created_by) > 100:
                result.add_error('created_by', 'Created by cannot exceed 100 characters', 'MAX_LENGTH')
        
        # Verified by validation
        verified_by = entity_data.get('verified_by')
        if verified_by:
            if not isinstance(verified_by, str):
                result.add_error('verified_by', 'Verified by must be a string', 'INVALID_TYPE')
            elif len(verified_by) > 100:
                result.add_error('verified_by', 'Verified by cannot exceed 100 characters', 'MAX_LENGTH')
        
        # Verified date validation
        verified_date = entity_data.get('verified_date')
        if verified_date:
            self._validate_date_field(verified_date, 'verified_date', result)
    
    def _validate_date_field(self, date_value: Any, field_name: str, result: ValidationResult):
        """Validate a date field."""
        if isinstance(date_value, (date, datetime)):
            return  # Already a valid date object
        
        if isinstance(date_value, str):
            try:
                # Try to parse ISO format
                datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            except ValueError:
                result.add_error(field_name, f'Invalid date format: {date_value}', 'INVALID_DATE_FORMAT', date_value)
        else:
            result.add_error(field_name, f'Date must be a string or date object, got {type(date_value).__name__}', 'INVALID_TYPE', date_value)  
  
    def detect_duplicates(self, entity_data: Dict[str, Any], existing_entities: List[Dict[str, Any]]) -> DuplicateDetectionResult:
        """
        Detect potential duplicates based on names, identifiers, and registration numbers.
        
        Args:
            entity_data: New entity data to check for duplicates
            existing_entities: List of existing entities to compare against
            
        Returns:
            DuplicateDetectionResult with potential matches
        """
        result = DuplicateDetectionResult()
        
        try:
            # Extract searchable data from new entity
            new_names = self._extract_names(entity_data)
            new_identifiers = self._extract_identifiers(entity_data)
            new_registration_numbers = self._extract_registration_numbers(entity_data)
            
            for existing_entity in existing_entities:
                # Skip if same entity (by ID)
                if entity_data.get('id') == existing_entity.get('id'):
                    continue
                
                # Check name matches
                existing_names = self._extract_names(existing_entity)
                name_matches = self._find_name_matches(new_names, existing_names)
                
                for match in name_matches:
                    duplicate_match = DuplicateMatch(
                        entity_id=existing_entity.get('id', ''),
                        internal_entry_id=existing_entity.get('internal_entry_id', ''),
                        match_type='name',
                        match_value=match['name'],
                        confidence=match['confidence'],
                        details={
                            'new_name': match['new_name'],
                            'existing_name': match['existing_name'],
                            'match_reason': match['reason']
                        }
                    )
                    result.add_match(duplicate_match)
                
                # Check identifier matches
                existing_identifiers = self._extract_identifiers(existing_entity)
                identifier_matches = self._find_identifier_matches(new_identifiers, existing_identifiers)
                
                for match in identifier_matches:
                    duplicate_match = DuplicateMatch(
                        entity_id=existing_entity.get('id', ''),
                        internal_entry_id=existing_entity.get('internal_entry_id', ''),
                        match_type='identifier',
                        match_value=f"{match['id_type']}:{match['id_value']}",
                        confidence=1.0,  # Exact identifier matches are high confidence
                        details={
                            'id_type': match['id_type'],
                            'id_value': match['id_value'],
                            'issuing_country': match.get('issuing_country')
                        }
                    )
                    result.add_match(duplicate_match)
                
                # Check registration number matches (for entities)
                existing_registration_numbers = self._extract_registration_numbers(existing_entity)
                registration_matches = self._find_registration_matches(new_registration_numbers, existing_registration_numbers)
                
                for match in registration_matches:
                    duplicate_match = DuplicateMatch(
                        entity_id=existing_entity.get('id', ''),
                        internal_entry_id=existing_entity.get('internal_entry_id', ''),
                        match_type='registration_number',
                        match_value=match['registration_number'],
                        confidence=0.9,  # High confidence for registration number matches
                        details={
                            'registration_number': match['registration_number'],
                            'registration_authority': match.get('registration_authority')
                        }
                    )
                    result.add_match(duplicate_match)
        
        except Exception as e:
            self.logger.error(f"Error during duplicate detection: {e}")
        
        return result
    
    def _extract_names(self, entity_data: Dict[str, Any]) -> List[str]:
        """Extract all names from entity data."""
        names = []
        
        # Get names from names array
        for name_data in entity_data.get('names', []):
            if isinstance(name_data, dict):
                full_name = name_data.get('full_name')
                if full_name and isinstance(full_name, str):
                    names.append(full_name.strip())
        
        return names
    
    def _extract_identifiers(self, entity_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract all identifiers from entity data."""
        identifiers = []
        
        for identifier_data in entity_data.get('identifiers', []):
            if isinstance(identifier_data, dict):
                id_type = identifier_data.get('id_type')
                id_value = identifier_data.get('id_value')
                issuing_country = identifier_data.get('issuing_country')
                
                if id_type and id_value:
                    identifiers.append({
                        'id_type': id_type.strip(),
                        'id_value': id_value.strip(),
                        'issuing_country': issuing_country.strip() if issuing_country else None
                    })
        
        return identifiers
    
    def _extract_registration_numbers(self, entity_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract registration numbers from entity data."""
        registration_numbers = []
        
        entity_details = entity_data.get('entity_details')
        if isinstance(entity_details, dict):
            registration_number = entity_details.get('registration_number')
            registration_authority = entity_details.get('registration_authority')
            
            if registration_number:
                registration_numbers.append({
                    'registration_number': registration_number.strip(),
                    'registration_authority': registration_authority.strip() if registration_authority else None
                })
        
        return registration_numbers
    
    def _find_name_matches(self, new_names: List[str], existing_names: List[str]) -> List[Dict[str, Any]]:
        """Find name matches between new and existing names."""
        matches = []
        
        for new_name in new_names:
            for existing_name in existing_names:
                confidence, reason = self._calculate_name_similarity(new_name, existing_name)
                
                if confidence >= 0.8:  # High similarity threshold
                    matches.append({
                        'new_name': new_name,
                        'existing_name': existing_name,
                        'name': new_name,  # For match_value
                        'confidence': confidence,
                        'reason': reason
                    })
        
        return matches
    
    def _find_identifier_matches(self, new_identifiers: List[Dict[str, str]], 
                               existing_identifiers: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Find exact identifier matches."""
        matches = []
        
        for new_id in new_identifiers:
            for existing_id in existing_identifiers:
                # Exact match on type and value
                if (new_id['id_type'].lower() == existing_id['id_type'].lower() and
                    new_id['id_value'].lower() == existing_id['id_value'].lower()):
                    matches.append(new_id)
                    break
        
        return matches
    
    def _find_registration_matches(self, new_registrations: List[Dict[str, str]], 
                                 existing_registrations: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Find registration number matches."""
        matches = []
        
        for new_reg in new_registrations:
            for existing_reg in existing_registrations:
                # Exact match on registration number
                if new_reg['registration_number'].lower() == existing_reg['registration_number'].lower():
                    matches.append(new_reg)
                    break
        
        return matches
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> Tuple[float, str]:
        """
        Calculate similarity between two names.
        
        Returns:
            Tuple of (confidence_score, reason)
        """
        name1_clean = self._normalize_name(name1)
        name2_clean = self._normalize_name(name2)
        
        # Exact match
        if name1_clean == name2_clean:
            return 1.0, "exact_match"
        
        # Check if one name contains the other
        if name1_clean in name2_clean or name2_clean in name1_clean:
            return 0.9, "substring_match"
        
        # Calculate Levenshtein distance-based similarity
        similarity = self._levenshtein_similarity(name1_clean, name2_clean)
        
        if similarity >= 0.9:
            return similarity, "high_similarity"
        elif similarity >= 0.8:
            return similarity, "moderate_similarity"
        else:
            return similarity, "low_similarity"
    
    def _normalize_name(self, name: str) -> str:
        """Normalize name for comparison."""
        if not name:
            return ""
        
        # Convert to lowercase and remove extra whitespace
        normalized = re.sub(r'\s+', ' ', name.lower().strip())
        
        # Remove common punctuation and replace hyphens with spaces
        normalized = re.sub(r'[.,;:!?()"\']', '', normalized)
        normalized = re.sub(r'[-]', ' ', normalized)  # Replace hyphens with spaces
        
        # Remove extra whitespace again after hyphen replacement
        normalized = re.sub(r'\s+', ' ', normalized.strip())
        
        # Remove common prefixes/suffixes
        prefixes = ['mr', 'mrs', 'ms', 'dr', 'prof', 'sir', 'lady']
        suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'inc', 'ltd', 'llc', 'corp']
        
        words = normalized.split()
        filtered_words = []
        
        for word in words:
            if word not in prefixes and word not in suffixes:
                filtered_words.append(word)
        
        return ' '.join(filtered_words)
    
    def _levenshtein_similarity(self, s1: str, s2: str) -> float:
        """Calculate Levenshtein similarity between two strings."""
        if not s1 and not s2:
            return 1.0
        if not s1 or not s2:
            return 0.0
        
        # Calculate Levenshtein distance
        distance = self._levenshtein_distance(s1, s2)
        max_len = max(len(s1), len(s2))
        
        # Convert to similarity score (0.0 to 1.0)
        similarity = 1.0 - (distance / max_len)
        return max(0.0, similarity)
    
    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings."""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def check_data_completeness(self, entity_data: Dict[str, Any]) -> ValidationResult:
        """
        Check data completeness and quality.
        
        Args:
            entity_data: Entity data to check
            
        Returns:
            ValidationResult with completeness assessment
        """
        result = ValidationResult()
        
        # Core completeness checks
        core_fields = ['internal_entry_id', 'subject_type', 'sanctioning_authority', 'program', 'listing_date', 'data_source']
        missing_core = []
        
        for field in core_fields:
            if not entity_data.get(field):
                missing_core.append(field)
        
        if missing_core:
            result.add_warning('completeness', f'Missing core fields: {", ".join(missing_core)}', 'INCOMPLETE_CORE_DATA')
        
        # Names completeness
        names = entity_data.get('names', [])
        if not names:
            result.add_error('completeness', 'No names provided', 'MISSING_NAMES')
        else:
            primary_names = [n for n in names if n.get('name_type') == NameType.PRIMARY.value]
            if not primary_names:
                result.add_error('completeness', 'No primary name provided', 'MISSING_PRIMARY_NAME')
        
        # Subject type specific completeness
        subject_type = entity_data.get('subject_type')
        
        if subject_type == SubjectType.INDIVIDUAL.value:
            individual_details = entity_data.get('individual_details', {})
            
            # Check for birth information
            has_birth_info = any([
                individual_details.get('birth_year'),
                individual_details.get('birth_full_date'),
                individual_details.get('place_of_birth')
            ])
            
            if not has_birth_info:
                result.add_info('completeness', 'No birth information provided for individual', 'MISSING_BIRTH_INFO')
            
            # Check for nationality
            nationalities = individual_details.get('nationalities', [])
            if not nationalities:
                result.add_info('completeness', 'No nationality information provided', 'MISSING_NATIONALITY')
        
        elif subject_type == SubjectType.ENTITY.value:
            entity_details = entity_data.get('entity_details', {})
            
            # Check for registration information
            has_registration_info = any([
                entity_details.get('registration_number'),
                entity_details.get('incorporation_date'),
                entity_details.get('company_type')
            ])
            
            if not has_registration_info:
                result.add_info('completeness', 'No registration information provided for entity', 'MISSING_REGISTRATION_INFO')
        
        # Address completeness
        addresses = entity_data.get('addresses', [])
        if not addresses:
            result.add_info('completeness', 'No address information provided', 'MISSING_ADDRESS')
        
        # Identifier completeness
        identifiers = entity_data.get('identifiers', [])
        if not identifiers:
            result.add_info('completeness', 'No identification documents provided', 'MISSING_IDENTIFIERS')
        
        # Sanction details completeness
        sanction_fields = ['legal_basis', 'measures_imposed', 'reason_for_listing']
        missing_sanction = []
        
        for field in sanction_fields:
            if not entity_data.get(field):
                missing_sanction.append(field)
        
        if missing_sanction:
            result.add_info('completeness', f'Missing sanction details: {", ".join(missing_sanction)}', 'INCOMPLETE_SANCTION_DETAILS')
        
        return result
    
    def validate_data_quality(self, entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Perform comprehensive data quality assessment on a collection of entities.
        
        Args:
            entities: List of entity data dictionaries
            
        Returns:
            Data quality report
        """
        report = {
            'total_entities': len(entities),
            'valid_entities': 0,
            'invalid_entities': 0,
            'entities_with_warnings': 0,
            'completeness_score': 0.0,
            'duplicate_groups': [],
            'quality_issues': {
                'errors': [],
                'warnings': [],
                'info': []
            },
            'statistics': {
                'subject_types': {},
                'record_statuses': {},
                'entities_with_names': 0,
                'entities_with_addresses': 0,
                'entities_with_identifiers': 0,
                'average_names_per_entity': 0.0,
                'average_addresses_per_entity': 0.0,
                'average_identifiers_per_entity': 0.0
            }
        }
        
        if not entities:
            return report
        
        total_completeness = 0.0
        total_names = 0
        total_addresses = 0
        total_identifiers = 0
        
        # Validate each entity
        for i, entity_data in enumerate(entities):
            validation_result = self.validate_entity_data(entity_data)
            completeness_result = self.check_data_completeness(entity_data)
            
            # Count valid/invalid entities
            if validation_result.is_valid:
                report['valid_entities'] += 1
            else:
                report['invalid_entities'] += 1
            
            if validation_result.warnings_count > 0 or completeness_result.warnings_count > 0:
                report['entities_with_warnings'] += 1
            
            # Collect issues
            for issue in validation_result.issues + completeness_result.issues:
                issue_dict = issue.to_dict()
                issue_dict['entity_index'] = i
                issue_dict['entity_id'] = entity_data.get('id', f'entity_{i}')
                
                report['quality_issues'][issue.severity.value].append(issue_dict)
            
            # Calculate completeness score for this entity
            entity_completeness = self._calculate_entity_completeness(entity_data)
            total_completeness += entity_completeness
            
            # Collect statistics
            subject_type = entity_data.get('subject_type', 'Unknown')
            report['statistics']['subject_types'][subject_type] = report['statistics']['subject_types'].get(subject_type, 0) + 1
            
            record_status = entity_data.get('record_status', 'Unknown')
            report['statistics']['record_statuses'][record_status] = report['statistics']['record_statuses'].get(record_status, 0) + 1
            
            names = entity_data.get('names', [])
            addresses = entity_data.get('addresses', [])
            identifiers = entity_data.get('identifiers', [])
            
            if names:
                report['statistics']['entities_with_names'] += 1
                total_names += len(names)
            
            if addresses:
                report['statistics']['entities_with_addresses'] += 1
                total_addresses += len(addresses)
            
            if identifiers:
                report['statistics']['entities_with_identifiers'] += 1
                total_identifiers += len(identifiers)
        
        # Calculate averages
        if report['total_entities'] > 0:
            report['completeness_score'] = total_completeness / report['total_entities']
            report['statistics']['average_names_per_entity'] = total_names / report['total_entities']
            report['statistics']['average_addresses_per_entity'] = total_addresses / report['total_entities']
            report['statistics']['average_identifiers_per_entity'] = total_identifiers / report['total_entities']
        
        # Detect duplicate groups
        report['duplicate_groups'] = self._find_duplicate_groups(entities)
        
        return report
    
    def _calculate_entity_completeness(self, entity_data: Dict[str, Any]) -> float:
        """Calculate completeness score for a single entity (0.0 to 1.0)."""
        total_fields = 0
        completed_fields = 0
        
        # Core fields (weight: 2)
        core_fields = ['internal_entry_id', 'subject_type', 'sanctioning_authority', 'program', 'listing_date', 'data_source']
        for field in core_fields:
            total_fields += 2
            if entity_data.get(field):
                completed_fields += 2
        
        # Names (weight: 2)
        total_fields += 2
        names = entity_data.get('names', [])
        if names and any(n.get('name_type') == NameType.PRIMARY.value for n in names):
            completed_fields += 2
        
        # Optional fields (weight: 1)
        optional_fields = ['legal_basis', 'measures_imposed', 'reason_for_listing']
        for field in optional_fields:
            total_fields += 1
            if entity_data.get(field):
                completed_fields += 1
        
        # Addresses (weight: 1)
        total_fields += 1
        if entity_data.get('addresses'):
            completed_fields += 1
        
        # Identifiers (weight: 1)
        total_fields += 1
        if entity_data.get('identifiers'):
            completed_fields += 1
        
        # Subject type specific fields
        subject_type = entity_data.get('subject_type')
        if subject_type == SubjectType.INDIVIDUAL.value:
            individual_details = entity_data.get('individual_details', {})
            total_fields += 2
            if individual_details.get('birth_year') or individual_details.get('birth_full_date'):
                completed_fields += 1
            if individual_details.get('nationalities'):
                completed_fields += 1
        elif subject_type == SubjectType.ENTITY.value:
            entity_details = entity_data.get('entity_details', {})
            total_fields += 2
            if entity_details.get('registration_number'):
                completed_fields += 1
            if entity_details.get('company_type'):
                completed_fields += 1
        
        return completed_fields / total_fields if total_fields > 0 else 0.0
    
    def _find_duplicate_groups(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find groups of potential duplicate entities."""
        duplicate_groups = []
        processed_entities = set()
        
        for i, entity in enumerate(entities):
            if i in processed_entities:
                continue
            
            # Find all duplicates for this entity
            duplicates = []
            entity_names = self._extract_names(entity)
            entity_identifiers = self._extract_identifiers(entity)
            
            for j, other_entity in enumerate(entities[i+1:], i+1):
                if j in processed_entities:
                    continue
                
                # Check for matches
                other_names = self._extract_names(other_entity)
                other_identifiers = self._extract_identifiers(other_entity)
                
                has_name_match = any(
                    self._calculate_name_similarity(name1, name2)[0] >= 0.8
                    for name1 in entity_names
                    for name2 in other_names
                )
                
                has_identifier_match = any(
                    id1['id_type'].lower() == id2['id_type'].lower() and
                    id1['id_value'].lower() == id2['id_value'].lower()
                    for id1 in entity_identifiers
                    for id2 in other_identifiers
                )
                
                if has_name_match or has_identifier_match:
                    duplicates.append({
                        'entity_index': j,
                        'entity_id': other_entity.get('id', f'entity_{j}'),
                        'internal_entry_id': other_entity.get('internal_entry_id', ''),
                        'match_reasons': []
                    })
                    processed_entities.add(j)
            
            if duplicates:
                duplicate_group = {
                    'primary_entity': {
                        'entity_index': i,
                        'entity_id': entity.get('id', f'entity_{i}'),
                        'internal_entry_id': entity.get('internal_entry_id', '')
                    },
                    'duplicates': duplicates,
                    'group_size': len(duplicates) + 1
                }
                duplicate_groups.append(duplicate_group)
                processed_entities.add(i)
        
        return duplicate_groups