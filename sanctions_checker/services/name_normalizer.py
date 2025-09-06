"""
Name normalization service for consistent entity name matching.

This module provides comprehensive name normalization capabilities including:
- Company suffix standardization
- Punctuation and spacing normalization
- Character encoding standardization
- Name preprocessing pipeline
"""

import re
import unicodedata
from typing import List, Dict, Set


class NameNormalizer:
    """
    Handles name preprocessing and normalization for consistent matching.
    
    Provides methods to normalize company names, individual names, and general
    text preprocessing to improve fuzzy matching accuracy.
    """
    
    def __init__(self):
        """Initialize the name normalizer with predefined mappings and patterns."""
        self._company_suffixes = self._build_company_suffix_mapping()
        self._common_abbreviations = self._build_abbreviation_mapping()
        self._punctuation_pattern = re.compile(r'[^\w\s]')
        self._whitespace_pattern = re.compile(r'\s+')
        
    def _build_company_suffix_mapping(self) -> Dict[str, str]:
        """
        Build a comprehensive mapping of company suffixes to their standardized forms.
        
        Returns:
            Dict mapping various suffix forms to standardized versions
        """
        return {
            # English suffixes
            'inc': 'incorporated',
            'inc.': 'incorporated',
            'incorporated': 'incorporated',
            'corp': 'corporation',
            'corp.': 'corporation',
            'corporation': 'corporation',
            'ltd': 'limited',
            'ltd.': 'limited',
            'limited': 'limited',
            'llc': 'limited liability company',
            'l.l.c.': 'limited liability company',
            'limited liability company': 'limited liability company',
            'lp': 'limited partnership',
            'l.p.': 'limited partnership',
            'limited partnership': 'limited partnership',
            'llp': 'limited liability partnership',
            'l.l.p.': 'limited liability partnership',
            'limited liability partnership': 'limited liability partnership',
            'co': 'company',
            'co.': 'company',
            'company': 'company',
            'plc': 'public limited company',
            'p.l.c.': 'public limited company',
            'public limited company': 'public limited company',
            
            # German suffixes
            'gmbh': 'gesellschaft mit beschränkter haftung',
            'g.m.b.h.': 'gesellschaft mit beschränkter haftung',
            'gesellschaft mit beschränkter haftung': 'gesellschaft mit beschränkter haftung',
            'ag': 'aktiengesellschaft',
            'a.g.': 'aktiengesellschaft',
            'aktiengesellschaft': 'aktiengesellschaft',
            'kg': 'kommanditgesellschaft',
            'k.g.': 'kommanditgesellschaft',
            'kommanditgesellschaft': 'kommanditgesellschaft',
            'ohg': 'offene handelsgesellschaft',
            'o.h.g.': 'offene handelsgesellschaft',
            'offene handelsgesellschaft': 'offene handelsgesellschaft',
            
            # French suffixes
            'sa': 'société anonyme',
            's.a.': 'société anonyme',
            'société anonyme': 'société anonyme',
            'sarl': 'société à responsabilité limitée',
            's.a.r.l.': 'société à responsabilité limitée',
            'société à responsabilité limitée': 'société à responsabilité limitée',
            'sas': 'société par actions simplifiée',
            's.a.s.': 'société par actions simplifiée',
            'société par actions simplifiée': 'société par actions simplifiée',
            
            # Swedish suffixes
            'ab': 'aktiebolag',
            'a.b.': 'aktiebolag',
            'aktiebolag': 'aktiebolag',
            'hb': 'handelsbolag',
            'h.b.': 'handelsbolag',
            'handelsbolag': 'handelsbolag',
            'kb': 'kommanditbolag',
            'k.b.': 'kommanditbolag',
            'kommanditbolag': 'kommanditbolag',
            
            # Italian suffixes
            'spa': 'società per azioni',
            's.p.a.': 'società per azioni',
            'società per azioni': 'società per azioni',
            'srl': 'società a responsabilità limitata',
            's.r.l.': 'società a responsabilità limitata',
            'società a responsabilità limitata': 'società a responsabilità limitata',
            
            # Spanish suffixes
            'sl': 'sociedad limitada',
            's.l.': 'sociedad limitada',
            'sociedad limitada': 'sociedad limitada',
            
            # Dutch suffixes
            'bv': 'besloten vennootschap',
            'b.v.': 'besloten vennootschap',
            'besloten vennootschap': 'besloten vennootschap',
            'nv': 'naamloze vennootschap',
            'n.v.': 'naamloze vennootschap',
            'naamloze vennootschap': 'naamloze vennootschap',
            
            # Asian suffixes
            
            # Japanese suffixes
            'kk': 'kabushiki kaisha',
            'k.k.': 'kabushiki kaisha',
            'kabushiki kaisha': 'kabushiki kaisha',
            'gk': 'godo kaisha',
            'g.k.': 'godo kaisha',
            'godo kaisha': 'godo kaisha',
            'yk': 'yugen kaisha',
            'y.k.': 'yugen kaisha',
            'yugen kaisha': 'yugen kaisha',
            
            # Korean suffixes
            'chusik hoesa': 'chusik hoesa',  # 주식회사 (corporation)
            'yuhan hoesa': 'yuhan hoesa',    # 유한회사 (limited company)
            'hapja hoesa': 'hapja hoesa',    # 합자회사 (limited partnership)
            'hapmeong hoesa': 'hapmeong hoesa',  # 합명회사 (general partnership)
            
            # Chinese suffixes (Mainland China)
            'youxian gongsi': 'youxian gongsi',  # 有限公司 (limited company)
            'gufen youxian gongsi': 'gufen youxian gongsi',  # 股份有限公司 (joint stock company)
            'youxian zeren gongsi': 'youxian zeren gongsi',  # 有限责任公司 (limited liability company)
            
            # Hong Kong suffixes
            'hk ltd': 'hong kong limited',
            'hk limited': 'hong kong limited',
            
            # Singapore suffixes
            'pte': 'private',
            'pte.': 'private',
            'pte ltd': 'private limited',
            'pte. ltd.': 'private limited',
            'private limited': 'private limited',
            
            # Indian suffixes
            'pvt ltd': 'private limited',
            'pvt. ltd.': 'private limited',
            'public ltd': 'public limited',
            'public ltd.': 'public limited',
            'opc': 'one person company',
            'o.p.c.': 'one person company',
            'one person company': 'one person company',
            'section 8 company': 'section 8 company',
            'producer company': 'producer company',
            
            # Malaysian suffixes
            'sdn': 'sendirian',
            'sdn.': 'sendirian',
            'sendirian': 'sendirian',
            'sdn bhd': 'sendirian berhad',
            'sdn. bhd.': 'sendirian berhad',
            'sendirian berhad': 'sendirian berhad',
            'bhd': 'berhad',
            'bhd.': 'berhad',
            'berhad': 'berhad',
            
            # Indonesian suffixes
            'pt': 'perseroan terbatas',
            'p.t.': 'perseroan terbatas',
            'perseroan terbatas': 'perseroan terbatas',
            'cv': 'commanditaire vennootschap',
            'c.v.': 'commanditaire vennootschap',
            'commanditaire vennootschap': 'commanditaire vennootschap',
            'firma': 'firma',
            'ud': 'usaha dagang',
            'u.d.': 'usaha dagang',
            'usaha dagang': 'usaha dagang',
            
            # Thai suffixes
            'co ltd': 'company limited',
            'co. ltd.': 'company limited',
            'company limited': 'company limited',
            'public co ltd': 'public company limited',
            'public co. ltd.': 'public company limited',
            'public company limited': 'public company limited',
            
            # Vietnamese suffixes
            'tnhh': 'trach nhiem huu han',  # Limited liability company
            't.n.h.h.': 'trach nhiem huu han',
            'trach nhiem huu han': 'trach nhiem huu han',
            'cp': 'cong phan',  # Joint stock company
            'c.p.': 'cong phan',
            'cong phan': 'cong phan',
            
            # Philippines suffixes
            'corp': 'corporation',
            'corporation': 'corporation',
            'inc': 'incorporated',
            'incorporated': 'incorporated',
            'partnership': 'partnership',
            'cooperative': 'cooperative',
            'coop': 'cooperative',
            'coop.': 'cooperative',
            
            # Latin American suffixes
            
            # Brazilian suffixes
            'ltda': 'limitada',
            'ltda.': 'limitada',
            'limitada': 'limitada',
            # Note: Brazilian SA conflicts with French SA, so we'll handle this contextually
            'sociedade anonima': 'sociedade anonima',
            'eireli': 'empresa individual de responsabilidade limitada',
            'e.i.r.e.l.i.': 'empresa individual de responsabilidade limitada',
            'empresa individual de responsabilidade limitada': 'empresa individual de responsabilidade limitada',
            'me': 'microempresa',
            'm.e.': 'microempresa',
            'microempresa': 'microempresa',
            'epp': 'empresa de pequeno porte',
            'e.p.p.': 'empresa de pequeno porte',
            'empresa de pequeno porte': 'empresa de pequeno porte',
            
            # Mexican suffixes
            'sa de cv': 'sociedad anonima de capital variable',
            's.a. de c.v.': 'sociedad anonima de capital variable',
            'sociedad anonima de capital variable': 'sociedad anonima de capital variable',
            'srl de cv': 'sociedad de responsabilidad limitada de capital variable',
            's.r.l. de c.v.': 'sociedad de responsabilidad limitada de capital variable',
            'sociedad de responsabilidad limitada de capital variable': 'sociedad de responsabilidad limitada de capital variable',
            'sc': 'sociedad civil',
            's.c.': 'sociedad civil',
            'sociedad civil': 'sociedad civil',
            'scp': 'sociedad civil particular',
            's.c.p.': 'sociedad civil particular',
            'sociedad civil particular': 'sociedad civil particular',
            
            # Argentine suffixes
            'srl': 'sociedad de responsabilidad limitada',
            's.r.l.': 'sociedad de responsabilidad limitada',
            'sociedad de responsabilidad limitada': 'sociedad de responsabilidad limitada',
            'sas': 'sociedad por acciones simplificada',
            's.a.s.': 'sociedad por acciones simplificada',
            'sociedad por acciones simplificada': 'sociedad por acciones simplificada',
            'se': 'sociedad del estado',
            's.e.': 'sociedad del estado',
            'sociedad del estado': 'sociedad del estado',
            
            # Chilean suffixes
            'spa': 'sociedad por acciones',
            's.p.a.': 'sociedad por acciones',
            'sociedad por acciones': 'sociedad por acciones',
            'eirl': 'empresa individual de responsabilidad limitada',
            'e.i.r.l.': 'empresa individual de responsabilidad limitada',
            'empresa individual de responsabilidad limitada': 'empresa individual de responsabilidad limitada',
            
            # Colombian suffixes
            'sas': 'sociedad por acciones simplificada',
            's.a.s.': 'sociedad por acciones simplificada',
            'sociedad por acciones simplificada': 'sociedad por acciones simplificada',
            'eu': 'empresa unipersonal',
            'e.u.': 'empresa unipersonal',
            'empresa unipersonal': 'empresa unipersonal',
            
            # African suffixes
            
            # South African suffixes
            'pty': 'proprietary',
            'pty.': 'proprietary',
            'proprietary': 'proprietary',
            'pty ltd': 'proprietary limited',
            'pty. ltd.': 'proprietary limited',
            'proprietary limited': 'proprietary limited',
            'cc': 'close corporation',
            'c.c.': 'close corporation',
            'close corporation': 'close corporation',
            'inc': 'incorporated',
            'inc.': 'incorporated',
            'incorporated': 'incorporated',
            'npc': 'non profit company',
            'n.p.c.': 'non profit company',
            'non profit company': 'non profit company',
            'soc': 'state owned company',
            's.o.c.': 'state owned company',
            'state owned company': 'state owned company',
            
            # Nigerian suffixes
            'ltd': 'limited',
            'ltd.': 'limited',
            'limited': 'limited',
            'plc': 'public limited company',
            'p.l.c.': 'public limited company',
            'public limited company': 'public limited company',
            'gte': 'guarantee',
            'g.t.e.': 'guarantee',
            'guarantee': 'guarantee',
            'unlimited': 'unlimited',
            
            # Kenyan suffixes
            'ltd': 'limited',
            'ltd.': 'limited',
            'limited': 'limited',
            'company limited by guarantee': 'company limited by guarantee',
            'company limited by shares': 'company limited by shares',
            
            # Egyptian suffixes
            'sae': 'societe anonyme egyptienne',
            's.a.e.': 'societe anonyme egyptienne',
            'societe anonyme egyptienne': 'societe anonyme egyptienne',
            'llc': 'limited liability company',
            'l.l.c.': 'limited liability company',
            'limited liability company': 'limited liability company',
            
            # Moroccan suffixes
            'sarl': 'société à responsabilité limitée',
            's.a.r.l.': 'société à responsabilité limitée',
            'société à responsabilité limitée': 'société à responsabilité limitée',
            'sa': 'société anonyme',
            's.a.': 'société anonyme',
            'société anonyme': 'société anonyme',
            'sas': 'société par actions simplifiée',
            's.a.s.': 'société par actions simplifiée',
            'société par actions simplifiée': 'société par actions simplifiée',
            
            # Other common international suffixes
            'pty': 'proprietary',
            'pty.': 'proprietary',
            'proprietary': 'proprietary',
            'pvt': 'private',
            'pvt.': 'private',
            'private': 'private',
        }
    
    def _build_abbreviation_mapping(self) -> Dict[str, str]:
        """
        Build a mapping of common business abbreviations to their full forms.
        
        Returns:
            Dict mapping abbreviations to full forms
        """
        return {
            # Common business terms
            'intl': 'international',
            'int\'l': 'international',
            'international': 'international',
            'natl': 'national',
            'nat\'l': 'national',
            'national': 'national',
            'assn': 'association',
            'assoc': 'association',
            'association': 'association',
            'mfg': 'manufacturing',
            'manufacturing': 'manufacturing',
            'tech': 'technology',
            'technology': 'technology',
            'sys': 'systems',
            'systems': 'systems',
            'svc': 'service',
            'svcs': 'services',
            'service': 'service',
            'services': 'services',
            'grp': 'group',
            'group': 'group',
            'hldg': 'holding',
            'holdings': 'holdings',
            'holding': 'holding',
            'inv': 'investment',
            'investments': 'investments',
            'investment': 'investment',
            'mgmt': 'management',
            'management': 'management',
            'dev': 'development',
            'development': 'development',
            'cons': 'consulting',
            'consulting': 'consulting',
            'sol': 'solutions',
            'solutions': 'solutions',
            'ent': 'enterprises',
            'enterprises': 'enterprises',
            'enterprise': 'enterprise',
            'co': 'company',
            'co.': 'company',
            
            # Asian business terms
            'mfg': 'manufacturing',
            'manufacturing': 'manufacturing',
            'trading': 'trading',
            'import': 'import',
            'export': 'export',
            'imp': 'import',
            'exp': 'export',
            'imp exp': 'import export',
            'import export': 'import export',
            'industrial': 'industrial',
            'ind': 'industrial',
            'ind.': 'industrial',
            'electronics': 'electronics',
            'elec': 'electronics',
            'elec.': 'electronics',
            'textiles': 'textiles',
            'tex': 'textiles',
            'tex.': 'textiles',
            'pharmaceutical': 'pharmaceutical',
            'pharma': 'pharmaceutical',
            'chemicals': 'chemicals',
            'chem': 'chemicals',
            'chem.': 'chemicals',
            'construction': 'construction',
            'const': 'construction',
            'const.': 'construction',
            'engineering': 'engineering',
            'eng': 'engineering',
            'eng.': 'engineering',
            'machinery': 'machinery',
            'mach': 'machinery',
            'mach.': 'machinery',
            'automotive': 'automotive',
            'auto': 'automotive',
            'auto.': 'automotive',
            'telecommunications': 'telecommunications',
            'telecom': 'telecommunications',
            'tel': 'telecommunications',
            'tel.': 'telecommunications',
            'finance': 'finance',
            'fin': 'financial',
            'fin.': 'financial',
            'financial': 'financial',
            'banking': 'banking',
            'insurance': 'insurance',
            'ins': 'insurance',
            'ins.': 'insurance',
            'real estate': 'real estate',
            'realty': 'real estate',
            'properties': 'properties',
            'prop': 'properties',
            'prop.': 'properties',
            'logistics': 'logistics',
            'log': 'logistics',
            'log.': 'logistics',
            'shipping': 'shipping',
            'ship': 'shipping',
            'ship.': 'shipping',
            'transport': 'transport',
            'trans': 'transport',
            'trans.': 'transport',
            'transportation': 'transportation',
            'energy': 'energy',
            'power': 'power',
            'oil': 'oil',
            'gas': 'gas',
            'petroleum': 'petroleum',
            'petro': 'petroleum',
            'petro.': 'petroleum',
            'mining': 'mining',
            'resources': 'resources',
            'res': 'resources',
            'res.': 'resources',
            'agriculture': 'agriculture',
            'agri': 'agriculture',
            'agri.': 'agriculture',
            'agro': 'agriculture',
            'agro.': 'agriculture',
            'food': 'food',
            'beverage': 'beverage',
            'bev': 'beverage',
            'bev.': 'beverage',
            'hospitality': 'hospitality',
            'hotel': 'hotel',
            'restaurant': 'restaurant',
            'retail': 'retail',
            'wholesale': 'wholesale',
            'distribution': 'distribution',
            'dist': 'distribution',
            'dist.': 'distribution',
            'medical': 'medical',
            'med': 'medical',
            'med.': 'medical',
            'healthcare': 'healthcare',
            'health': 'healthcare',
            'education': 'education',
            'edu': 'education',
            'edu.': 'education',
            'research': 'research',
            'res': 'research',
            'development': 'development',
            'dev': 'development',
            'dev.': 'development',
            'r and d': 'research and development',
            'div': 'division',
            'div.': 'division',
            'division': 'division',
            'information technology': 'information technology',
            'it': 'information technology',
            'i.t.': 'information technology',
            'software': 'software',
            'sw': 'software',
            's.w.': 'software',
            'hardware': 'hardware',
            'hw': 'hardware',
            'h.w.': 'hardware',
        }    

    def normalize_company_name(self, name: str) -> str:
        """
        Normalize a company name for consistent matching.
        
        Args:
            name: The company name to normalize
            
        Returns:
            Normalized company name
        """
        if not name or not isinstance(name, str):
            return ""
        
        # Apply the full preprocessing pipeline
        normalized = self.preprocess_name(name)
        
        # Apply company-specific normalizations
        # Expand abbreviations first, then handle suffixes
        normalized = self._expand_abbreviations(normalized)
        normalized = self._normalize_company_suffixes(normalized)
        
        return normalized.strip()
    
    def normalize_individual_name(self, name: str) -> str:
        """
        Normalize an individual's name for consistent matching.
        
        Args:
            name: The individual's name to normalize
            
        Returns:
            Normalized individual name
        """
        if not name or not isinstance(name, str):
            return ""
        
        # Handle name order BEFORE preprocessing (while comma is still there)
        normalized = self._normalize_name_order(name)
        
        # Apply basic preprocessing
        normalized = self.preprocess_name(normalized)
        
        # Individual-specific normalizations
        normalized = self._handle_titles_and_suffixes(normalized)
        
        return normalized.strip()
    
    def preprocess_name(self, name: str) -> str:
        """
        Apply general preprocessing to any name string.
        
        This is the core preprocessing pipeline that handles:
        - Unicode normalization
        - Case normalization
        - Punctuation standardization
        - Whitespace normalization
        
        Args:
            name: The name string to preprocess
            
        Returns:
            Preprocessed name string
        """
        if not name or not isinstance(name, str):
            return ""
        
        # Unicode normalization (NFD - decomposed form)
        normalized = unicodedata.normalize('NFD', name)
        
        # Remove diacritical marks but keep base characters
        normalized = ''.join(
            char for char in normalized 
            if unicodedata.category(char) != 'Mn'
        )
        
        # Convert to lowercase for consistent processing
        normalized = normalized.lower()
        
        # Standardize punctuation and spacing
        normalized = self._standardize_punctuation(normalized)
        normalized = self._standardize_whitespace(normalized)
        
        return normalized
    
    def _normalize_company_suffixes(self, name: str) -> str:
        """
        Normalize company legal suffixes to standardized forms.
        
        Args:
            name: Company name with potential suffixes
            
        Returns:
            Name with normalized suffixes
        """
        words = name.split()
        if not words:
            return name
        
        # First, remove periods from potential suffixes and check
        cleaned_words = []
        for word in words:
            cleaned_words.append(word.rstrip('.'))
        
        # Check if the last word(s) match any known suffix
        for i in range(len(cleaned_words)):
            suffix_candidate = ' '.join(cleaned_words[i:])
            if suffix_candidate in self._company_suffixes:
                # Replace with standardized form
                standardized_suffix = self._company_suffixes[suffix_candidate]
                return ' '.join(cleaned_words[:i] + [standardized_suffix])
        
        # If no match found, return the cleaned version
        return ' '.join(cleaned_words)
    
    def _expand_abbreviations(self, name: str) -> str:
        """
        Expand common business abbreviations to their full forms.
        
        Args:
            name: Name with potential abbreviations
            
        Returns:
            Name with expanded abbreviations
        """
        words = name.split()
        expanded_words = []
        
        for word in words:
            # Remove trailing punctuation for matching
            clean_word = word.rstrip('.,;:')
            if clean_word in self._common_abbreviations:
                expanded_words.append(self._common_abbreviations[clean_word])
            else:
                expanded_words.append(clean_word)  # Use cleaned word
        
        return ' '.join(expanded_words)
    
    def _standardize_punctuation(self, text: str) -> str:
        """
        Standardize punctuation in text.
        
        Args:
            text: Text with various punctuation
            
        Returns:
            Text with standardized punctuation
        """
        # Handle apostrophes specially - remove them without adding spaces
        text = text.replace("'", "")
        text = text.replace("'", "")  # Smart apostrophe
        
        # Handle dotted abbreviations specially (like S.A., A.G., G.m.b.H., S.A.R.L.)
        # Replace periods between letters with nothing to keep them together
        # This handles both single letters and longer sequences
        text = re.sub(r'([A-Za-z])\.([A-Za-z])', r'\1\2', text)
        # Apply multiple times to handle cases like S.A.R.L.
        text = re.sub(r'([A-Za-z])\.([A-Za-z])', r'\1\2', text)
        text = re.sub(r'([A-Za-z])\.([A-Za-z])', r'\1\2', text)
        
        # Replace common punctuation variations with spaces around them
        text = text.replace('&', ' and ')
        text = text.replace('+', ' and ')
        text = text.replace('/', ' ')
        text = text.replace('-', ' ')
        text = text.replace('_', ' ')
        
        # Handle remaining periods by replacing with single space
        text = re.sub(r'\.+', ' ', text)
        
        # Remove most other punctuation
        text = re.sub(r'[^\w\s]', ' ', text)
        
        return text
    
    def _standardize_whitespace(self, text: str) -> str:
        """
        Standardize whitespace in text.
        
        Args:
            text: Text with irregular whitespace
            
        Returns:
            Text with normalized whitespace
        """
        # Replace multiple whitespace characters with single space
        text = self._whitespace_pattern.sub(' ', text)
        
        # Strip leading and trailing whitespace
        return text.strip()
    
    def _normalize_name_order(self, name: str) -> str:
        """
        Normalize the order of name components for individuals.
        
        Handles cases like "Last, First" -> "First Last"
        Also handles suffixes properly: "Last, First Jr." -> "First Last Jr."
        
        Args:
            name: Individual name potentially in various formats
            
        Returns:
            Name in standardized order
        """
        # Handle "Last, First" format
        if ',' in name:
            parts = [part.strip() for part in name.split(',')]
            if len(parts) == 2 and parts[1] and parts[0]:
                # Check if the first name part has a suffix
                first_part_words = parts[1].split()
                last_part_words = parts[0].split()
                
                # If there are multiple words in the first part, the last might be a suffix
                if len(first_part_words) > 1:
                    # Check if the last word looks like a suffix
                    potential_suffix = first_part_words[-1].lower().rstrip('.')
                    suffix_indicators = {'jr', 'sr', 'ii', 'iii', 'iv', 'v', 'junior', 'senior'}
                    
                    if potential_suffix in suffix_indicators:
                        # Reorder as: First Last Suffix
                        first_name = ' '.join(first_part_words[:-1])
                        suffix = first_part_words[-1]
                        return f"{first_name} {parts[0]} {suffix}"
                
                # Standard reorder: First Last
                return f"{parts[1]} {parts[0]}"
        
        return name
    
    def _handle_titles_and_suffixes(self, name: str) -> str:
        """
        Handle titles and suffixes in individual names.
        
        Args:
            name: Individual name with potential titles/suffixes
            
        Returns:
            Name with standardized titles/suffixes
        """
        # Common titles to remove or standardize
        titles = {
            'mr', 'mr.', 'mister',
            'mrs', 'mrs.', 'missus',
            'ms', 'ms.', 'miss',
            'dr', 'dr.', 'doctor',
            'prof', 'prof.', 'professor',
            'sir', 'dame', 'lord', 'lady'
        }
        
        # Common suffixes to standardize
        suffixes = {
            'jr': 'junior',
            'jr.': 'junior',
            'junior': 'junior',
            'sr': 'senior',
            'sr.': 'senior',
            'senior': 'senior',
            'ii': '2nd',
            'iii': '3rd',
            'iv': '4th',
            'v': '5th'
        }
        
        words = name.split()
        filtered_words = []
        
        for word in words:
            clean_word = word.lower().rstrip('.,')
            
            # Skip common titles
            if clean_word in titles:
                continue
            
            # Standardize suffixes
            if clean_word in suffixes:
                filtered_words.append(suffixes[clean_word])
            else:
                filtered_words.append(word)
        
        return ' '.join(filtered_words)
    
    def get_name_variations(self, name: str, entity_type: str = 'company') -> List[str]:
        """
        Generate common variations of a name for improved matching.
        
        Args:
            name: The original name
            entity_type: Type of entity ('company' or 'individual')
            
        Returns:
            List of name variations
        """
        if not name:
            return []
        
        variations = set()
        
        # Always include the normalized version
        if entity_type.lower() == 'company':
            normalized = self.normalize_company_name(name)
        else:
            normalized = self.normalize_individual_name(name)
        
        variations.add(normalized)
        
        # Add the original (preprocessed only)
        variations.add(self.preprocess_name(name))
        
        # Generate additional variations
        if entity_type.lower() == 'company':
            variations.update(self._generate_company_variations(normalized))
        else:
            variations.update(self._generate_individual_variations(normalized))
        
        # Remove empty strings and return as list
        return [v for v in variations if v.strip()]
    
    def _generate_company_variations(self, normalized_name: str) -> Set[str]:
        """
        Generate company-specific name variations.
        
        Args:
            normalized_name: The normalized company name
            
        Returns:
            Set of company name variations
        """
        variations = set()
        
        # Version without legal suffix
        words = normalized_name.split()
        if words and words[-1] in self._company_suffixes.values():
            variations.add(' '.join(words[:-1]))
        
        # Version with common abbreviations
        abbreviated = normalized_name
        for full_form, abbrev in {v: k for k, v in self._common_abbreviations.items()}.items():
            if full_form in abbreviated:
                variations.add(abbreviated.replace(full_form, abbrev))
        
        return variations
    
    def _generate_individual_variations(self, normalized_name: str) -> Set[str]:
        """
        Generate individual-specific name variations.
        
        Args:
            normalized_name: The normalized individual name
            
        Returns:
            Set of individual name variations
        """
        variations = set()
        
        words = normalized_name.split()
        if len(words) >= 2:
            # First name + last name only
            variations.add(f"{words[0]} {words[-1]}")
            
            # Last name + first name
            variations.add(f"{words[-1]} {words[0]}")
            
            # Initials + last name
            initials = ''.join(word[0] for word in words[:-1] if word)
            if initials:
                variations.add(f"{initials} {words[-1]}")
        
        return variations
    
    def normalize_name(self, name: str, entity_type: str = None) -> str:
        """
        General name normalization method that determines entity type and applies appropriate normalization.
        
        Args:
            name: The name to normalize
            entity_type: Optional entity type ('company', 'individual'). If None, will be inferred.
            
        Returns:
            Normalized name
        """
        if not name:
            return ""
        
        # If entity type is not specified, try to infer it
        if entity_type is None:
            # Simple heuristic: if it contains common company suffixes or indicators, treat as company
            name_lower = name.lower()
            company_indicators = ['inc', 'corp', 'ltd', 'llc', 'company', 'co.', 'corporation', 'limited']
            
            if any(indicator in name_lower for indicator in company_indicators):
                entity_type = 'company'
            else:
                entity_type = 'individual'
        
        # Apply appropriate normalization
        if entity_type.lower() == 'company':
            return self.normalize_company_name(name)
        else:
            return self.normalize_individual_name(name)