"""
Fuzzy matching algorithms for sanctions screening.

This module implements multiple fuzzy string matching algorithms including:
- Levenshtein distance matching
- Jaro-Winkler similarity matching  
- Soundex phonetic matching

Each algorithm provides configurable thresholds and confidence scoring.
"""

import re
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class MatchResult:
    """Result of a fuzzy matching operation."""
    algorithm: str
    score: float
    threshold: float
    is_match: bool
    normalized_query: str
    normalized_target: str


class LevenshteinMatcher:
    """Implements Levenshtein distance matching with configurable thresholds."""
    
    def __init__(self, threshold: float = 0.8):
        """
        Initialize Levenshtein matcher.
        
        Args:
            threshold: Minimum similarity score (0.0-1.0) to consider a match
        """
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Threshold must be between 0.0 and 1.0")
        self.threshold = threshold
    
    def distance(self, s1: str, s2: str) -> int:
        """
        Calculate Levenshtein distance between two strings.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Edit distance between the strings
        """
        if len(s1) < len(s2):
            return self.distance(s2, s1)
        
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
    
    def similarity(self, s1: str, s2: str) -> float:
        """
        Calculate similarity score (0.0-1.0) based on Levenshtein distance.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Similarity score where 1.0 is identical
        """
        if not s1 and not s2:
            return 1.0
        if not s1 or not s2:
            return 0.0
            
        max_len = max(len(s1), len(s2))
        distance = self.distance(s1, s2)
        return 1.0 - (distance / max_len)
    
    def match(self, query: str, target: str) -> MatchResult:
        """
        Perform Levenshtein matching between query and target strings.
        
        Args:
            query: Search query string
            target: Target string to match against
            
        Returns:
            MatchResult with score and match status
        """
        # Normalize strings for comparison
        norm_query = self._normalize_string(query)
        norm_target = self._normalize_string(target)
        
        score = self.similarity(norm_query, norm_target)
        is_match = score >= self.threshold
        
        return MatchResult(
            algorithm="levenshtein",
            score=score,
            threshold=self.threshold,
            is_match=is_match,
            normalized_query=norm_query,
            normalized_target=norm_target
        )
    
    def _normalize_string(self, s: str) -> str:
        """Normalize string for matching (lowercase, remove extra spaces)."""
        return re.sub(r'\s+', ' ', s.lower().strip())


class JaroWinklerMatcher:
    """Implements Jaro-Winkler similarity matching algorithm."""
    
    def __init__(self, threshold: float = 0.85, prefix_scale: float = 0.1):
        """
        Initialize Jaro-Winkler matcher.
        
        Args:
            threshold: Minimum similarity score (0.0-1.0) to consider a match
            prefix_scale: Scaling factor for common prefix bonus (0.0-0.25)
        """
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Threshold must be between 0.0 and 1.0")
        if not 0.0 <= prefix_scale <= 0.25:
            raise ValueError("Prefix scale must be between 0.0 and 0.25")
        self.threshold = threshold
        self.prefix_scale = prefix_scale
    
    def jaro_similarity(self, s1: str, s2: str) -> float:
        """
        Calculate Jaro similarity between two strings.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Jaro similarity score (0.0-1.0)
        """
        if not s1 and not s2:
            return 1.0
        if not s1 or not s2:
            return 0.0
        if s1 == s2:
            return 1.0
        
        len1, len2 = len(s1), len(s2)
        match_window = max(len1, len2) // 2 - 1
        match_window = max(0, match_window)
        
        s1_matches = [False] * len1
        s2_matches = [False] * len2
        
        matches = 0
        transpositions = 0
        
        # Find matches
        for i in range(len1):
            start = max(0, i - match_window)
            end = min(i + match_window + 1, len2)
            
            for j in range(start, end):
                if s2_matches[j] or s1[i] != s2[j]:
                    continue
                s1_matches[i] = s2_matches[j] = True
                matches += 1
                break
        
        if matches == 0:
            return 0.0
        
        # Count transpositions
        k = 0
        for i in range(len1):
            if not s1_matches[i]:
                continue
            while not s2_matches[k]:
                k += 1
            if s1[i] != s2[k]:
                transpositions += 1
            k += 1
        
        jaro = (matches / len1 + matches / len2 + 
                (matches - transpositions / 2) / matches) / 3.0
        
        return jaro
    
    def similarity(self, s1: str, s2: str) -> float:
        """
        Calculate Jaro-Winkler similarity with prefix bonus.
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Jaro-Winkler similarity score (0.0-1.0)
        """
        jaro = self.jaro_similarity(s1, s2)
        
        if jaro < 0.7:  # Only apply prefix bonus if Jaro similarity is high enough
            return jaro
        
        # Calculate common prefix length (up to 4 characters)
        prefix_len = 0
        for i in range(min(len(s1), len(s2), 4)):
            if s1[i] == s2[i]:
                prefix_len += 1
            else:
                break
        
        # Apply prefix bonus only if there is a common prefix
        if prefix_len > 0:
            return jaro + (prefix_len * self.prefix_scale * (1 - jaro))
        else:
            return jaro
    
    def match(self, query: str, target: str) -> MatchResult:
        """
        Perform Jaro-Winkler matching between query and target strings.
        
        Args:
            query: Search query string
            target: Target string to match against
            
        Returns:
            MatchResult with score and match status
        """
        # Normalize strings for comparison
        norm_query = self._normalize_string(query)
        norm_target = self._normalize_string(target)
        
        score = self.similarity(norm_query, norm_target)
        is_match = score >= self.threshold
        
        return MatchResult(
            algorithm="jaro_winkler",
            score=score,
            threshold=self.threshold,
            is_match=is_match,
            normalized_query=norm_query,
            normalized_target=norm_target
        )
    
    def _normalize_string(self, s: str) -> str:
        """Normalize string for matching (lowercase, remove extra spaces)."""
        return re.sub(r'\s+', ' ', s.lower().strip())


class SoundexMatcher:
    """Implements Soundex phonetic matching algorithm."""
    
    def __init__(self, threshold: float = 1.0):
        """
        Initialize Soundex matcher.
        
        Args:
            threshold: Minimum similarity score (0.0-1.0) to consider a match
                      For Soundex, typically 1.0 (exact match) or 0.0 (any match)
        """
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Threshold must be between 0.0 and 1.0")
        self.threshold = threshold
    
    def soundex(self, s: str) -> str:
        """
        Generate Soundex code for a string.
        
        Args:
            s: Input string
            
        Returns:
            4-character Soundex code
        """
        if not s:
            return "0000"
        
        s = s.upper()
        # Keep only alphabetic characters
        s = re.sub(r'[^A-Z]', '', s)
        
        if not s:
            return "0000"
        
        # Soundex mapping
        mapping = {
            'B': '1', 'F': '1', 'P': '1', 'V': '1',
            'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
            'D': '3', 'T': '3',
            'L': '4',
            'M': '5', 'N': '5',
            'R': '6'
        }
        
        # Start with first letter
        soundex_code = s[0]
        prev_code = mapping.get(s[0], '0')
        
        # Process remaining letters
        for char in s[1:]:
            if char in mapping:
                code = mapping[char]
                # Don't add duplicate codes (including the first letter's code)
                if code != prev_code:
                    soundex_code += code
                    prev_code = code
                    # Stop when we have 4 characters
                    if len(soundex_code) == 4:
                        break
            else:
                # Reset previous code for vowels/consonants not in mapping
                prev_code = '0'
        
        # Pad with zeros to make exactly 4 characters
        soundex_code = (soundex_code + "000")[:4]
        
        return soundex_code
    
    def similarity(self, s1: str, s2: str) -> float:
        """
        Calculate Soundex similarity (1.0 for exact match, 0.0 for no match).
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Similarity score (0.0 or 1.0)
        """
        soundex1 = self.soundex(s1)
        soundex2 = self.soundex(s2)
        return 1.0 if soundex1 == soundex2 else 0.0
    
    def match(self, query: str, target: str) -> MatchResult:
        """
        Perform Soundex matching between query and target strings.
        
        Args:
            query: Search query string
            target: Target string to match against
            
        Returns:
            MatchResult with score and match status
        """
        # Normalize strings for comparison
        norm_query = self._normalize_string(query)
        norm_target = self._normalize_string(target)
        
        score = self.similarity(norm_query, norm_target)
        is_match = score >= self.threshold
        
        return MatchResult(
            algorithm="soundex",
            score=score,
            threshold=self.threshold,
            is_match=is_match,
            normalized_query=norm_query,
            normalized_target=norm_target
        )
    
    def _normalize_string(self, s: str) -> str:
        """Normalize string for matching (remove non-alphabetic chars, uppercase)."""
        return re.sub(r'[^A-Za-z\s]', '', s).strip()


class FuzzyMatcher:
    """
    Main fuzzy matching class that orchestrates multiple algorithms.
    """
    
    def __init__(self, 
                 levenshtein_threshold: float = 0.8,
                 jaro_winkler_threshold: float = 0.85,
                 soundex_threshold: float = 1.0,
                 jaro_prefix_scale: float = 0.1):
        """
        Initialize fuzzy matcher with all algorithms.
        
        Args:
            levenshtein_threshold: Threshold for Levenshtein matching
            jaro_winkler_threshold: Threshold for Jaro-Winkler matching
            soundex_threshold: Threshold for Soundex matching
            jaro_prefix_scale: Prefix scale for Jaro-Winkler algorithm
        """
        self.levenshtein = LevenshteinMatcher(levenshtein_threshold)
        self.jaro_winkler = JaroWinklerMatcher(jaro_winkler_threshold, jaro_prefix_scale)
        self.soundex = SoundexMatcher(soundex_threshold)
    
    def match_all(self, query: str, target: str) -> Dict[str, MatchResult]:
        """
        Run all fuzzy matching algorithms on query and target.
        
        Args:
            query: Search query string
            target: Target string to match against
            
        Returns:
            Dictionary mapping algorithm names to MatchResult objects
        """
        results = {}
        
        results['levenshtein'] = self.levenshtein.match(query, target)
        results['jaro_winkler'] = self.jaro_winkler.match(query, target)
        results['soundex'] = self.soundex.match(query, target)
        
        return results
    
    def get_best_match(self, query: str, targets: List[str]) -> Tuple[Optional[str], Dict[str, MatchResult]]:
        """
        Find the best matching target from a list of candidates.
        
        Args:
            query: Search query string
            targets: List of target strings to match against
            
        Returns:
            Tuple of (best_target, match_results) or (None, {}) if no matches
        """
        best_target = None
        best_results = {}
        best_score = 0.0
        
        for target in targets:
            results = self.match_all(query, target)
            
            # Calculate overall confidence as weighted average
            total_score = 0.0
            total_weight = 0.0
            
            for result in results.values():
                if result.is_match:
                    # Weight algorithms differently based on their reliability
                    weight = {'levenshtein': 0.4, 'jaro_winkler': 0.4, 'soundex': 0.2}.get(result.algorithm, 0.33)
                    total_score += result.score * weight
                    total_weight += weight
            
            if total_weight > 0:
                overall_score = total_score / total_weight
                if overall_score > best_score:
                    best_score = overall_score
                    best_target = target
                    best_results = results
        
        return best_target, best_results
    
    def update_thresholds(self, 
                         levenshtein_threshold: Optional[float] = None,
                         jaro_winkler_threshold: Optional[float] = None,
                         soundex_threshold: Optional[float] = None):
        """
        Update matching thresholds for all algorithms.
        
        Args:
            levenshtein_threshold: New threshold for Levenshtein matching
            jaro_winkler_threshold: New threshold for Jaro-Winkler matching
            soundex_threshold: New threshold for Soundex matching
        """
        if levenshtein_threshold is not None:
            self.levenshtein.threshold = levenshtein_threshold
        if jaro_winkler_threshold is not None:
            self.jaro_winkler.threshold = jaro_winkler_threshold
        if soundex_threshold is not None:
            self.soundex.threshold = soundex_threshold