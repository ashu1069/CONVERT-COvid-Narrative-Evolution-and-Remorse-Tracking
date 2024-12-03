import re
from typing import Dict
import pandas as pd
import logging

class CommentAnalyzer:
    """Handles individual comment analysis"""
    
    def analyze_comment(self, comment_row, remorse_patterns):
        """
        Analyze a single comment for signs of remorse and other metrics
        """
        # Initialize result dictionary
        result = {
            'has_remorse': False,
            'has_edit': False,
            'sentiment_score': 0.0,
            'edit_count': 0,
            'remorse_patterns_found': [],
            'timestamp': comment_row.get('publishedAt', None)
        }
        
        # Get the comment text
        comment_text = str(comment_row.get('text', '')).lower()
        
        # Skip empty comments
        if not comment_text:
            return result
        
        # Check for edit indicators
        edit_indicators = ['edit:', 'edited:', 'update:', 'updated:', '*edit', '*update']
        result['has_edit'] = any(indicator in comment_text for indicator in edit_indicators)
        
        # Check for remorse patterns
        for pattern in remorse_patterns:
            if pattern.search(comment_text):
                result['has_remorse'] = True
                result['remorse_patterns_found'].append(pattern.pattern)
        
        # Calculate sentiment score
        try:
            result['sentiment_score'] = self.sentiment_analyzer.polarity_scores(comment_text)['compound']
        except Exception as e:
            logging.warning(f"Error calculating sentiment: {str(e)}")
            result['sentiment_score'] = 0.0
        
        # Count edits
        result['edit_count'] = sum(1 for indicator in edit_indicators if indicator in comment_text)
        
        return result

    def _classify_remorse_type(self, text: str) -> str:
        """Classify the type of remorse expressed"""
        if any(pattern.search(text) for pattern in [
            re.compile(r'family|friend|loved one|personal', re.IGNORECASE)
        ]):
            return 'personal_experience'
        elif any(pattern.search(text) for pattern in [
            re.compile(r'research|evidence|studies|data|science', re.IGNORECASE)
        ]):
            return 'scientific_evidence'
        elif any(pattern.search(text) for pattern in [
            re.compile(r'doctor|medical|healthcare|professional', re.IGNORECASE)
        ]):
            return 'medical_authority'
        return 'general_remorse'

    def _analyze_catalyst(self, text: str, catalyst: str) -> Dict:
        """Analyze the catalyst type and severity"""
        severity = 1  # Default severity
        
        # Determine catalyst type
        if re.search(r'died|passed|death|fatal', text, re.IGNORECASE):
            catalyst_type = 'death'
            severity = 3
        elif re.search(r'hospital|icu|ventilator', text, re.IGNORECASE):
            catalyst_type = 'severe_illness'
            severity = 2
        elif re.search(r'sick|covid|ill', text, re.IGNORECASE):
            catalyst_type = 'illness'
            severity = 1
        else:
            catalyst_type = 'other'
        
        return {
            'type': catalyst_type,
            'severity': severity
        }