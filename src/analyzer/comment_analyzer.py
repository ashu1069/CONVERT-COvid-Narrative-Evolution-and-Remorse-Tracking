import re
from typing import Dict
import pandas as pd

class CommentAnalyzer:
    """Handles individual comment analysis"""
    
    def analyze_comment(self, row: pd.Series, patterns: Dict) -> Dict:
        """
        Analyze a single comment for indicators of vaccine bias remorse
        
        Parameters:
        row: pandas Series containing comment data
        patterns: Dictionary of compiled regex patterns
        
        Returns:
        Dict containing analysis results
        """
        # Get text from either 'cleaned_text' or 'text' column
        text = row.get('cleaned_text', row.get('text', ''))
        if pd.isna(text):
            text = ''
        text = str(text).lower()  # Ensure text is string and lowercase
        
        result = {
            'comment_id': row.get('commentId', ''),
            'has_remorse': False,
            'remorse_type': None,
            'previous_stance': None,
            'catalyst': None,
            'political_lean': None,
            'confidence_score': 0,
            'timestamp': pd.to_datetime(row.get('publishedAt')),
            'channel': row.get('channel', 'unknown'),
            'engagement_score': float(row.get('likeCount', 0)) + float(row.get('totalReplyCount', 0)),
            'has_edit': pd.to_datetime(row.get('publishedAt')) != pd.to_datetime(row.get('updatedAt'))
            if not pd.isna(row.get('updatedAt')) else False
        }
        
        # Check for remorse indicators
        admission_matches = sum(1 for pattern in patterns['admission'] 
                              if pattern.search(text))
        
        if admission_matches > 0:
            result['has_remorse'] = True
            result['confidence_score'] += admission_matches
            
            # Check previous anti-vax stance
            anti_vax_matches = sum(1 for pattern in patterns['previous_anti_vax'] 
                                 if pattern.search(text))
            if anti_vax_matches > 0:
                result['previous_stance'] = 'anti_vax'
                result['confidence_score'] += anti_vax_matches
            
            # Check for catalyst
            for pattern in patterns['catalyst']:
                match = pattern.search(text)
                if match:
                    result['catalyst'] = match.group()
                    result['confidence_score'] += 1
                    break
            
            # Check current pro-vax stance
            pro_vax_matches = sum(1 for pattern in patterns['current_pro_vax'] 
                                if pattern.search(text))
            if pro_vax_matches > 0:
                result['confidence_score'] += pro_vax_matches
            
            # Classify remorse type
            result['remorse_type'] = self._classify_remorse_type(text)
        
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