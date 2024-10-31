import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

class VaccineBiasRemorseAnalyzer:
    def __init__(self):
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Define indicators of bias remorse
        self.remorse_patterns = {
            'admission': [
                r'i (?:was|have been) wrong',
                r'changed my (?:mind|opinion|stance|view)',
                r'i regret',
                r'(?:now )?(?:i )?realize',
                r'used to (?:think|believe)',
                r'i admit',
                r'wish i had',
                r'should have listened',
                r'(?:now )?understand',
                r'take back what i said'
            ],
            
            'previous_anti_vax': [
                r'refused (?:to get|getting) (?:the )?(?:vaccine|vax|shot)',
                r'(?:was )?against (?:the )?(?:vaccine|vax|shot)',
                r'thought (?:covid|it) was fake',
                r'conspiracy',
                r'didn\'t trust (?:the )?(?:vaccine|science|doctors)',
                r'wouldn\'t get (?:the )?(?:vaccine|vax|shot)',
                r'(?:vaccine|covid) hoax',
                r'experimental gene therapy',
                r'resisted (?:the )?(?:vaccine|vax|shot)'
            ],
            
            'catalyst': [
                r'got (?:really )?sick',
                r'got covid',
                r'family member',
                r'friend (?:died|passed)',
                r'(?:was )?hospitalized',
                r'lost (?:my |our )?(?:friend|family|parent|spouse)',
                r'personal experience',
                r'(?:doctor|research) showed'
            ],
            
            'current_pro_vax': [
                r'(?:got|getting) (?:the )?(?:vaccine|vax|shot)',
                r'(?:now )?(?:trust|believe) (?:the )?science',
                r'protect (?:others|community|family)',
                r'follow (?:the )?evidence',
                r'listen to doctors',
                r'science is real',
                r'vaccines work',
                r'changed perspective'
            ]
        }
        
        # Political alignment patterns
        self.political_patterns = {
            'conservative': [
                r'republican',
                r'trump',
                r'conservative',
                r'right(?:-| )wing',
                r'freedom',
                r'liberty',
                r'personal choice',
                r'mandate',
                r'government control',
                r'fox news'
            ],
            'progressive': [
                r'democrat',
                r'biden',
                r'liberal',
                r'left(?:-| )wing',
                r'public health',
                r'community',
                r'science',
                r'collective',
                r'responsibility',
                r'msnbc'
            ]
        }
        
        # Compile all regex patterns
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile regex patterns for efficient matching"""
        for category, patterns in self.remorse_patterns.items():
            self.remorse_patterns[category] = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
        
        for leaning, patterns in self.political_patterns.items():
            self.political_patterns[leaning] = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]

    def analyze_comment(self, row: pd.Series) -> Dict:
        """
        Analyze a single comment for indicators of vaccine bias remorse
        
        Parameters:
        row: pandas Series containing comment data
        
        Returns:
        Dict with analysis results
        """
        text = row['cleaned_text']
        
        result = {
            'comment_id': row['commentId'],
            'has_remorse': False,
            'remorse_type': None,
            'previous_stance': None,
            'catalyst': None,
            'political_lean': None,
            'confidence_score': 0,
            'timestamp': row['publishedAt'],
            'channel': row['channel'],
            'engagement_score': row.get('engagement_score', 0),
            'has_edit': row.get('has_edited', False)
        }
        
        # Check for remorse indicators
        admission_matches = sum(1 for pattern in self.remorse_patterns['admission'] 
                              if pattern.search(text))
        
        if admission_matches > 0:
            result['has_remorse'] = True
            result['confidence_score'] += admission_matches
            
            # Check previous anti-vax stance
            anti_vax_matches = sum(1 for pattern in self.remorse_patterns['previous_anti_vax'] 
                                 if pattern.search(text))
            if anti_vax_matches > 0:
                result['previous_stance'] = 'anti_vax'
                result['confidence_score'] += anti_vax_matches
            
            # Check for catalyst
            for pattern in self.remorse_patterns['catalyst']:
                match = pattern.search(text)
                if match:
                    result['catalyst'] = match.group()
                    result['confidence_score'] += 1
                    break
            
            # Check current pro-vax stance
            pro_vax_matches = sum(1 for pattern in self.remorse_patterns['current_pro_vax'] 
                                if pattern.search(text))
            if pro_vax_matches > 0:
                result['confidence_score'] += pro_vax_matches
            
            # Determine political leaning
            conservative_matches = sum(1 for pattern in self.political_patterns['conservative'] 
                                    if pattern.search(text))
            progressive_matches = sum(1 for pattern in self.political_patterns['progressive'] 
                                   if pattern.search(text))
            
            if conservative_matches > progressive_matches:
                result['political_lean'] = 'conservative'
            elif progressive_matches > conservative_matches:
                result['political_lean'] = 'progressive'
            
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

    def analyze_dataset(self, df: pd.DataFrame) -> Dict:
        """
        Analyze entire dataset for bias remorse
        
        Parameters:
        df: DataFrame from dataset.py's get_analysis_ready_data()
        
        Returns:
        Dict containing analysis results and statistics
        """
        self.logger.info("Starting dataset analysis...")
        
        results = []
        for _, row in df.iterrows():
            analysis = self.analyze_comment(row)
            if analysis['has_remorse']:
                results.append(analysis)
        
        return self._generate_analysis_report(results, df)

    def _generate_analysis_report(self, results: List[Dict], full_df: pd.DataFrame) -> Dict:
        """Generate comprehensive analysis report"""
        if not results:
            return {"error": "No bias remorse cases detected"}
        
        report = {
            'summary': {
                'total_comments_analyzed': len(full_df),
                'remorse_cases': len(results),
                'remorse_rate': (len(results) / len(full_df)) * 100
            },
            'channel_analysis': self._analyze_by_channel(results, full_df),
            'temporal_analysis': self._analyze_temporal_patterns(results),
            'remorse_types': self._analyze_remorse_types(results),
            'catalysts': self._analyze_catalysts(results),
            'political_distribution': self._analyze_political_distribution(results),
            'engagement_metrics': self._analyze_engagement(results),
            'edit_patterns': self._analyze_edit_patterns(results),
            'key_findings': []
        }
        
        # Generate key findings
        report['key_findings'] = self._extract_key_findings(report)
        
        return report

    def _analyze_by_channel(self, results: List[Dict], full_df: pd.DataFrame) -> Dict:
        """Analyze remorse patterns by channel"""
        channel_stats = defaultdict(lambda: {
            'remorse_count': 0,
            'total_comments': 0,
            'remorse_rate': 0,
            'avg_confidence': 0
        })
        
        # Count total comments per channel
        channel_totals = full_df['channel'].value_counts().to_dict()
        
        for result in results:
            channel = result['channel']
            channel_stats[channel]['remorse_count'] += 1
            channel_stats[channel]['avg_confidence'] += result['confidence_score']
        
        # Calculate statistics
        for channel, stats in channel_stats.items():
            total = channel_totals.get(channel, 0)
            stats['total_comments'] = total
            stats['remorse_rate'] = (stats['remorse_count'] / total * 100) if total > 0 else 0
            stats['avg_confidence'] /= stats['remorse_count'] if stats['remorse_count'] > 0 else 1
        
        return dict(channel_stats)

    def _analyze_temporal_patterns(self, results: List[Dict]) -> Dict:
        """Analyze temporal patterns in remorse expressions"""
        temporal = defaultdict(lambda: {
            'count': 0,
            'types': defaultdict(int),
            'catalysts': defaultdict(int)
        })
        
        for result in results:
            month = result['timestamp'].strftime('%Y-%m')
            temporal[month]['count'] += 1
            temporal[month]['types'][result['remorse_type']] += 1
            if result['catalyst']:
                temporal[month]['catalysts'][result['catalyst']] += 1
        
        return dict(temporal)

    def _analyze_remorse_types(self, results: List[Dict]) -> Dict:
        """Analyze distribution of remorse types"""
        types = defaultdict(lambda: {
            'count': 0,
            'avg_confidence': 0,
            'political_distribution': defaultdict(int)
        })
        
        for result in results:
            rtype = result['remorse_type']
            types[rtype]['count'] += 1
            types[rtype]['avg_confidence'] += result['confidence_score']
            if result['political_lean']:
                types[rtype]['political_distribution'][result['political_lean']] += 1
        
        # Calculate averages
        for stats in types.values():
            stats['avg_confidence'] /= stats['count'] if stats['count'] > 0 else 1
        
        return dict(types)

    def _analyze_catalysts(self, results: List[Dict]) -> Dict:
        """Analyze what triggered remorse"""
        return {
            'distribution': defaultdict(int, 
                Counter(r['catalyst'] for r in results if r['catalyst'])),
            'by_channel': defaultdict(lambda: defaultdict(int))
        }

    def _analyze_political_distribution(self, results: List[Dict]) -> Dict:
        """Analyze political leanings in remorse cases"""
        return {
            'overall': defaultdict(int, 
                Counter(r['political_lean'] for r in results if r['political_lean'])),
            'by_channel': defaultdict(lambda: defaultdict(int))
        }

    def _analyze_engagement(self, results: List[Dict]) -> Dict:
        """Analyze engagement patterns"""
        return {
            'avg_engagement': np.mean([r['engagement_score'] for r in results]),
            'by_remorse_type': defaultdict(list)
        }

    def _analyze_edit_patterns(self, results: List[Dict]) -> Dict:
        """Analyze patterns in edited comments"""
        return {
            'edit_rate': sum(1 for r in results if r['has_edit']) / len(results),
            'edited_confidence': np.mean([r['confidence_score'] 
                                       for r in results if r['has_edit']])
        }

    def _extract_key_findings(self, report: Dict) -> List[str]:
        """Extract detailed insights from the analysis"""
        findings = []
        
        # 1. Overall Statistics
        findings.append("\n=== OVERALL STATISTICS ===")
        findings.append(
            f"• Total Comments Analyzed: {report['summary']['total_comments_analyzed']:,}"
            f"\n• Remorse Cases Identified: {report['summary']['remorse_cases']:,}"
            f"\n• Overall Remorse Rate: {report['summary']['remorse_rate']:.2f}%"
        )
        
        # 2. Channel Analysis
        findings.append("\n=== CHANNEL ANALYSIS ===")
        for channel, stats in report['channel_analysis'].items():
            findings.append(
                f"• {channel}:"
                f"\n  - Remorse Cases: {stats['remorse_count']:,}"
                f"\n  - Total Comments: {stats['total_comments']:,}"
                f"\n  - Remorse Rate: {stats['remorse_rate']:.2f}%"
                f"\n  - Average Confidence Score: {stats['avg_confidence']:.2f}"
            )
        
        # 3. Remorse Types Analysis
        findings.append("\n=== REMORSE TYPES BREAKDOWN ===")
        for rtype, stats in report['remorse_types'].items():
            findings.append(
                f"• {rtype.replace('_', ' ').title()}:"
                f"\n  - Count: {stats['count']:,}"
                f"\n  - Average Confidence: {stats['avg_confidence']:.2f}"
                f"\n  - Political Distribution: {dict(stats['political_distribution'])}"
            )
        
        # 4. Catalyst Analysis
        findings.append("\n=== CATALYST PATTERNS ===")
        catalyst_dist = report['catalysts']['distribution']
        total_catalysts = sum(catalyst_dist.values())
        for catalyst, count in catalyst_dist.items():
            if catalyst:  # Skip None/empty catalysts
                findings.append(
                    f"• {catalyst}: {count:,} cases"
                    f" ({(count/total_catalysts*100):.1f}%)"
                )
        
        # 5. Political Distribution
        findings.append("\n=== POLITICAL ALIGNMENT ===")
        pol_dist = report['political_distribution']['overall']
        total_political = sum(pol_dist.values())
        for leaning, count in pol_dist.items():
            if leaning:  # Skip None/empty leanings
                findings.append(
                    f"• {leaning.title()}: {count:,} cases"
                    f" ({(count/total_political*100):.1f}%)"
                )
        
        # 6. Engagement Metrics
        findings.append("\n=== ENGAGEMENT METRICS ===")
        findings.append(
            f"• Average Engagement Score: {report['engagement_metrics']['avg_engagement']:.2f}"
        )
        
        # 7. Edit Patterns
        findings.append("\n=== EDIT PATTERNS ===")
        findings.append(
            f"• Edit Rate: {report['edit_patterns']['edit_rate']*100:.1f}%"
            f"\n• Average Confidence in Edited Comments: "
            f"{report['edit_patterns']['edited_confidence']:.2f}"
        )
        
        # 8. Temporal Insights
        findings.append("\n=== TEMPORAL PATTERNS ===")
        temporal_data = report['temporal_analysis']
        for month, data in sorted(temporal_data.items()):
            findings.append(
                f"• {month}:"
                f"\n  - Cases: {data['count']}"
                f"\n  - Types: {dict(data['types'])}"
            )
        
        return findings

# Example usage
if __name__ == "__main__":
    from dataset import create_dataset
    import sys
    from datetime import datetime
    
    # Redirect output to both console and file
    class Logger:
        def __init__(self, filename):
            self.terminal = sys.stdout
            self.log = open(filename, 'w', encoding='utf-8')
        
        def write(self, message):
            self.terminal.write(message)
            self.log.write(message)
            
        def flush(self):
            self.terminal.flush()
            self.log.flush()
    
    # Create results directory if it doesn't exist
    from pathlib import Path
    Path('results').mkdir(exist_ok=True)
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = f'results/analysis_results_{timestamp}.txt'
    
    # Set up logging to both console and file
    sys.stdout = Logger(results_file)
    
    try:
        print(f"=== VACCINE BIAS REMORSE ANALYSIS ===")
        print(f"Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        
        # Initialize dataset and analyzer
        dataset = create_dataset("extracted_text_cnn")
        analyzer = VaccineBiasRemorseAnalyzer()
        
        # Get analysis-ready data and run analysis
        print("\nProcessing data...")
        analysis_data = dataset.get_analysis_ready_data()
        results = analyzer.analyze_dataset(analysis_data)
        
        # Print detailed findings
        print("\n=== DETAILED ANALYSIS REPORT ===")
        print("=" * 50)
        
        # Print dataset overview
        print("\n1. DATASET OVERVIEW")
        print(f"Total comments analyzed: {len(analysis_data):,}")
        print(f"Time range: {analysis_data['publishedAt'].min()} to {analysis_data['publishedAt'].max()}")
        print("\nChannel distribution:")
        print(analysis_data['channel'].value_counts().to_string())
        
        # Print detailed findings
        print("\n2. ANALYSIS FINDINGS")
        for finding in results['key_findings']:
            print(finding)
            
        # Print summary statistics
        print("\n3. SUMMARY STATISTICS")
        print(f"Total remorse cases: {results['summary']['remorse_cases']:,}")
        print(f"Overall remorse rate: {results['summary']['remorse_rate']:.2f}%")
        
        # Print engagement metrics
        print("\n4. ENGAGEMENT ANALYSIS")
        print(f"Average engagement score: {results['engagement_metrics']['avg_engagement']:.2f}")
        print(f"Edit rate: {results['edit_patterns']['edit_rate']*100:.1f}%")
        
        print("\n=" * 25 + " END OF REPORT " + "=" * 25)
        print(f"\nAnalysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Results saved to: {results_file}")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        raise
    finally:
        # Restore original stdout
        sys.stdout = sys.stdout.terminal