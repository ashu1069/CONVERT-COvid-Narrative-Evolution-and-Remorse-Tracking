from .patterns import REMORSE_PATTERNS, POLITICAL_PATTERNS
from .comment_analyzer import CommentAnalyzer
from .statistical_analyzer import StatisticalAnalyzer
from .report_generator import ReportGenerator
import pandas as pd
import logging
from typing import Dict, List
import re
from datetime import datetime
from pathlib import Path

class VaccineBiasRemorseAnalyzer:
    def __init__(self):
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.comment_analyzer = CommentAnalyzer()
        self.statistical_analyzer = StatisticalAnalyzer()
        self.report_generator = ReportGenerator()
        
        # Import and compile patterns
        self.remorse_patterns = REMORSE_PATTERNS
        self.political_patterns = POLITICAL_PATTERNS
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile regex patterns for efficient matching"""
        for category, patterns in self.remorse_patterns.items():
            self.remorse_patterns[category] = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
        
        for leaning, patterns in self.political_patterns.items():
            self.political_patterns[leaning] = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]

    def analyze_dataset(self, df: pd.DataFrame) -> Dict:
        """Analyze dataset and generate formatted report"""
        self.logger.info("Starting dataset analysis...")
        
        results = []
        for _, row in df.iterrows():
            analysis = self.comment_analyzer.analyze_comment(row, self.remorse_patterns)
            if analysis['has_remorse']:
                results.append(analysis)
        
        # Generate report using ReportGenerator
        report = self.report_generator.generate_analysis_report(results, df)
        
        # Format and save results
        self._save_formatted_results(report)
        
        return report

    def _save_formatted_results(self, report: Dict):
        """Save formatted results to file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = f'results/analysis_results_{timestamp}.txt'
        
        Path('results').mkdir(exist_ok=True)
        
        with open(results_file, 'w', encoding='utf-8') as f:
            f.write("=== VACCINE BIAS REMORSE ANALYSIS ===\n")
            f.write(f"Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 50 + "\n")
            
            # Write findings sections
            for finding in report['key_findings']:
                f.write(f"{finding}\n")