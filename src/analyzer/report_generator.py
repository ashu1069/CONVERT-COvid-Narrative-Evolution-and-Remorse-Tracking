from typing import Dict, List
from collections import defaultdict

class ReportGenerator:
    """Handles generation of analysis reports"""
    
    def generate_analysis_report(self, results: List[Dict], full_df) -> Dict:
        """Generate comprehensive analysis report"""
        if not results:
            return {"error": "No bias remorse cases detected"}
        
        report = self._compile_report(results, full_df)
        report['key_findings'] = self._extract_key_findings(report)
        
        return report

    def _compile_report(self, results: List[Dict], full_df) -> Dict:
        """Compile all analysis components into a report"""
        report = {
            'summary': {
                'total_comments_analyzed': len(full_df),
                'remorse_cases': len(results),
                'remorse_rate': (len(results) / len(full_df)) * 100
            },
            'channel_analysis': self._get_channel_analysis(results, full_df),
            'temporal_analysis': self._get_temporal_distribution(results),
            'remorse_types': self._get_remorse_types(results),
            'catalysts': self._get_catalyst_analysis(results),
            'political_distribution': self._get_political_distribution(results),
            'engagement_metrics': self._get_engagement_metrics(results),
            'edit_patterns': self._get_edit_patterns(results)
        }
        return report

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
        
        # Add other sections...
        # [Additional sections similar to your example output]
        
        return findings

    def _get_channel_analysis(self, results: List[Dict], full_df) -> Dict:
        """Analyze patterns by channel"""
        channel_stats = defaultdict(lambda: {
            'remorse_count': 0,
            'total_comments': 0,
            'remorse_rate': 0,
            'avg_confidence': 0
        })
        
        # Count total comments per channel
        channel_totals = full_df['channel'].value_counts().to_dict()
        
        for case in results:
            channel = case.get('channel', 'unknown')
            channel_stats[channel]['remorse_count'] += 1
            channel_stats[channel]['avg_confidence'] += case.get('confidence_score', 0)
        
        # Calculate statistics
        for channel, stats in channel_stats.items():
            total = channel_totals.get(channel, 0)
            stats['total_comments'] = total
            stats['remorse_rate'] = (stats['remorse_count'] / total * 100) if total > 0 else 0
            stats['avg_confidence'] /= stats['remorse_count'] if stats['remorse_count'] > 0 else 1
        
        return dict(channel_stats)

    def _get_temporal_distribution(self, results: List[Dict]) -> Dict:
        """Analyze temporal distribution of cases"""
        temporal = defaultdict(lambda: {
            'count': 0,
            'types': defaultdict(int),
            'catalysts': defaultdict(int)
        })
        
        for case in results:
            month = case['timestamp'].strftime('%Y-%m')
            temporal[month]['count'] += 1
            temporal[month]['types'][case.get('remorse_type', 'unknown')] += 1
            if case.get('catalyst'):
                temporal[month]['catalysts'][case['catalyst']] += 1
        
        return dict(temporal)

    def _get_remorse_types(self, results: List[Dict]) -> Dict:
        """Analyze remorse types"""
        remorse_types = defaultdict(int)
        type_details = defaultdict(lambda: {
            'count': 0,
            'avg_confidence': 0.0,
            'catalysts': defaultdict(int),
            'political_distribution': defaultdict(int)
        })
        
        for case in results:
            remorse_type = case.get('remorse_type', 'unknown')
            remorse_types[remorse_type] += 1
            
            # Update detailed statistics
            type_details[remorse_type]['count'] += 1
            type_details[remorse_type]['avg_confidence'] += case.get('confidence_score', 0)
            
            if case.get('catalyst'):
                type_details[remorse_type]['catalysts'][case['catalyst']] += 1
            
            if case.get('political_lean'):
                type_details[remorse_type]['political_distribution'][case['political_lean']] += 1
        
        # Calculate averages
        for rtype in type_details:
            if type_details[rtype]['count'] > 0:
                type_details[rtype]['avg_confidence'] /= type_details[rtype]['count']
        
        return {
            'distribution': dict(remorse_types),
            'details': dict(type_details)
        }

    def _get_catalyst_analysis(self, results: List[Dict]) -> Dict:
        """Analyze catalysts"""
        catalysts = defaultdict(lambda: {
            'count': 0,
            'remorse_types': defaultdict(int),
            'confidence_scores': []
        })
        
        for case in results:
            if case.get('catalyst'):
                catalysts[case['catalyst']]['count'] += 1
                catalysts[case['catalyst']]['remorse_types'][case.get('remorse_type', 'unknown')] += 1
                catalysts[case['catalyst']]['confidence_scores'].append(case.get('confidence_score', 0))
        
        return dict(catalysts)

    def _get_political_distribution(self, results: List[Dict]) -> Dict:
        """Analyze political distribution"""
        distribution = defaultdict(int)
        by_channel = defaultdict(lambda: defaultdict(int))
        
        for case in results:
            political_lean = case.get('political_lean', 'unknown')
            distribution[political_lean] += 1
            by_channel[case.get('channel', 'unknown')][political_lean] += 1
        
        return {
            'overall': dict(distribution),
            'by_channel': dict(by_channel)
        }

    def _get_engagement_metrics(self, results: List[Dict]) -> Dict:
        """Analyze engagement metrics"""
        engagement = {
            'avg_engagement': sum(case.get('engagement_score', 0) for case in results) / len(results),
            'by_type': defaultdict(list),
            'by_channel': defaultdict(list)
        }
        
        for case in results:
            engagement['by_type'][case.get('remorse_type', 'unknown')].append(case.get('engagement_score', 0))
            engagement['by_channel'][case.get('channel', 'unknown')].append(case.get('engagement_score', 0))
        
        return engagement

    def _get_edit_patterns(self, results: List[Dict]) -> Dict:
        """Analyze edit patterns"""
        return {
            'edit_rate': sum(1 for case in results if case.get('has_edit', False)) / len(results),
            'by_type': defaultdict(lambda: {'edited': 0, 'total': 0}),
            'by_channel': defaultdict(lambda: {'edited': 0, 'total': 0})
        }