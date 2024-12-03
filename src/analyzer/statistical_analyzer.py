from collections import defaultdict
from typing import Dict, List
import numpy as np

class StatisticalAnalyzer:
    """Handles statistical analysis of results"""
    
    def analyze_by_channel(self, results: List[Dict], full_df) -> Dict:
        """Analyze remorse patterns by channel"""
        channel_stats = defaultdict(lambda: {
            'count': 0,
            'avg_intensity': 0.0,
            'remorse_types': defaultdict(int)
        })
        
        for result in results:
            channel = result.get('channel', 'unknown')
            channel_stats[channel]['count'] += 1
            channel_stats[channel]['avg_intensity'] += result.get('intensity', 0)
            channel_stats[channel]['remorse_types'][result.get('remorse_type', 'unknown')] += 1
        
        # Calculate averages
        for channel in channel_stats:
            if channel_stats[channel]['count'] > 0:
                channel_stats[channel]['avg_intensity'] /= channel_stats[channel]['count']
        
        return dict(channel_stats)

    def analyze_temporal_patterns(self, results: List[Dict]) -> Dict:
        """Analyze temporal patterns in remorse expressions"""
        temporal_stats = {
            'hourly_distribution': defaultdict(int),
            'daily_distribution': defaultdict(int),
            'monthly_distribution': defaultdict(int),
            'intensity_over_time': []
        }
        
        for result in results:
            timestamp = result.get('timestamp')
            if timestamp:
                temporal_stats['hourly_distribution'][timestamp.hour] += 1
                temporal_stats['daily_distribution'][timestamp.strftime('%A')] += 1
                temporal_stats['monthly_distribution'][timestamp.strftime('%B')] += 1
                temporal_stats['intensity_over_time'].append({
                    'timestamp': timestamp,
                    'intensity': result.get('intensity', 0)
                })
        
        # Sort intensity_over_time by timestamp
        temporal_stats['intensity_over_time'].sort(key=lambda x: x['timestamp'])
        
        return dict(temporal_stats)

    def analyze_remorse_types(self, results: List[Dict]) -> Dict:
        """Analyze distribution of remorse types"""
        remorse_stats = {
            'type_distribution': defaultdict(int),
            'type_intensity': defaultdict(list),
            'co_occurrence': defaultdict(lambda: defaultdict(int))
        }
        
        for result in results:
            remorse_type = result.get('remorse_type', 'unknown')
            intensity = result.get('intensity', 0)
            
            # Update type distribution
            remorse_stats['type_distribution'][remorse_type] += 1
            
            # Update intensity distribution
            remorse_stats['type_intensity'][remorse_type].append(intensity)
            
            # Analyze co-occurring types if present
            secondary_types = result.get('secondary_types', [])
            for secondary_type in secondary_types:
                remorse_stats['co_occurrence'][remorse_type][secondary_type] += 1
        
        # Calculate average intensities
        avg_intensities = {}
        for rtype, intensities in remorse_stats['type_intensity'].items():
            avg_intensities[rtype] = np.mean(intensities) if intensities else 0
        remorse_stats['average_intensities'] = avg_intensities
        
        return dict(remorse_stats)

    def analyze_patterns(self, results: List[Dict]) -> Dict:
        """Comprehensive pattern analysis"""
        analysis = {
            'temporal': self.analyze_temporal_patterns(results),
            'channel': self.analyze_by_channel(results),
            'political': self._analyze_political_distribution(results),
            'catalysts': self._analyze_catalysts(results),
            'engagement': self._analyze_engagement(results)
        }
        return analysis

    def _analyze_political_distribution(self, results: List[Dict]) -> Dict:
        """Analyze political leanings and their correlation with remorse"""
        political_stats = {
            'distribution': defaultdict(int),
            'engagement_by_leaning': defaultdict(list),
            'catalyst_correlation': defaultdict(lambda: defaultdict(int))
        }
        
        for result in results:
            lean = result.get('political_lean')
            if lean:
                political_stats['distribution'][lean] += 1
                political_stats['engagement_by_leaning'][lean].append(
                    result['engagement_metrics']['likes'] + result['engagement_metrics']['replies']
                )
                if result.get('catalyst_details'):
                    political_stats['catalyst_correlation'][lean][result['catalyst_details']['type']] += 1
        
        return dict(political_stats)

    def _analyze_catalysts(self, results: List[Dict]) -> Dict:
        """Analyze catalyst patterns and their impact"""
        catalyst_stats = {
            'types': defaultdict(int),
            'severity_distribution': defaultdict(int),
            'impact_on_engagement': defaultdict(list)
        }
        
        for result in results:
            if details := result.get('catalyst_details'):
                catalyst_stats['types'][details['type']] += 1
                catalyst_stats['severity_distribution'][details['severity']] += 1
                catalyst_stats['impact_on_engagement'][details['type']].append(
                    result['engagement_metrics']['likes'] + result['engagement_metrics']['replies']
                )
        
        return dict(catalyst_stats)

    def _analyze_engagement(self, results: List[Dict]) -> Dict:
        """Analyze engagement patterns"""
        engagement_stats = {
            'edit_patterns': {
                'edited_count': sum(1 for r in results if r.get('has_edit')),
                'total_count': len(results)
            },
            'engagement_by_remorse_type': defaultdict(list),
            'temporal_engagement': defaultdict(list)
        }
        
        for result in results:
            remorse_type = result.get('remorse_type')
            if remorse_type:
                engagement_stats['engagement_by_remorse_type'][remorse_type].append(
                    result['engagement_metrics']['likes'] + result['engagement_metrics']['replies']
                )
            
            # Group engagement by time periods
            if timestamp := result.get('timestamp'):
                hour = timestamp.hour
                engagement_stats['temporal_engagement'][hour].append(
                    result['engagement_metrics']['likes'] + result['engagement_metrics']['replies']
                )
        
        return dict(engagement_stats)