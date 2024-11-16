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