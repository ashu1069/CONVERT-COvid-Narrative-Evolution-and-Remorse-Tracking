import pytest
import pandas as pd
from datetime import datetime
from src.analyzer.bias_remorse import VaccineBiasRemorseAnalyzer

@pytest.fixture
def analyzer():
    return VaccineBiasRemorseAnalyzer()

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'commentId': ['1', '2', '3'],
        'cleaned_text': [
            "I was wrong about vaccines. I used to be against them but got covid and changed my mind.",
            "Just a regular comment with no remorse.",
            "I regret being anti-vax. After my friend died from covid, I realized the truth."
        ],
        'publishedAt': [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
            datetime(2023, 1, 3)
        ],
        'channel': ['channel1', 'channel2', 'channel1'],
        'engagement_score': [10, 5, 15],
        'has_edited': [True, False, False]
    })

def test_analyze_dataset(analyzer, sample_data):
    result = analyzer.analyze_dataset(sample_data)
    
    # Test basic report structure
    assert 'total_cases' in result
    assert 'detection_rate' in result
    assert 'cases' in result
    assert 'key_findings' in result
    
    # Test detection accuracy
    assert result['total_cases'] == 2  # Should detect 2 remorse cases
    assert len(result['cases']) == 2
    assert result['detection_rate'] == (2/3) * 100

def test_remorse_detection(analyzer, sample_data):
    result = analyzer.analyze_dataset(sample_data)
    cases = result['cases']
    
    # Test first remorse case
    assert cases[0]['comment_id'] == '1'
    assert cases[0]['has_remorse'] == True
    assert cases[0]['channel'] == 'channel1'
    
    # Test non-remorse case
    non_remorse_ids = [case['comment_id'] for case in cases]
    assert '2' not in non_remorse_ids

def test_empty_dataset(analyzer):
    empty_df = pd.DataFrame({
        'commentId': [],
        'cleaned_text': [],
        'publishedAt': [],
        'channel': [],
        'engagement_score': [],
        'has_edited': []
    })
    
    result = analyzer.analyze_dataset(empty_df)
    assert result == {"error": "No bias remorse cases detected"}

def test_statistical_components(analyzer, sample_data):
    result = analyzer.analyze_dataset(sample_data)
    
    # Test severity breakdown
    assert 'severity_breakdown' in result
    
    # Test temporal distribution
    assert 'temporal_distribution' in result
    assert len(result['temporal_distribution']) > 0
    
    # Test statistical summary
    assert 'statistical_summary' in result
    assert 'mean_confidence' in result['statistical_summary']
    assert 'categories' in result['statistical_summary']
