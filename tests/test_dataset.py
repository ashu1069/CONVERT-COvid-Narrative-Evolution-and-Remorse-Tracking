import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
from src.data.dataset import VaccinationCommentDataset, create_dataset

@pytest.fixture
def sample_data_folder(tmp_path):
    """Create a temporary folder with sample CSV files for testing"""
    folder = tmp_path / "test_data"
    folder.mkdir()
    
    # Create a sample CSV file
    df = pd.DataFrame({
        'commentId': ['123', '456'],
        'text': ['This vaccine is effective', 'Normal comment'],
        'publishedAt': ['2021-01-01T00:00:00Z', '2021-01-02T00:00:00Z'],
        'updatedAt': ['2021-01-01T00:00:00Z', '2021-01-02T00:00:00Z'],
        'likeCount': [10, 20],
        'totalReplyCount': [5, 8],
        'isPublic': [True, True],
        'source_file': ['cnn_video1.csv', 'fox_video1.csv']
    })
    
    df.to_csv(folder / "test_comments.csv", index=False)
    return folder

def test_dataset_initialization(sample_data_folder):
    """Test dataset initialization"""
    dataset = VaccinationCommentDataset(str(sample_data_folder))
    assert dataset.data_folder == Path(sample_data_folder)
    assert dataset.raw_data is None
    assert dataset.processed_data is None

def test_load_data(sample_data_folder):
    """Test data loading functionality"""
    dataset = VaccinationCommentDataset(str(sample_data_folder))
    csv_file = next(sample_data_folder.glob("*.csv"))
    df = dataset.load_data(str(csv_file))
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert 'commentId' in df.columns

def test_preprocess_data(sample_data_folder):
    """Test data preprocessing"""
    dataset = create_dataset(str(sample_data_folder))
    
    assert dataset.processed_data is not None
    assert 'is_vaccine_related' in dataset.processed_data.columns
    assert dataset.processed_data['is_vaccine_related'].sum() == 1  # Only one vaccine-related comment

def test_get_vaccination_comments(sample_data_folder):
    """Test filtering of vaccination-related comments"""
    dataset = create_dataset(str(sample_data_folder))
    vax_comments = dataset.get_vaccination_comments()
    
    assert len(vax_comments) == 1
    assert 'vaccine' in vax_comments.iloc[0]['cleaned_text']

def test_get_channel_data(sample_data_folder):
    """Test channel-specific data extraction"""
    dataset = create_dataset(str(sample_data_folder))
    cnn_data = dataset.get_channel_data('cnn')
    
    assert len(cnn_data) == 1
    assert 'cnn' in cnn_data.iloc[0]['source_file'].lower()

def test_get_analysis_ready_data(sample_data_folder):
    """Test preparation of analysis-ready dataset"""
    dataset = create_dataset(str(sample_data_folder))
    analysis_data = dataset.get_analysis_ready_data()
    
    required_columns = [
        'commentId', 'cleaned_text', 'channel', 'publishedAt', 
        'updatedAt', 'has_edited', 'days_between_edit', 
        'likeCount', 'totalReplyCount'
    ]
    
    assert all(col in analysis_data.columns for col in required_columns)
    assert len(analysis_data) == 1  # Only vaccine-related comments

def test_create_dataset(sample_data_folder):
    """Test dataset creation helper function"""
    dataset = create_dataset(str(sample_data_folder))
    
    assert dataset.raw_data is not None
    assert dataset.processed_data is not None
    assert len(dataset.raw_data) == 2
    assert len(dataset.processed_data) == 2

def test_invalid_data_folder():
    """Test handling of invalid data folder"""
    with pytest.raises(FileNotFoundError):
        create_dataset("nonexistent_folder")
