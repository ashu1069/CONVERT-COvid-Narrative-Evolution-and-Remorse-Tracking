import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import re
from typing import Dict, List, Optional, Tuple
import logging

class VaccinationCommentDataset:
    def __init__(self, data_folder: str):
        """
        Initialize the dataset handler for vaccination comments analysis
        
        Parameters:
        data_folder (str): Path to folder containing CSV files with YouTube comments
        """
        self.data_folder = Path(data_folder)
        self.raw_data: Optional[pd.DataFrame] = None
        self.processed_data: Optional[pd.DataFrame] = None
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Keywords for vaccination-related content filtering
        self.vaccine_keywords = [
            'vaccine', 'vaccination', 'vaccinated', 'vaccines', 'pfizer', 
            'moderna', 'johnson', 'j&j', 'booster', 'shot', 'dose', 'jab',
            'mrna', 'immunization', 'vaxx', 'antivaxx', 'anti-vaxx'
        ]

    def load_data(self, file: str) -> pd.DataFrame:
        """
        Load data from a CSV file
        """
        try:
            # First load without parsing dates
            df = pd.read_csv(file, low_memory=False, dtype={
                'likeCount': 'Int64',
                'totalReplyCount': 'Int64',
                'isPublic': 'boolean'
            })
            
            # Add source_file column
            df['source_file'] = Path(file).name
            
            # Then parse dates with error handling
            for col in ['publishedAt', 'updatedAt']:
                df[col] = pd.to_datetime(df[col], format='ISO8601', errors='coerce')
            
            # Drop rows where date parsing failed
            df = df.dropna(subset=['publishedAt', 'updatedAt'])
            
            self.logger.info(f"Successfully loaded {file}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading {file}: {str(e)}")
            raise

    def preprocess_data(self) -> pd.DataFrame:
        """
        Preprocess the loaded data for analysis
        """
        if self.raw_data is None:
            raise ValueError("No data loaded. Call load_data() first.")
        
        try:
            df = self.raw_data.copy()
            
            # Convert timestamps to datetime
            for col in ['publishedAt', 'updatedAt']:
                df[col] = pd.to_datetime(df[col], format='ISO8601', errors='coerce')
            
            # Drop rows where datetime conversion failed
            df = df.dropna(subset=['publishedAt', 'updatedAt'])
            
            # Clean text and identify vaccine-related comments
            df['cleaned_text'] = df['text'].apply(self._clean_text)
            df['is_vaccine_related'] = df['cleaned_text'].apply(self._is_vaccine_related)
            
            # Convert numeric columns
            df['likeCount'] = pd.to_numeric(df['likeCount'], errors='coerce')
            df['totalReplyCount'] = pd.to_numeric(df['totalReplyCount'], errors='coerce')
            
            # Convert boolean columns
            df['isPublic'] = df['isPublic'].astype(bool)
            
            return df

        except Exception as e:
            self.logger.error(f"Error in preprocess_data: {str(e)}")
            raise

    def get_vaccination_comments(self) -> pd.DataFrame:
        """
        Return only vaccination-related comments
        """
        if self.processed_data is None:
            raise ValueError("No processed data available. Call preprocess_data() first.")
        
        return self.processed_data[self.processed_data['is_vaccine_related']].copy()

    def get_temporal_splits(self, date_ranges: List[Tuple[str, str]]) -> Dict[str, pd.DataFrame]:
        """
        Split the data into temporal chunks for analysis across different time periods
        
        Parameters:
        date_ranges: List of (start_date, end_date) tuples in 'YYYY-MM-DD' format
        
        Returns:
        Dict mapping period names to corresponding DataFrames
        """
        if self.processed_data is None:
            raise ValueError("No processed data available. Call preprocess_data() first.")
        
        splits = {}
        for i, (start_date, end_date) in enumerate(date_ranges):
            period_name = f"period_{i+1}"
            mask = (
                (self.processed_data['publishedAt'] >= start_date) & 
                (self.processed_data['publishedAt'] < end_date)
            )
            splits[period_name] = self.processed_data[mask].copy()
            
        return splits

    def get_channel_data(self, channel_name: str) -> pd.DataFrame:
        """
        Get comments from a specific channel
        """
        if self.processed_data is None:
            raise ValueError("No processed data available. Call preprocess_data() first.")
            
        # Extract channel name from source file or other metadata
        # Adjust this based on how channel information is stored in your files
        return self.processed_data[
            self.processed_data['source_file'].str.contains(channel_name, case=False)
        ].copy()

    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize comment text
        """
        if pd.isna(text):
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www.\S+', '', text)
        
        # Remove special characters but keep apostrophes for contractions
        text = re.sub(r'[^a-zA-Z0-9\'\s]', ' ', text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text

    def _is_vaccine_related(self, text: str) -> bool:
        """
        Check if comment is related to vaccination
        """
        return any(keyword in text for keyword in self.vaccine_keywords)

    def get_analysis_ready_data(self) -> pd.DataFrame:
        """
        Prepare final dataset for bias remorse analysis
        """
        if self.processed_data is None:
            raise ValueError("No processed data available. Call preprocess_data() first.")
        
        # Get vaccination-related comments
        analysis_df = self.get_vaccination_comments()
        
        # Add additional features useful for remorse analysis
        analysis_df['has_edited'] = analysis_df['publishedAt'] != analysis_df['updatedAt']
        analysis_df['days_between_edit'] = (
            analysis_df['updatedAt'] - analysis_df['publishedAt']
        ).dt.total_seconds() / (24 * 60 * 60)
        
        # Extract channel information from source file
        if 'source_file' in analysis_df.columns:
            analysis_df['channel'] = analysis_df['source_file'].apply(
                lambda x: 'CNN' if 'cnn' in str(x).lower() 
                else 'FOX' if 'fox' in str(x).lower() 
                else 'MSNBC' if 'msnbc' in str(x).lower() 
                else 'Unknown'
            )
        else:
            analysis_df['channel'] = 'Unknown'
        
        # Select relevant columns for analysis
        final_df = analysis_df[[
            'commentId',
            'cleaned_text',
            'channel',
            'publishedAt',
            'updatedAt',
            'has_edited',
            'days_between_edit',
            'likeCount',
            'totalReplyCount'
        ]].copy()
        
        return final_df

def create_dataset(data_folder: str) -> VaccinationCommentDataset:
    """
    Helper function to create and initialize dataset
    """
    dataset = VaccinationCommentDataset(data_folder)
    
    # Load all CSV files from the data folder
    all_data = []
    for csv_file in Path(data_folder).glob('*.csv'):
        df = dataset.load_data(str(csv_file))
        all_data.append(df)
    
    # Combine all dataframes
    dataset.raw_data = pd.concat(all_data, ignore_index=True)
    dataset.logger.info(f"Total records loaded: {len(dataset.raw_data)}")
    
    # Process the combined data
    dataset.processed_data = dataset.preprocess_data()
    return dataset

# Example usage
if __name__ == "__main__":
    # Example code to demonstrate usage
    data_folder = "DSCI789_data/CNN/extracted_text_CNN"
    
    try:
        # Create and load dataset
        dataset = create_dataset(data_folder)
        
        # Get analysis-ready data
        analysis_data = dataset.get_analysis_ready_data()
        
        # Print some basic statistics
        print("\nDataset Statistics:")
        print(f"Total comments: {len(dataset.raw_data)}")
        print(f"Vaccination-related comments: {len(analysis_data)}")
        print("\nComments by channel:")
        print(analysis_data['channel'].value_counts())
        
        # Example of temporal split
        date_ranges = [
            ('2020-01-01', '2020-06-30'),
            ('2020-07-01', '2020-12-31'),
            ('2021-01-01', '2021-06-30')
        ]
        temporal_splits = dataset.get_temporal_splits(date_ranges)
        
        for period, data in temporal_splits.items():
            print(f"\n{period} comment count: {len(data)}")
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")