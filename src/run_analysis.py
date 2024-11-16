import pandas as pd
from analyzer.bias_remorse import VaccineBiasRemorseAnalyzer
import logging
from pathlib import Path

def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('analysis.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_dataset(folder_path: Path) -> dict[str, pd.DataFrame]:
    """Load datasets from multiple channel folders"""
    logger = logging.getLogger(__name__)
    logger.info(f"Loading datasets from {folder_path}")
    
    channel_data = {}
    channels = ['CNN', 'FoxNews', 'MSNBC']
    
    try:
        for channel in channels:
            channel_folder = folder_path / f"{channel}" / f"extracted_text_{channel}"
            
            if not channel_folder.exists():
                logger.warning(f"Channel folder not found: {channel_folder}")
                continue
                
            csv_files = list(channel_folder.glob('*.csv'))
            if not csv_files:
                logger.warning(f"No CSV files found in {channel_folder}")
                continue
                
            logger.info(f"Found {len(csv_files)} CSV files for {channel}")
            
            # Load each CSV file with error handling
            dfs = []
            for f in csv_files:
                try:
                    df = pd.read_csv(
                        f,
                        encoding='utf-8',
                        on_bad_lines='skip',
                        engine='python'
                    )
                    if not df.empty:
                        dfs.append(df)
                    else:
                        logger.warning(f"Empty dataframe from {f}")
                except Exception as e:
                    logger.error(f"Error reading {f}: {str(e)}")
            
            if not dfs:
                logger.warning(f"No valid data loaded for {channel}")
                continue
            
            # Combine all CSV files for the channel
            channel_df = pd.concat(dfs, ignore_index=True)
            
            # Convert timestamp column to datetime
            if 'publishedAt' in channel_df.columns:
                channel_df['publishedAt'] = pd.to_datetime(channel_df['publishedAt'])
            
            channel_data[channel] = channel_df
            logger.info(f"Loaded {len(channel_df)} records for {channel}")
        
        if not channel_data:
            raise ValueError("No data could be loaded for any channel")
            
        return channel_data
    
    except Exception as e:
        logger.error(f"Error loading datasets: {str(e)}")
        raise

def main():
    # Setup logging
    logger = setup_logging()
    
    try:
        # Initialize analyzer
        analyzer = VaccineBiasRemorseAnalyzer()
        
        # Load datasets from each channel
        data_path = Path("DSCI789_data")  # Parent folder containing channel subfolders
        channel_data = load_dataset(data_path)
        
        # Analyze each channel separately
        for channel, df in channel_data.items():
            logger.info(f"Starting analysis for {channel}...")
            results = analyzer.analyze_dataset(df)
            
            # Print channel-specific summary statistics
            print(f"\nAnalysis Summary - {channel}")
            print("-" * 50)
            print(f"Total comments analyzed: {results['summary']['total_comments_analyzed']:,}")
            print(f"Remorse cases identified: {results['summary']['remorse_cases']:,}")
            print(f"Remorse rate: {results['summary']['remorse_rate']:.2f}%")
            print()
        
        logger.info("Analysis completed successfully for all channels")
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()