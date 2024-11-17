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
    """Load datasets from multiple channel folders (limited to 100 files per channel)"""
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
                
            # Get all CSV files and sort them (for consistency)
            all_csv_files = sorted(channel_folder.glob('*.csv'))
            csv_files = all_csv_files[:100]  # Limit to first 100 files
            
            if not csv_files:
                logger.warning(f"No CSV files found in {channel_folder}")
                continue
                
            logger.info(f"Found {len(all_csv_files)} total CSV files for {channel}")
            logger.info(f"Processing {len(csv_files)} CSV files for {channel}")
            
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
        data_path = Path("DSCI789_data")
        channel_data = load_dataset(data_path)
        
        # Analyze each channel separately
        for channel, df in channel_data.items():
            logger.info(f"Starting analysis for {channel}...")
            
            df['channel'] = channel
            results = analyzer.analyze_dataset(df)
            
            # Print ALL analysis results
            print(f"\nDetailed Analysis Results - {channel}")
            print("=" * 60)
            
            # Print summary statistics
            print("\nSummary Statistics:")
            print("-" * 20)
            print(f"Total comments analyzed: {results['summary']['total_comments_analyzed']:,}")
            print(f"Remorse cases identified: {results['summary']['remorse_cases']:,}")
            print(f"Remorse rate: {results['summary']['remorse_rate']:.2f}%")
            
            # Print temporal analysis
            print("\nTemporal Analysis:")
            print("-" * 20)
            for period, stats in results['temporal_analysis'].items():
                print(f"\nPeriod: {period}")
                print(f"Total comments: {stats.get('total_comments', 'N/A'):,}")
                print(f"Remorse cases: {stats.get('remorse_cases', 'N/A'):,}")
                print(f"Remorse rate: {stats.get('remorse_rate', 'N/A'):.2f}%" if isinstance(stats.get('remorse_rate'), (int, float)) else "Remorse rate: N/A")
            
            # Print sentiment analysis
            print("\nSentiment Analysis:")
            print("-" * 20)
            sentiment_analysis = results.get('sentiment_analysis', {})
            print(f"Average sentiment score: {sentiment_analysis.get('avg_sentiment', 'N/A'):.3f}" if isinstance(sentiment_analysis.get('avg_sentiment'), (int, float)) else "Average sentiment score: N/A")
            print(f"Sentiment distribution: {sentiment_analysis.get('sentiment_distribution', 'N/A')}")
            
            # Print topic analysis if available
            if 'topic_analysis' in results and results['topic_analysis']:
                print("\nTop Topics:")
                print("-" * 20)
                for topic, freq in results['topic_analysis'].items():
                    print(f"{topic}: {freq:.2f}%" if isinstance(freq, (int, float)) else f"{topic}: N/A")
            
            print("\n" + "=" * 60 + "\n")
        
        logger.info("Analysis completed successfully for all channels")
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()