import logging
import sys
from pathlib import Path
from datetime import datetime

def setup_logging(results_dir: str = "results") -> logging.Logger:
    """
    Configure logging to both console and file
    
    Parameters:
    results_dir (str): Directory to store log files
    
    Returns:
    logging.Logger: Configured logger instance
    """
    # Create results directory if it doesn't exist
    Path(results_dir).mkdir(exist_ok=True)
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'{results_dir}/analysis_{timestamp}.log'
    
    # Configure logging
    logger = logging.getLogger('vaccine_bias_remorse')
    logger.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger