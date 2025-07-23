import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    # Base paths using environment variables
    PROJECT_ROOT = Path(os.getenv('PROJECT_ROOT'))
    DATA_DIR = Path(os.getenv('DATA_DIR'))
    RAW_DIR = Path(os.getenv('RAW_DIR'))
    PROCESSED_DIR = Path(os.getenv('PROCESSED_DIR'))
    
    # Processed data subdirectories
    ATTRIBUTION_RESULTS_DIR = PROCESSED_DIR / 'attribution_results'
    SUMMARY_STATS_DIR = PROCESSED_DIR / 'summary_stats'
    
    @classmethod
    def ensure_directories(cls):
        """Create necessary directories if they don't exist."""
        directories = [
            cls.DATA_DIR,
            cls.RAW_DIR,
            cls.PROCESSED_DIR,
            cls.ATTRIBUTION_RESULTS_DIR,
            cls.SUMMARY_STATS_DIR
        ]
        for dir_path in directories:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_raw_data_path(cls, filename):
        """Get path to raw data file."""
        return cls.RAW_DIR / filename
    
    @classmethod
    def get_attribution_results_path(cls, model_type, timestamp=None):
        """
        Get path for attribution results file.
        
        Args:
            model_type (str): Type of attribution model (e.g., 'single_touch', 'multi_touch')
            timestamp (str, optional): Timestamp for file versioning
        """
        if timestamp is None:
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        filename = f'{model_type}_results_{timestamp}.csv'
        return cls.ATTRIBUTION_RESULTS_DIR / filename
    
    
# Create a config instance to be imported by other modules
config = Config()