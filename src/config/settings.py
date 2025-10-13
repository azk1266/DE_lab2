"""Configuration settings for the F1 Qualifying ETL Pipeline."""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class Settings:
    """Centralized configuration management for the F1 ETL pipeline."""
    
    def __init__(self):
        """Initialize configuration settings from environment variables."""
        # Database Configuration
        self.DATABASE_HOST = os.getenv('DATABASE_HOST', '127.0.0.1')
        self.DATABASE_PORT = int(os.getenv('DATABASE_PORT', '3306'))
        self.DATABASE_USER = os.getenv('DATABASE_USER', 'azalia2')
        self.DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD', '123456')
        self.DATABASE_NAME = os.getenv('DATABASE_NAME', 'f1_qlf_db')
        
        # Data Source Paths
        self.DATA_PATH = Path(os.getenv('DATA_PATH', 'data/'))
        self.CIRCUITS_FILE = os.getenv('CIRCUITS_FILE', 'circuits.csv')
        self.CONSTRUCTORS_FILE = os.getenv('CONSTRUCTORS_FILE', 'constructors.csv')
        self.DRIVERS_FILE = os.getenv('DRIVERS_FILE', 'drivers.csv')
        self.RACES_FILE = os.getenv('RACES_FILE', 'races.csv')
        self.QUALIFYING_FILE = os.getenv('QUALIFYING_FILE', 'qualifying.csv')
        
        # ETL Configuration
        self.BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10000'))
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        self.LOG_FILE = os.getenv('LOG_FILE', 'logs/etl_pipeline.log')
        self.LOG_DIRECTORY = Path(os.getenv('LOG_DIRECTORY', 'logs'))
        
        # Processing Options
        self.SKIP_DATA_VALIDATION = os.getenv('SKIP_DATA_VALIDATION', 'false').lower() == 'true'
        self.TRUNCATE_TABLES = os.getenv('TRUNCATE_TABLES', 'true').lower() == 'true'
        self.CREATE_INDEXES = os.getenv('CREATE_INDEXES', 'true').lower() == 'true'
    
    @property
    def database_url(self) -> str:
        """Get the complete database URL for SQLAlchemy."""
        return (f"mysql+pymysql://{self.DATABASE_USER}:{self.DATABASE_PASSWORD}"
                f"@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}")
    
    @property
    def circuits_path(self) -> Path:
        """Get the full path to the circuits CSV file."""
        return self.DATA_PATH / self.CIRCUITS_FILE
    
    @property
    def constructors_path(self) -> Path:
        """Get the full path to the constructors CSV file."""
        return self.DATA_PATH / self.CONSTRUCTORS_FILE
    
    @property
    def drivers_path(self) -> Path:
        """Get the full path to the drivers CSV file."""
        return self.DATA_PATH / self.DRIVERS_FILE
    
    @property
    def races_path(self) -> Path:
        """Get the full path to the races CSV file."""
        return self.DATA_PATH / self.RACES_FILE
    
    @property
    def qualifying_path(self) -> Path:
        """Get the full path to the qualifying CSV file."""
        return self.DATA_PATH / self.QUALIFYING_FILE
    
    def validate_file_paths(self) -> list[str]:
        """Validate that all required CSV files exist.
        
        Returns:
            List of missing file paths (empty if all files exist)
        """
        missing_files = []
        
        for file_path in [self.circuits_path, self.constructors_path, self.drivers_path, 
                         self.races_path, self.qualifying_path]:
            if not file_path.exists():
                missing_files.append(str(file_path))
        
        return missing_files
    
    def create_log_directory(self) -> None:
        """Create the log directory if it doesn't exist."""
        log_path = Path(self.LOG_FILE).parent
        log_path.mkdir(parents=True, exist_ok=True)


# Global settings instance
settings = Settings()