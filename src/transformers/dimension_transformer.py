"""Data transformation module for dimensional tables in the F1 Qualifying ETL Pipeline."""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np

from ..config.settings import settings
from ..utils.logging_config import get_logger, ETLProgressLogger


class DimensionTransformer:
    """Handles transformation of data for F1 dimensional tables."""
    
    def __init__(self):
        """Initialize the dimension transformer."""
        self.logger = get_logger(__name__)
        self.progress_logger = ETLProgressLogger(self.logger)
    
    def transform_circuits(self, circuits_df: pd.DataFrame) -> pd.DataFrame:
        """Transform circuits data for dim_circuit table.
        
        Args:
            circuits_df: Raw circuits DataFrame from CSV
            
        Returns:
            Transformed DataFrame ready for dim_circuit table
        """
        self.logger.info("Transforming circuits data for dimensional model")
        
        # Create a copy to avoid modifying the original
        df = circuits_df.copy()
        
        # Standardize column names to match dimensional model
        df = df.rename(columns={
            'circuitId': 'circuit_key',
            'name': 'name',
            'location': 'city',  # location maps to city in schema
            'country': 'country',
            'lat': 'latitude',
            'lng': 'longitude',
            'alt': 'altitude_m'
        })
        
        # Clean and standardize data
        df['name'] = df['name'].str.strip()
        df['city'] = df['city'].str.strip()
        df['country'] = df['country'].str.strip()
        
        # Handle missing coordinates and altitude
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['altitude_m'] = pd.to_numeric(df['altitude_m'], errors='coerce').fillna(0).astype(int)
        
        # Add "unknown" circuit as a fallback for missing circuit references
        unknown_circuit = pd.DataFrame({
            'circuit_key': [999],
            'name': ['Unknown Circuit'],
            'city': ['Unknown'],
            'country': ['Unknown'],
            'latitude': [0.0],
            'longitude': [0.0],
            'altitude_m': [0]
        })
        
        # Concatenate unknown circuit at the end (high key value)
        df = pd.concat([df, unknown_circuit], ignore_index=True)
        
        # Select and order columns for dimensional model
        df = df[['circuit_key', 'name', 'city', 'country', 'latitude', 'longitude', 'altitude_m']]
        
        self.logger.info(f"Transformed {len(df)} circuit records (including 1 unknown circuit)")
        return df
    
    def transform_constructors(self, constructors_df: pd.DataFrame) -> pd.DataFrame:
        """Transform constructors data for dim_constructor table.
        
        Args:
            constructors_df: Raw constructors DataFrame from CSV
            
        Returns:
            Transformed DataFrame ready for dim_constructor table
        """
        self.logger.info("Transforming constructors data for dimensional model")
        
        # Create a copy to avoid modifying the original
        df = constructors_df.copy()
        
        # Standardize column names to match dimensional model
        df = df.rename(columns={
            'constructorId': 'constructor_key',
            'name': 'name',
            'nationality': 'nationality'
        })
        
        # Clean and standardize data
        df['name'] = df['name'].str.strip()
        df['nationality'] = df['nationality'].str.strip().fillna('Unknown')
        
        # Select and order columns for dimensional model
        df = df[['constructor_key', 'name', 'nationality']]
        
        self.logger.info(f"Transformed {len(df)} constructor records")
        return df
    
    def transform_drivers(self, drivers_df: pd.DataFrame) -> pd.DataFrame:
        """Transform drivers data for dim_driver table.
        
        Args:
            drivers_df: Raw drivers DataFrame from CSV
            
        Returns:
            Transformed DataFrame ready for dim_driver table
        """
        self.logger.info("Transforming drivers data for dimensional model")
        
        # Create a copy to avoid modifying the original
        df = drivers_df.copy()
        
        # Standardize column names to match dimensional model
        df = df.rename(columns={
            'driverId': 'driver_key',
            'nationality': 'nationality',
            'dob': 'birthdate'
        })
        
        # Combine forename and surname into name
        df['name'] = (df['forename'].fillna('') + ' ' + df['surname'].fillna('')).str.strip()
        
        # Clean and standardize data
        df['name'] = df['name'].str.strip()
        df['nationality'] = df['nationality'].str.strip().fillna('Unknown')
        
        # Handle birthdate - convert to proper date format
        df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce')
        
        # Extract country from nationality (simplified mapping)
        df['country'] = df['nationality'].apply(self._map_nationality_to_country)
        
        # Select and order columns for dimensional model
        df = df[['driver_key', 'name', 'nationality', 'birthdate', 'country']]
        
        self.logger.info(f"Transformed {len(df)} driver records")
        return df
    
    def create_date_dimension(self, races_df: pd.DataFrame) -> pd.DataFrame:
        """Create date dimension table based on race dates (qualifying = race date - 1).
        
        Args:
            races_df: DataFrame with race information including dates
            
        Returns:
            DataFrame ready for dim_date table
        """
        self.logger.info("Creating date dimension from race dates (qualifying = race date - 1)")
        
        # Convert race dates to datetime
        races_df['race_date'] = pd.to_datetime(races_df['date'], errors='coerce')
        
        # Calculate qualifying dates (race date - 1 day)
        races_df['qualifying_date'] = races_df['race_date'] - timedelta(days=1)
        
        # Create unique dates from qualifying dates
        unique_dates = races_df['qualifying_date'].dropna().unique()
        dates = pd.to_datetime(unique_dates)
        
        # Create DataFrame
        df = pd.DataFrame({'date': dates})
        
        # Add date key as integer (YYYYMMDD format)
        df['date_key'] = df['date'].dt.strftime('%Y%m%d').astype(int)
        
        # Add date attributes
        df['year'] = df['date'].dt.year.astype(int)
        df['month'] = df['date'].dt.month.astype(int)
        df['day_of_month'] = df['date'].dt.day.astype(int)
        df['day_name'] = df['date'].dt.day_name()
        df['day_of_week'] = df['date'].dt.dayofweek + 1  # 1 = Monday, 7 = Sunday
        
        # Add time bucket based on typical qualifying session times
        # Most qualifying sessions are in afternoon/evening
        df['time_bucket'] = df['date'].apply(self._determine_time_bucket)
        
        # Convert date to string format for database storage
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # Reorder columns to match database schema
        df = df[['date_key', 'year', 'month', 'day_of_month', 'day_name', 'day_of_week', 'time_bucket']]
        
        # Sort by date_key for consistent ordering
        df = df.sort_values('date_key').reset_index(drop=True)
        
        self.logger.info(f"Created {len(df)} date dimension records from qualifying dates")
        return df
    
    def _map_nationality_to_country(self, nationality: str) -> str:
        """Map nationality to country name (simplified implementation).
        
        Args:
            nationality: Driver nationality
            
        Returns:
            Country name
        """
        if pd.isna(nationality):
            return 'Unknown'
        
        # Simplified mapping 
        nationality_mapping = {
            'British': 'United Kingdom',
            'German': 'Germany',
            'Spanish': 'Spain',
            'Italian': 'Italy',
            'French': 'France',
            'Brazilian': 'Brazil',
            'Finnish': 'Finland',
            'Austrian': 'Austria',
            'Dutch': 'Netherlands',
            'Australian': 'Australia',
            'Canadian': 'Canada',
            'American': 'United States',
            'Mexican': 'Mexico',
            'Japanese': 'Japan',
            'Polish': 'Poland',
            'Russian': 'Russia',
            'Belgian': 'Belgium',
            'Swiss': 'Switzerland',
            'Danish': 'Denmark',
            'Swedish': 'Sweden',
            'Thai': 'Thailand',
            'Chinese': 'China',
            'Indian': 'India',
            'Venezuelan': 'Venezuela',
            'Colombian': 'Colombia',
            'Argentine': 'Argentina',
            'Argentinian': 'Argentina',
            'Chilean': 'Chile',
            'Monegasque': 'Monaco',
            'New Zealander': 'New Zealand',
            'South African': 'South Africa',
            'Portuguese': 'Portugal',
            'Irish': 'Ireland',
            'Malaysian': 'Malaysia',
            'Indonesian': 'Indonesia'
        }
        
        return nationality_mapping.get(str(nationality).strip(), str(nationality).strip())
    
    def _determine_time_bucket(self, date: pd.Timestamp) -> str:
        """Determine time bucket for qualifying sessions.
        
        Most F1 qualifying sessions happen in afternoon/evening.
        This is a simplified implementation.
        
        Args:
            date: Date of qualifying session
            
        Returns:
            Time bucket category
        """
        # Simplified logic - most qualifying sessions are in afternoon
        # In practice, this could be enhanced with actual session times
        day_of_week = date.dayofweek
        
        if day_of_week in [5, 6]:  # Saturday, Sunday
            return 'afternoon'  # Weekend sessions typically afternoon
        else:
            return 'evening'    # Weekday sessions typically evening
    
    def get_dimension_summary(self, dimensions: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, any]]:
        """Generate summary statistics for all dimension tables.
        
        Args:
            dimensions: Dictionary of dimension DataFrames
            
        Returns:
            Summary statistics for each dimension
        """
        self.logger.info("Generating dimension transformation summary")
        
        summary = {}
        
        for dim_name, dim_df in dimensions.items():
            summary[dim_name] = {
                'record_count': len(dim_df),
                'columns': list(dim_df.columns),
                'null_counts': dim_df.isnull().sum().to_dict(),
                'duplicate_count': dim_df.duplicated().sum()
            }
            
            # Add dimension-specific statistics
            if dim_name == 'circuits':
                summary[dim_name]['unique_countries'] = dim_df['country'].nunique()
            elif dim_name == 'constructors':
                summary[dim_name]['unique_nationalities'] = dim_df['nationality'].nunique()
            elif dim_name == 'drivers':
                summary[dim_name]['unique_nationalities'] = dim_df['nationality'].nunique()
                summary[dim_name]['unique_countries'] = dim_df['country'].nunique()
            elif dim_name == 'dates':
                summary[dim_name]['date_range'] = {
                    'min': dim_df['date_key'].min(),
                    'max': dim_df['date_key'].max()
                }
                summary[dim_name]['time_buckets'] = dim_df['time_bucket'].value_counts().to_dict()
        
        return summary