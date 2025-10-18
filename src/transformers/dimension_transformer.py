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
        """Transform circuits data for dim_circuit table (AUTO_INCREMENT keys).
        
        Args:
            circuits_df: Raw circuits DataFrame from CSV
            
        Returns:
            Tuple of (transformed_dataframe_for_loading, original_id_mapping)
        """
        self.logger.info("Transforming circuits data for AUTO_INCREMENT dimensional model")
        
        # Create a copy to avoid modifying the original
        df = circuits_df.copy()
        
        # Keep original circuitId for mapping, but don't include in final table
        # Standardize column names to match dimensional model (exclude circuit_key)
        df = df.rename(columns={
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
        
        # Select columns for dimensional model (NO circuit_key - AUTO_INCREMENT will handle it)
        # Keep circuitId temporarily for mapping creation
        df_for_loading = df[['name', 'city', 'country', 'latitude', 'longitude', 'altitude_m']].copy()
        
        self.logger.info(f"Transformed {len(df_for_loading)} circuit records for AUTO_INCREMENT loading")
        return df_for_loading, df[['circuitId']].copy()
    
    def transform_constructors(self, constructors_df: pd.DataFrame) -> pd.DataFrame:
        """Transform constructors data for dim_constructor table (AUTO_INCREMENT keys).
        
        Args:
            constructors_df: Raw constructors DataFrame from CSV
            
        Returns:
            Tuple of (transformed_dataframe_for_loading, original_id_mapping)
        """
        self.logger.info("Transforming constructors data for AUTO_INCREMENT dimensional model")
        
        # Create a copy to avoid modifying the original
        df = constructors_df.copy()
        
        # Clean and standardize data (keep constructorId for mapping)
        df['name'] = df['name'].str.strip()
        df['nationality'] = df['nationality'].str.strip().fillna('Unknown')
        
        # Select columns for dimensional model (NO constructor_key - AUTO_INCREMENT will handle it)
        df_for_loading = df[['name', 'nationality']].copy()
        
        self.logger.info(f"Transformed {len(df_for_loading)} constructor records for AUTO_INCREMENT loading")
        return df_for_loading, df[['constructorId']].copy()
    
    def transform_drivers(self, drivers_df: pd.DataFrame) -> pd.DataFrame:
        """Transform drivers data for dim_driver table (AUTO_INCREMENT keys).
        
        Args:
            drivers_df: Raw drivers DataFrame from CSV
            
        Returns:
            Tuple of (transformed_dataframe_for_loading, original_id_mapping)
        """
        self.logger.info("Transforming drivers data for AUTO_INCREMENT dimensional model")
        
        # Create a copy to avoid modifying the original
        df = drivers_df.copy()
        
        # Combine forename and surname into name
        df['name'] = (df['forename'].fillna('') + ' ' + df['surname'].fillna('')).str.strip()
        
        # Clean and standardize data (keep driverId for mapping)
        df['name'] = df['name'].str.strip()
        df['nationality'] = df['nationality'].str.strip().fillna('Unknown')
        
        # Handle birthdate - convert to proper date format
        df['birthdate'] = pd.to_datetime(df['dob'], errors='coerce')
        
        # Extract country from nationality (simplified mapping)
        df['country'] = df['nationality'].apply(self._map_nationality_to_country)
        
        # Select columns for dimensional model (NO driver_key - AUTO_INCREMENT will handle it)
        df_for_loading = df[['name', 'nationality', 'birthdate', 'country']].copy()
        
        self.logger.info(f"Transformed {len(df_for_loading)} driver records for AUTO_INCREMENT loading")
        return df_for_loading, df[['driverId']].copy()
    
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
    
    def create_date_dimension_for_schema(self, races_df: pd.DataFrame, schema_type: str = 'qualifying') -> pd.DataFrame:
        """Create date dimension based on schema type.
        
        Args:
            races_df: DataFrame with race information
            schema_type: Type of schema ('qualifying', 'pit_stop', 'race_results')
            
        Returns:
            DataFrame formatted for dim_date table
        """
        self.logger.info(f"Creating date dimension for {schema_type} schema from {len(races_df)} race records")
        
        races_df = races_df.copy()
        races_df['race_date'] = pd.to_datetime(races_df['date'], errors='coerce')
        
        if schema_type == 'qualifying':
            # For qualifying: date = race_date - 1 day
            races_df['target_date'] = races_df['race_date'] - timedelta(days=1)
        else:
            # For pit_stop and race_results: date = race_date
            races_df['target_date'] = races_df['race_date']
        
        # Create date keys and extract date components
        races_df['date_key'] = races_df['target_date'].dt.strftime('%Y%m%d').astype(int)
        races_df['year'] = races_df['target_date'].dt.year
        races_df['month'] = races_df['target_date'].dt.month
        races_df['day_of_month'] = races_df['target_date'].dt.day
        races_df['day_name'] = races_df['target_date'].dt.day_name()
        races_df['day_of_week'] = races_df['target_date'].dt.dayofweek + 1  # 1=Monday
        
        # Add time buckets
        def get_time_bucket(date):
            if schema_type == 'qualifying':
                return 'afternoon'  # Qualifying typically in afternoon
            else:
                return 'afternoon'  # Race sessions typically in afternoon
        
        races_df['time_bucket'] = races_df['target_date'].apply(get_time_bucket)
        
        # Select and prepare final columns
        date_dimension = races_df[[
            'date_key', 'year', 'month', 'day_of_month', 
            'day_name', 'day_of_week', 'time_bucket'
        ]].copy()
        
        # Remove duplicates
        initial_count = len(date_dimension)
        date_dimension = date_dimension.drop_duplicates(subset=['date_key'])
        final_count = len(date_dimension)
        
        if initial_count != final_count:
            self.logger.info(f"Removed {initial_count - final_count} duplicate date entries")
        
        date_dimension = date_dimension.sort_values('date_key')
        
        min_year = date_dimension['year'].min()
        max_year = date_dimension['year'].max()
        self.logger.info(f"Date dimension covers {min_year} to {max_year} ({final_count} unique dates)")
        
        return date_dimension
    
    def transform_races(self, races_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Transform races data for dim_race table (AUTO_INCREMENT keys).
        
        Args:
            races_df: Raw races DataFrame from CSV
            
        Returns:
            Tuple of (transformed_dataframe_for_loading, original_id_mapping)
        """
        self.logger.info("Transforming races data for AUTO_INCREMENT dimensional model")
        
        # Create a copy to avoid modifying the original
        df = races_df.copy()
        
        # Convert race date to proper date format
        df['race_date'] = pd.to_datetime(df['date'], errors='coerce')
        df['race_date_key'] = df['race_date'].dt.strftime('%Y%m%d').astype(int)
        
        # Handle start time - convert to HHMMSS format as integer
        def convert_start_time(time_str):
            if pd.isna(time_str) or time_str == '':
                return None
            try:
                # Expected format: "HH:MM:SS"
                time_parts = str(time_str).strip().split(':')
                if len(time_parts) == 3:
                    hours = int(time_parts[0])
                    minutes = int(time_parts[1]) 
                    seconds = int(time_parts[2])
                    return hours * 10000 + minutes * 100 + seconds
                return None
            except (ValueError, AttributeError):
                return None
        
        df['start_time_key'] = df['time'].apply(convert_start_time)
        
        # Clean and standardize data
        df['name'] = df['name'].str.strip()
        df['year'] = df['year'].astype(int)
        df['round'] = df['round'].astype(int)
        
        # Select columns for dimensional model (NO race_key - AUTO_INCREMENT will handle it)
        df_for_loading = df[['year', 'round', 'name', 'race_date_key', 'start_time_key']].copy()
        
        self.logger.info(f"Transformed {len(df_for_loading)} race records for AUTO_INCREMENT loading")
        return df_for_loading, df[['raceId']].copy()

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
