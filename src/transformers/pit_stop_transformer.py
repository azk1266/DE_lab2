"""Pit stop fact transformer for the F1 ETL Pipeline."""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
import re

from ..config.settings import settings
from ..utils.logging_config import get_logger, ETLProgressLogger


class PitStopFactTransformer:
    """Handles transformation of data for the pit stop fact table."""
    
    def __init__(self):
        """Initialize the pit stop fact transformer."""
        self.logger = get_logger(__name__)
        self.progress_logger = ETLProgressLogger(self.logger)
    
    def transform_pit_stops(
        self,
        pit_stops_df: pd.DataFrame,
        dim_lookups: Dict[str, Dict[int, int]],
        races_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Transform pit stop data for fact_pit_stop table.
        
        Args:
            pit_stops_df: Raw pit stops DataFrame from CSV
            dim_lookups: Dictionary containing CSV_ID → AUTO_INCREMENT_KEY mappings
            races_df: DataFrame with race information for date mapping
            
        Returns:
            Transformed DataFrame ready for fact_pit_stop table
        """
        self.logger.info(f"Transforming {len(pit_stops_df):,} pit stop records")
        self.progress_logger.start_process("Pit Stop Fact Transformation", 6)
        
        # Step 1: Validate and filter records
        self.progress_logger.log_step("Validating records and filtering invalid data")
        fact_df, skipped_count = self._validate_and_filter_records(
            pit_stops_df, dim_lookups, races_df
        )
        
        if fact_df.empty:
            self.logger.warning("All pit stop records were invalid - returning empty DataFrame")
            return pd.DataFrame()
        
        # Step 2: Add race information and date keys
        self.progress_logger.log_step("Adding race information and date keys")
        fact_df = self._add_race_and_date_info(fact_df, races_df)
        
        # Step 3: Add dimension foreign keys using AUTO_INCREMENT mappings
        self.progress_logger.log_step("Adding AUTO_INCREMENT dimension foreign keys")
        fact_df = self._add_auto_increment_dimension_keys(fact_df, dim_lookups)
        
        # Step 4: Transform pit stop specific measures
        self.progress_logger.log_step("Transforming pit stop measures")
        fact_df = self._transform_pit_stop_measures(fact_df)
        
        # Step 5: Final cleanup and column selection
        self.progress_logger.log_step("Final cleanup and column selection")
        fact_df = self._finalize_pit_stop_fact_data(fact_df)
        
        # Step 6: Data quality validation
        self.progress_logger.log_step("Performing data quality validation")
        fact_df = self._validate_data_quality(fact_df)
        
        self.progress_logger.complete_process("Pit Stop Fact Transformation", len(fact_df))
        self.logger.info(f"Transformation complete: {len(fact_df):,} valid records, {skipped_count:,} skipped")
        return fact_df
    
    def _validate_and_filter_records(
        self,
        pit_stops_df: pd.DataFrame,
        dim_lookups: Dict[str, Dict[int, int]],
        races_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, int]:
        """Validate records and filter out invalid ones.
        
        Args:
            pit_stops_df: Raw pit stops DataFrame
            dim_lookups: Dimension lookup dictionaries
            races_df: Race information DataFrame
            
        Returns:
            Tuple of (filtered_dataframe, skipped_count)
        """
        initial_count = len(pit_stops_df)
        skipped_reasons = {
            'missing_driver': 0,
            'missing_race': 0,
            'invalid_data': 0
        }
        
        # Create lookup sets for validation
        race_lookup = set(races_df['raceId'].values) if 'raceId' in races_df.columns else set()
        driver_lookup = set(dim_lookups.get('drivers', {}).keys())
        
        # Create validation mask
        valid_mask = pd.Series([True] * len(pit_stops_df), index=pit_stops_df.index)
        
        # Check for missing drivers
        if 'driverId' in pit_stops_df.columns:
            missing_driver_mask = ~pit_stops_df['driverId'].isin(driver_lookup)
            skipped_reasons['missing_driver'] = missing_driver_mask.sum()
            valid_mask &= ~missing_driver_mask
        
        # Check for missing races
        if 'raceId' in pit_stops_df.columns:
            missing_race_mask = ~pit_stops_df['raceId'].isin(race_lookup)
            skipped_reasons['missing_race'] = missing_race_mask.sum()
            valid_mask &= ~missing_race_mask
        
        # Check for invalid pit stop data
        required_columns = ['raceId', 'driverId', 'stop', 'lap', 'milliseconds']
        for col in required_columns:
            if col in pit_stops_df.columns:
                invalid_mask = pit_stops_df[col].isnull()
                skipped_reasons['invalid_data'] += invalid_mask.sum()
                valid_mask &= ~invalid_mask
        
        # Filter to valid records only
        df_valid = pit_stops_df[valid_mask].copy()
        skipped_count = initial_count - len(df_valid)
        
        # Log skipping reasons
        if skipped_count > 0:
            self.logger.info(f"Skipped {skipped_count:,} invalid pit stop records:")
            for reason, count in skipped_reasons.items():
                if count > 0:
                    self.logger.info(f"  - {reason}: {count:,} records")
        
        return df_valid, skipped_count
    
    def _add_race_and_date_info(self, pit_stops_df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """Add race information and calculate date keys.
        
        Args:
            pit_stops_df: Pit stops DataFrame
            races_df: Race information DataFrame
            
        Returns:
            DataFrame with race info and date keys
        """
        # Create race lookup with date and circuit information
        race_info = races_df[['raceId', 'date', 'circuitId']].copy()
        race_info['race_date'] = pd.to_datetime(race_info['date'], errors='coerce')
        race_info['date_key'] = race_info['race_date'].dt.strftime('%Y%m%d').astype(int)
        
        # Merge race information
        pit_stops_df = pit_stops_df.merge(
            race_info[['raceId', 'date_key', 'circuitId']], 
            on='raceId', 
            how='left'
        )
        
        return pit_stops_df
    
    def _add_auto_increment_dimension_keys(self, fact_df: pd.DataFrame, dim_lookups: Dict[str, Dict[int, int]]) -> pd.DataFrame:
        """Add foreign keys for dimensional tables using AUTO_INCREMENT mappings.
        
        Args:
            fact_df: Pit stop fact DataFrame
            dim_lookups: Dictionary containing CSV_ID → AUTO_INCREMENT_KEY mappings
            
        Returns:
            DataFrame with AUTO_INCREMENT dimension keys added
        """
        # Race keys - map CSV raceId to AUTO_INCREMENT race_key
        if 'races' in dim_lookups:
            fact_df['dim_race_race_key'] = fact_df['raceId'].map(dim_lookups['races'])
            unmapped_races = fact_df[fact_df['dim_race_race_key'].isna()]
            if len(unmapped_races) > 0:
                self.logger.warning(f"Found {len(unmapped_races)} unmapped races")
                fact_df = fact_df.dropna(subset=['dim_race_race_key'])
            fact_df['dim_race_race_key'] = fact_df['dim_race_race_key'].astype(int)
        
        # Circuit keys - map CSV circuitId to AUTO_INCREMENT circuit_key
        if 'circuits' in dim_lookups:
            fact_df['dim_circuit_circuit_key'] = fact_df['circuitId'].map(dim_lookups['circuits'])
            unmapped_circuits = fact_df[fact_df['dim_circuit_circuit_key'].isna()]
            if len(unmapped_circuits) > 0:
                self.logger.warning(f"Found {len(unmapped_circuits)} unmapped circuits")
                fact_df = fact_df.dropna(subset=['dim_circuit_circuit_key'])
            fact_df['dim_circuit_circuit_key'] = fact_df['dim_circuit_circuit_key'].astype(int)
        
        # Driver keys - map CSV driverId to AUTO_INCREMENT driver_key
        if 'drivers' in dim_lookups:
            fact_df['dim_driver_driver_key'] = fact_df['driverId'].map(dim_lookups['drivers'])
            unmapped_drivers = fact_df[fact_df['dim_driver_driver_key'].isna()]
            if len(unmapped_drivers) > 0:
                self.logger.error(f"Found {len(unmapped_drivers)} unmapped drivers after validation!")
                fact_df = fact_df.dropna(subset=['dim_driver_driver_key'])
            fact_df['dim_driver_driver_key'] = fact_df['dim_driver_driver_key'].astype(int)
        
        # Constructor keys - use fallback approach for pit stops
        self.logger.info("Using fallback constructor mapping for pit stops")
        if 'constructors' in dim_lookups and len(dim_lookups['constructors']) > 0:
            # Use the first available constructor as fallback
            fallback_constructor_key = list(dim_lookups['constructors'].values())[0]
            fact_df['dim_constructor_constructor_key'] = fallback_constructor_key
            self.logger.warning(f"Using fallback constructor key {fallback_constructor_key} for all pit stops")
        else:
            self.logger.error("No constructor lookup available")
            fact_df['dim_constructor_constructor_key'] = 1  # Fallback
        
        # Date keys (use calculated date_key directly)
        fact_df['dim_date_date_key'] = fact_df['date_key'].astype(int)
        
        return fact_df
    
    def _transform_pit_stop_measures(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Transform pit stop specific measures.
        
        Args:
            fact_df: DataFrame with pit stop data
            
        Returns:
            DataFrame with transformed measures
        """
        # Stop number (already exists as 'stop')
        fact_df['stop_number'] = fact_df['stop'].astype(int)
        
        # Lap (already exists)
        fact_df['lap'] = fact_df['lap'].astype(int)
        
        # Duration in milliseconds (rename from 'milliseconds')
        fact_df['duration_ms'] = fact_df['milliseconds'].astype(int)
        
        return fact_df
    
    def _add_business_measures(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Add business measures and indicators.
        
        Args:
            fact_df: DataFrame with basic pit stop data
            
        Returns:
            DataFrame with business measures added
        """
        # Add derived measures if needed
        # For example: pit stop efficiency indicators, etc.
        
        return fact_df
    
    def _finalize_pit_stop_fact_data(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Final cleanup and column selection for pit stop fact table.
        
        Args:
            fact_df: Transformed DataFrame
            
        Returns:
            Final DataFrame ready for database loading
        """
        # Select only the columns needed for the fact table (removed position and lap_time_ms)
        fact_columns = [
            'dim_race_race_key',
            'dim_circuit_circuit_key',
            'dim_constructor_constructor_key',
            'dim_driver_driver_key',
            'dim_date_date_key',
            'stop_number',
            'lap',
            'duration_ms'
        ]
        
        # Ensure all required columns exist
        missing_columns = [col for col in fact_columns if col not in fact_df.columns]
        if missing_columns:
            self.logger.error(f"Missing required fact columns: {missing_columns}")
            raise ValueError(f"Missing required fact columns: {missing_columns}")
        
        # Select columns and ensure proper data types
        df_fact = fact_df[fact_columns].copy()
        
        # Ensure integer types for all columns (no nullable columns anymore)
        for col in fact_columns:
            df_fact[col] = df_fact[col].fillna(0).astype(int)
        
        self.logger.info(f"Finalized {len(df_fact)} pit stop fact records for loading")
        return df_fact
    
    def _validate_data_quality(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Validate data quality and log warnings for unusual values.
        
        Args:
            fact_df: Final fact DataFrame
            
        Returns:
            Validated DataFrame
        """
        # Validate stop numbers
        invalid_stops = fact_df[fact_df['stop_number'] > 10]  # More than 10 stops unusual
        if len(invalid_stops) > 0:
            self.logger.warning(f"Found {len(invalid_stops)} records with stop_number > 10")
        
        # Validate lap numbers
        invalid_laps = fact_df[fact_df['lap'] > 100]  # More than 100 laps unusual
        if len(invalid_laps) > 0:
            self.logger.warning(f"Found {len(invalid_laps)} records with lap > 100")
        
        # Validate pit stop duration (should be reasonable for F1)
        if len(fact_df) > 0:
            min_duration = fact_df['duration_ms'].min()
            max_duration = fact_df['duration_ms'].max()
            
            # Reasonable F1 pit stop times: 2 seconds to 2 minutes
            if min_duration < 2000:  # Less than 2 seconds
                fast_stops = (fact_df['duration_ms'] < 2000).sum()
                self.logger.warning(f"Found {fast_stops} pit stops < 2 seconds")
            
            if max_duration > 120000:  # More than 2 minutes
                slow_stops = (fact_df['duration_ms'] > 120000).sum()
                self.logger.warning(f"Found {slow_stops} pit stops > 2 minutes")
        
        return fact_df
    
    def get_fact_summary(self, fact_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for the pit stop fact table.
        
        Args:
            fact_df: Transformed fact DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        if fact_df.empty:
            return {'total_records': 0, 'note': 'Empty DataFrame'}
        
        summary = {
            'total_records': len(fact_df),
            'stop_statistics': {
                'avg_stops_per_driver': fact_df.groupby('dim_driver_driver_key')['stop_number'].count().mean(),
                'max_stops_in_race': fact_df['stop_number'].max(),
                'avg_duration_ms': fact_df['duration_ms'].mean(),
                'min_duration_ms': fact_df['duration_ms'].min(),
                'max_duration_ms': fact_df['duration_ms'].max()
            },
            'lap_statistics': {
                'avg_lap': fact_df['lap'].mean(),
                'min_lap': fact_df['lap'].min(),
                'max_lap': fact_df['lap'].max()
            },
            'data_quality': {
                'null_counts': fact_df.isnull().sum().to_dict()
            }
        }
        
        return summary
