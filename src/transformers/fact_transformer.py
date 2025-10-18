"""Data transformation module for fact tables in the F1 Qualifying ETL Pipeline."""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
import re

from ..config.settings import settings
from ..utils.logging_config import get_logger, ETLProgressLogger


class FactTransformer:
    """Handles transformation of data for the facts table."""
    
    def __init__(self):
        """Initialize the fact transformer."""
        self.logger = get_logger(__name__)
        self.progress_logger = ETLProgressLogger(self.logger)
    
    def transform_qualifying(
        self,
        qualifying_df: pd.DataFrame,
        dim_lookups: Dict[str, Dict[int, int]],
        races_df: pd.DataFrame,
        batch_start_row: int = 1
    ) -> pd.DataFrame:
        """Transform qualifying data for facts table (AUTO_INCREMENT schema).
        
        Args:
            qualifying_df: Raw qualifying DataFrame from CSV
            dim_lookups: Dictionary containing CSV_ID → AUTO_INCREMENT_KEY mappings
            races_df: DataFrame with race information for date mapping
            batch_start_row: Starting row number for this batch (for error logging)
            
        Returns:
            Transformed DataFrame ready for facts table (without qualifying_id)
        """
        self.logger.info(f"Transforming {len(qualifying_df):,} qualifying records for AUTO_INCREMENT fact table")
        self.progress_logger.start_process("Qualifying Fact Transformation", 7)
        
        # Create a copy to avoid modifying the original
        df = qualifying_df.copy()
        
        # Step 1: Validate and filter records (skip invalid ones)
        self.progress_logger.log_step("Validating records and filtering invalid data")
        df, skipped_count = self._validate_and_filter_records(df, dim_lookups, races_df, batch_start_row)
        
        if df.empty:
            self.logger.warning("All records in batch were invalid - returning empty DataFrame")
            return pd.DataFrame()
        
        # Step 2: Add race information and date keys
        self.progress_logger.log_step("Adding race information and date keys")
        df = self._add_race_and_date_info(df, races_df)
        
        # Step 3: Convert qualifying times to milliseconds
        self.progress_logger.log_step("Converting qualifying times to milliseconds")
        df = self._convert_qualifying_times(df)
        
        # Step 4: Derive qualifying status
        self.progress_logger.log_step("Deriving qualifying status")
        df = self._derive_qualifying_status(df)
        
        # Step 5: Add dimension foreign keys using AUTO_INCREMENT mappings
        self.progress_logger.log_step("Adding AUTO_INCREMENT dimension foreign keys")
        df = self._add_auto_increment_dimension_keys(df, dim_lookups)
        
        # Step 6: Add business measures
        self.progress_logger.log_step("Adding business measures and indicators")
        df = self._add_business_measures(df)
        
        # Step 7: Final cleanup and column selection (exclude qualifying_id)
        self.progress_logger.log_step("Final cleanup and column selection for AUTO_INCREMENT")
        df = self._finalize_auto_increment_fact_data(df)
        
        self.progress_logger.complete_process("Qualifying Fact Transformation", len(df))
        self.logger.info(f"Transformation complete: {len(df):,} valid records, {skipped_count:,} skipped")
        return df
    
    def _validate_and_filter_records(
        self,
        df: pd.DataFrame,
        dim_lookups: Dict[str, Dict[int, int]],
        races_df: pd.DataFrame,
        batch_start_row: int
    ) -> Tuple[pd.DataFrame, int]:
        """Validate records and filter out invalid ones (AUTO_INCREMENT version).
        
        Args:
            df: Raw qualifying DataFrame
            dim_lookups: Dimension lookup dictionaries (CSV_ID → AUTO_INCREMENT_KEY)
            races_df: Race information DataFrame
            batch_start_row: Starting row for error logging
            
        Returns:
            Tuple of (filtered_dataframe, skipped_count)
        """
        initial_count = len(df)
        skipped_reasons = {
            'missing_driver': 0,
            'missing_constructor': 0,
            'missing_race': 0,
            'invalid_data': 0
        }
        
        # Create lookup sets for validation (using CSV IDs)
        race_lookup = set(races_df['raceId'].values) if 'raceId' in races_df.columns else set()
        driver_lookup = set(dim_lookups.get('drivers', {}).keys())
        constructor_lookup = set(dim_lookups.get('constructors', {}).keys())
        
        # Create validation mask
        valid_mask = pd.Series([True] * len(df), index=df.index)
        
        # Check for missing drivers (using CSV driverId)
        if 'driverId' in df.columns:
            missing_driver_mask = ~df['driverId'].isin(driver_lookup)
            skipped_reasons['missing_driver'] = missing_driver_mask.sum()
            valid_mask &= ~missing_driver_mask
        
        # Check for missing constructors (using CSV constructorId)
        if 'constructorId' in df.columns:
            missing_constructor_mask = ~df['constructorId'].isin(constructor_lookup)
            skipped_reasons['missing_constructor'] = missing_constructor_mask.sum()
            valid_mask &= ~missing_constructor_mask
        
        # Check for missing races (using CSV raceId)
        if 'raceId' in df.columns:
            missing_race_mask = ~df['raceId'].isin(race_lookup)
            skipped_reasons['missing_race'] = missing_race_mask.sum()
            valid_mask &= ~missing_race_mask
        
        # Check for invalid qualifying IDs
        if 'qualifyId' in df.columns:
            invalid_qualify_mask = df['qualifyId'].isnull()
            skipped_reasons['invalid_data'] = invalid_qualify_mask.sum()
            valid_mask &= ~invalid_qualify_mask
        
        # Filter to valid records only
        df_valid = df[valid_mask].copy()
        skipped_count = initial_count - len(df_valid)
        
        # Log skipping reasons
        if skipped_count > 0:
            self.logger.info(f"Skipped {skipped_count:,} invalid records:")
            for reason, count in skipped_reasons.items():
                if count > 0:
                    self.logger.info(f"  - {reason}: {count:,} records")
        
        return df_valid, skipped_count
    
    def _add_race_and_date_info(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """Add race information and calculate date keys.
        
        Args:
            df: Qualifying DataFrame
            races_df: Race information DataFrame
            
        Returns:
            DataFrame with race info and date keys
        """
        # Create race lookup with date information
        race_info = races_df[['raceId', 'date', 'circuitId']].copy()
        race_info['race_date'] = pd.to_datetime(race_info['date'], errors='coerce')
        race_info['qualifying_date'] = race_info['race_date'] - timedelta(days=1)
        race_info['date_key'] = race_info['qualifying_date'].dt.strftime('%Y%m%d').astype(int)
        
        # Merge race information
        df = df.merge(
            race_info[['raceId', 'date_key', 'circuitId']], 
            on='raceId', 
            how='left'
        )
        
        return df
    
    def _convert_qualifying_times(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert qualifying times from MM:SS.SSS format to milliseconds.
        
        Args:
            df: DataFrame with qualifying time strings
            
        Returns:
            DataFrame with times converted to milliseconds
        """
        time_columns = ['q1', 'q2', 'q3']
        
        for col in time_columns:
            if col in df.columns:
                # Convert to milliseconds
                df[f'{col}_ms'] = df[col].apply(self._time_string_to_milliseconds)
        
        return df
    
    def _time_string_to_milliseconds(self, time_str: str) -> Optional[int]:
        """Convert time string to milliseconds.
        
        Args:
            time_str: Time string in format "M:SS.SSS" or "MM:SS.SSS"
            
        Returns:
            Time in milliseconds, or None if invalid
        """
        if pd.isna(time_str) or time_str == '':
            return None
        
        try:
            # Remove any whitespace
            time_str = str(time_str).strip()
            
            # Pattern for M:SS.SSS or MM:SS.SSS
            pattern = r'^(\d{1,2}):(\d{2})\.(\d{3})$'
            match = re.match(pattern, time_str)
            
            if match:
                minutes = int(match.group(1))
                seconds = int(match.group(2))
                milliseconds = int(match.group(3))
                
                # Convert to total milliseconds
                total_ms = (minutes * 60 + seconds) * 1000 + milliseconds
                return total_ms
            else:
                # Try alternative format without milliseconds (MM:SS)
                pattern_alt = r'^(\d{1,2}):(\d{2})$'
                match_alt = re.match(pattern_alt, time_str)
                if match_alt:
                    minutes = int(match_alt.group(1))
                    seconds = int(match_alt.group(2))
                    total_ms = (minutes * 60 + seconds) * 1000
                    return total_ms
                
                return None
                
        except (ValueError, AttributeError):
            return None
    
    def _derive_qualifying_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """Derive qualifying status based on available times.
        
        Status logic:
        - 'OK': Has times in all qualifying rounds (Q1, Q2, Q3)
        - 'DNQ': Has Q1 but no Q2 (did not qualify for Q2)
        - 'DNS': No times recorded (did not start)
        - 'DSQ': Disqualified or invalid times
        
        Args:
            df: DataFrame with qualifying times
            
        Returns:
            DataFrame with status column added
        """
        def determine_status(row):
            q1_time = row.get('q1_ms')
            q2_time = row.get('q2_ms')
            q3_time = row.get('q3_ms')
            
            # Check if times are valid (not None and > 0)
            has_q1 = pd.notna(q1_time) and q1_time > 0
            has_q2 = pd.notna(q2_time) and q2_time > 0
            has_q3 = pd.notna(q3_time) and q3_time > 0
            
            if has_q1 and has_q2 and has_q3:
                return 'OK'
            elif has_q1 and not has_q2:
                return 'DNQ'
            elif not has_q1:
                return 'DNS'
            else:
                return 'DSQ'  # Has Q1, maybe Q2, but missing Q3 (unusual case)
        
        df['status'] = df.apply(determine_status, axis=1)
        
        # Log status distribution
        status_counts = df['status'].value_counts()
        self.logger.info("Qualifying status distribution:")
        for status, count in status_counts.items():
            self.logger.info(f"  {status}: {count:,} records")
        
        return df
    
    def _add_auto_increment_dimension_keys(self, df: pd.DataFrame, dim_lookups: Dict[str, Dict[int, int]]) -> pd.DataFrame:
        """Add foreign keys for dimensional tables using AUTO_INCREMENT mappings.
        
        Args:
            df: Qualifying DataFrame
            dim_lookups: Dictionary containing CSV_ID → AUTO_INCREMENT_KEY mappings
            
        Returns:
            DataFrame with AUTO_INCREMENT dimension keys added
        """
        # Circuit keys - map CSV circuitId to AUTO_INCREMENT circuit_key
        if 'circuits' in dim_lookups:
            df['dim_circuit_circuit_key'] = df['circuitId'].map(dim_lookups['circuits'])
            unmapped_circuits = df[df['dim_circuit_circuit_key'].isna()]['circuitId'].nunique()
            if unmapped_circuits > 0:
                self.logger.warning(f"Found {unmapped_circuits} unmapped circuits - these records will be skipped")
                # For AUTO_INCREMENT, we can't use fallback keys - skip these records
                df = df.dropna(subset=['dim_circuit_circuit_key'])
            df['dim_circuit_circuit_key'] = df['dim_circuit_circuit_key'].astype(int)
        
        # Constructor keys - map CSV constructorId to AUTO_INCREMENT constructor_key
        if 'constructors' in dim_lookups:
            df['dim_constructor_constructor_key'] = df['constructorId'].map(dim_lookups['constructors'])
            # Should all be valid due to pre-filtering, but check anyway
            unmapped_constructors = df[df['dim_constructor_constructor_key'].isna()]
            if len(unmapped_constructors) > 0:
                self.logger.error(f"Found {len(unmapped_constructors)} unmapped constructors after validation!")
                df = df.dropna(subset=['dim_constructor_constructor_key'])
            df['dim_constructor_constructor_key'] = df['dim_constructor_constructor_key'].astype(int)
        
        # Driver keys - map CSV driverId to AUTO_INCREMENT driver_key
        if 'drivers' in dim_lookups:
            df['dim_driver_driver_key'] = df['driverId'].map(dim_lookups['drivers'])
            # Should all be valid due to pre-filtering, but check anyway
            unmapped_drivers = df[df['dim_driver_driver_key'].isna()]
            if len(unmapped_drivers) > 0:
                self.logger.error(f"Found {len(unmapped_drivers)} unmapped drivers after validation!")
                df = df.dropna(subset=['dim_driver_driver_key'])
            df['dim_driver_driver_key'] = df['dim_driver_driver_key'].astype(int)
        
        # Date keys (use calculated date_key directly)
        df['dim_date_date_key'] = df['date_key'].astype(int)
        
        return df
    
    def _add_business_measures(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add business measures and indicators.
        
        Args:
            df: DataFrame with basic qualifying data
            
        Returns:
            DataFrame with business measures added
        """
        # Position (already exists, just ensure it's properly typed)
        df['position'] = pd.to_numeric(df['position'], errors='coerce').fillna(0).astype(int)
        
        # Convert position to TINYINT range (0 if null/invalid)
        df['position'] = df['position'].clip(lower=0, upper=255)
        
        return df
    
    def _finalize_auto_increment_fact_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Final cleanup and column selection for AUTO_INCREMENT fact table.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Final DataFrame ready for database loading (without qualifying_id)
        """
        # Select only the columns needed for the fact table (NO qualifying_id - AUTO_INCREMENT handles it)
        fact_columns = [
            'dim_circuit_circuit_key',
            'dim_constructor_constructor_key',
            'dim_driver_driver_key',
            'dim_date_date_key',
            'position',
            'q1_ms',
            'q2_ms',
            'q3_ms',
            'status'
        ]
        
        # Ensure all required columns exist
        missing_columns = [col for col in fact_columns if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required fact columns: {missing_columns}")
            raise ValueError(f"Missing required fact columns: {missing_columns}")
        
        # Select columns and ensure proper data types
        df_fact = df[fact_columns].copy()
        
        # Ensure integer types for keys
        key_columns = ['dim_circuit_circuit_key', 'dim_constructor_constructor_key',
                      'dim_driver_driver_key', 'dim_date_date_key', 'position']
        for col in key_columns:
            df_fact[col] = df_fact[col].fillna(0).astype(int)
        
        # Ensure integer types for time measures (can be NULL)
        time_columns = ['q1_ms', 'q2_ms', 'q3_ms']
        for col in time_columns:
            # Keep None values as None, convert valid values to int
            df_fact[col] = df_fact[col].apply(lambda x: int(x) if pd.notna(x) else None)
        
        # Ensure status is string
        df_fact['status'] = df_fact['status'].astype(str)
        
        # Validate data ranges
        invalid_positions = df_fact[df_fact['position'] > 30]  # More than 30 cars
        if len(invalid_positions) > 0:
            self.logger.warning(f"Found {len(invalid_positions)} records with position > 30")
        
        # Validate time ranges (should be reasonable for F1)
        for time_col in time_columns:
            if time_col in df_fact.columns:
                valid_times = df_fact[df_fact[time_col].notna()]
                if len(valid_times) > 0:
                    min_time = valid_times[time_col].min()
                    max_time = valid_times[time_col].max()
                    
                    # Reasonable F1 qualifying times: 60 seconds to 10 minutes
                    if min_time < 60000:  # Less than 1 minute
                        fast_times = (valid_times[time_col] < 60000).sum()
                        self.logger.warning(f"Found {fast_times} {time_col} times < 60 seconds")
                    
                    if max_time > 600000:  # More than 10 minutes
                        slow_times = (valid_times[time_col] > 600000).sum()
                        self.logger.warning(f"Found {slow_times} {time_col} times > 10 minutes")
        
        self.logger.info(f"Finalized {len(df_fact)} fact records for AUTO_INCREMENT loading")
        return df_fact
    
    def _add_race_and_date_info(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """Add race information and calculate date keys.
        
        Args:
            df: Qualifying DataFrame
            races_df: Race information DataFrame
            
        Returns:
            DataFrame with race info and date keys
        """
        # Create race lookup with date and circuit information
        race_info = races_df[['raceId', 'date', 'circuitId']].copy()
        race_info['race_date'] = pd.to_datetime(race_info['date'], errors='coerce')
        race_info['qualifying_date'] = race_info['race_date'] - timedelta(days=1)
        race_info['date_key'] = race_info['qualifying_date'].dt.strftime('%Y%m%d').astype(int)
        
        # Merge race information
        df = df.merge(
            race_info[['raceId', 'date_key', 'circuitId']], 
            on='raceId', 
            how='left'
        )
        
        return df
    
    def get_fact_summary(self, fact_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for the fact table.
        
        Args:
            fact_df: Transformed fact DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        if fact_df.empty:
            return {'total_records': 0, 'note': 'Empty DataFrame'}
        
        summary = {
            'total_records': len(fact_df),
            'status_distribution': fact_df['status'].value_counts().to_dict(),
            'position_statistics': {
                'avg_position': fact_df['position'].mean(),
                'median_position': fact_df['position'].median(),
                'max_position': fact_df['position'].max(),
                'min_position': fact_df['position'].min()
            },
            'time_statistics': {},
            'data_quality': {
                'null_counts': fact_df.isnull().sum().to_dict(),
                'missing_keys': {
                    'circuit_key_zeros': (fact_df['dim_circuit_circuit_key'] == 0).sum(),
                    'constructor_key_zeros': (fact_df['dim_constructor_constructor_key'] == 0).sum(),
                    'driver_key_zeros': (fact_df['dim_driver_driver_key'] == 0).sum()
                }
            }
        }
        
        # Add time statistics for each qualifying session
        for time_col in ['q1_ms', 'q2_ms', 'q3_ms']:
            if time_col in fact_df.columns:
                valid_times = fact_df[fact_df[time_col].notna()][time_col]
                if len(valid_times) > 0:
                    summary['time_statistics'][time_col] = {
                        'count': len(valid_times),
                        'avg_ms': valid_times.mean(),
                        'min_ms': valid_times.min(),
                        'max_ms': valid_times.max()
                    }
        
        return summary