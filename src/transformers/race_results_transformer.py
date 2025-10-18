"""Race results fact transformer for the F1 ETL Pipeline."""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
import re

from ..config.settings import settings
from ..utils.logging_config import get_logger, ETLProgressLogger


class RaceResultsFactTransformer:
    """Handles transformation of data for the race results fact table."""
    
    def __init__(self):
        """Initialize the race results fact transformer."""
        self.logger = get_logger(__name__)
        self.progress_logger = ETLProgressLogger(self.logger)
    
    def transform_race_results(
        self,
        results_df: pd.DataFrame,
        status_df: pd.DataFrame,
        dim_lookups: Dict[str, Dict[int, int]],
        races_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Transform race results data for fact_race_result table.
        
        Args:
            results_df: Raw results DataFrame from CSV
            status_df: Raw status DataFrame from CSV
            dim_lookups: Dictionary containing CSV_ID → AUTO_INCREMENT_KEY mappings
            races_df: DataFrame with race information for date mapping
            
        Returns:
            Transformed DataFrame ready for fact_race_result table
        """
        self.logger.info(f"Transforming {len(results_df):,} race result records")
        self.progress_logger.start_process("Race Results Fact Transformation", 8)
        
        # Step 1: Validate and filter records
        self.progress_logger.log_step("Validating records and filtering invalid data")
        results_df, skipped_count = self._validate_and_filter_records(
            results_df, dim_lookups, races_df
        )
        
        if results_df.empty:
            self.logger.warning("All race result records were invalid - returning empty DataFrame")
            return pd.DataFrame()
        
        # Step 2: Add race information and date keys
        self.progress_logger.log_step("Adding race information and date keys")
        results_df = self._add_race_and_date_info(results_df, races_df)
        
        # Step 3: Join with status data
        self.progress_logger.log_step("Joining with status data")
        fact_df = self._join_with_status(results_df, status_df)
        
        # Step 4: Add dimension foreign keys using AUTO_INCREMENT mappings
        self.progress_logger.log_step("Adding AUTO_INCREMENT dimension foreign keys")
        fact_df = self._add_auto_increment_dimension_keys(fact_df, dim_lookups)
        
        # Step 4.5: Handle duplicate race-driver combinations (after dimension mapping)
        self.progress_logger.log_step("Handling duplicate race-driver combinations")
        fact_df = self._handle_duplicates_by_dimension_keys(fact_df)
        
        # Step 5: Transform race result specific measures
        self.progress_logger.log_step("Transforming race result measures")
        fact_df = self._transform_race_result_measures(fact_df)
        
        # Step 6: Add business measures and indicators
        self.progress_logger.log_step("Adding business measures and indicators")
        fact_df = self._add_business_measures(fact_df)
        
        # Step 7: Final cleanup and column selection
        self.progress_logger.log_step("Final cleanup and column selection")
        fact_df = self._finalize_race_result_fact_data(fact_df)
        
        # Step 8: Data quality validation
        self.progress_logger.log_step("Performing data quality validation")
        fact_df = self._validate_data_quality(fact_df)
        
        self.progress_logger.complete_process("Race Results Fact Transformation", len(fact_df))
        self.logger.info(f"Transformation complete: {len(fact_df):,} valid records, {skipped_count:,} skipped")
        return fact_df
    
    def _validate_and_filter_records(
        self,
        results_df: pd.DataFrame,
        dim_lookups: Dict[str, Dict[int, int]],
        races_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, int]:
        """Validate records and filter out invalid ones.
        
        Args:
            results_df: Raw results DataFrame
            dim_lookups: Dimension lookup dictionaries
            races_df: Race information DataFrame
            
        Returns:
            Tuple of (filtered_dataframe, skipped_count)
        """
        initial_count = len(results_df)
        skipped_reasons = {
            'missing_driver': 0,
            'missing_constructor': 0,
            'missing_race': 0,
            'invalid_data': 0
        }
        
        # Create lookup sets for validation
        race_lookup = set(races_df['raceId'].values) if 'raceId' in races_df.columns else set()
        driver_lookup = set(dim_lookups.get('drivers', {}).keys())
        constructor_lookup = set(dim_lookups.get('constructors', {}).keys())
        
        # Create validation mask
        valid_mask = pd.Series([True] * len(results_df), index=results_df.index)
        
        # Check for missing drivers
        if 'driverId' in results_df.columns:
            missing_driver_mask = ~results_df['driverId'].isin(driver_lookup)
            skipped_reasons['missing_driver'] = missing_driver_mask.sum()
            valid_mask &= ~missing_driver_mask
        
        # Check for missing constructors
        if 'constructorId' in results_df.columns:
            missing_constructor_mask = ~results_df['constructorId'].isin(constructor_lookup)
            skipped_reasons['missing_constructor'] = missing_constructor_mask.sum()
            valid_mask &= ~missing_constructor_mask
        
        # Check for missing races
        if 'raceId' in results_df.columns:
            missing_race_mask = ~results_df['raceId'].isin(race_lookup)
            skipped_reasons['missing_race'] = missing_race_mask.sum()
            valid_mask &= ~missing_race_mask
        
        # Check for invalid result data
        required_columns = ['resultId', 'raceId', 'driverId', 'constructorId']
        for col in required_columns:
            if col in results_df.columns:
                invalid_mask = results_df[col].isnull()
                skipped_reasons['invalid_data'] += invalid_mask.sum()
                valid_mask &= ~invalid_mask
        
        # Filter to valid records only
        df_valid = results_df[valid_mask].copy()
        skipped_count = initial_count - len(df_valid)
        
        # Log skipping reasons
        if skipped_count > 0:
            self.logger.info(f"Skipped {skipped_count:,} invalid race result records:")
            for reason, count in skipped_reasons.items():
                if count > 0:
                    self.logger.info(f"  - {reason}: {count:,} records")
        
        return df_valid, skipped_count
    
    def _add_race_and_date_info(self, results_df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """Add race information and calculate date keys.
        
        Args:
            results_df: Results DataFrame
            races_df: Race information DataFrame
            
        Returns:
            DataFrame with race info and date keys
        """
        # Create race lookup with date and circuit information
        race_info = races_df[['raceId', 'date', 'circuitId']].copy()
        race_info['race_date'] = pd.to_datetime(race_info['date'], errors='coerce')
        race_info['date_key'] = race_info['race_date'].dt.strftime('%Y%m%d').astype(int)
        
        # Merge race information
        results_df = results_df.merge(
            race_info[['raceId', 'date_key', 'circuitId']], 
            on='raceId', 
            how='left'
        )
        
        return results_df
    
    def _join_with_status(self, results_df: pd.DataFrame, status_df: pd.DataFrame) -> pd.DataFrame:
        """Join results with status data to get status text.
        
        Args:
            results_df: Results DataFrame
            status_df: Status DataFrame
            
        Returns:
            DataFrame with status data joined
        """
        self.logger.info("Joining race results with status data")
        
        # Join on statusId to get status text
        fact_df = results_df.merge(
            status_df[['statusId', 'status']],
            on='statusId',
            how='left'
        )
        
        # Rename status column to match schema
        fact_df = fact_df.rename(columns={'status': 'status_text'})
        
        # Log join statistics
        total_results = len(results_df)
        matched_with_status = len(fact_df[fact_df['status_text'].notna()])
        self.logger.info(f"Matched {matched_with_status:,} of {total_results:,} results with status text")
        
        return fact_df
    
    def _handle_duplicates(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Handle duplicate race-driver combinations to avoid unique constraint violations.
        
        Args:
            fact_df: DataFrame with potential duplicates
            
        Returns:
            DataFrame with duplicates resolved
        """
        initial_count = len(fact_df)
        
        # Check for duplicates based on raceId and driverId (before dimension mapping)
        duplicate_mask = fact_df.duplicated(subset=['raceId', 'driverId'], keep=False)
        duplicate_count = duplicate_mask.sum()
        
        if duplicate_count == 0:
            self.logger.info("No duplicate race-driver combinations found")
            return fact_df
        
        self.logger.warning(f"Found {duplicate_count:,} duplicate race-driver combinations")
        
        # Log details about duplicates
        duplicate_pairs = fact_df[duplicate_mask][['raceId', 'driverId', 'positionOrder', 'resultId']].copy()
        unique_pairs = duplicate_pairs.groupby(['raceId', 'driverId']).size()
        self.logger.info(f"Duplicate race-driver pairs: {len(unique_pairs)} pairs with multiple entries")
        
        # Strategy: Keep the record with the best (lowest) position order for each race-driver
        # If position order is the same or null, keep the first record (lowest resultId)
        def resolve_duplicate_group(group):
            """Resolve duplicates within a race-driver group."""
            if len(group) == 1:
                return group.iloc[0]  # No duplicates, return the single record
            
            # Sort by position order (ascending, nulls last), then by resultId (ascending)
            group_sorted = group.sort_values(['positionOrder', 'resultId'], 
                                           na_position='last')
            
            # Keep the first record after sorting (best position or earliest result)
            return group_sorted.iloc[0]
        
        # Apply deduplication
        fact_df_deduplicated = fact_df.groupby(['raceId', 'driverId'], as_index=False).apply(
            resolve_duplicate_group, include_groups=False
        ).reset_index(drop=True)
        
        final_count = len(fact_df_deduplicated)
        removed_count = initial_count - final_count
        
        self.logger.info(f"Duplicate resolution complete:")
        self.logger.info(f"  - Original records: {initial_count:,}")
        self.logger.info(f"  - After deduplication: {final_count:,}")
        self.logger.info(f"  - Duplicates removed: {removed_count:,}")
        self.logger.info("  - Strategy: Kept record with best position order per race-driver pair")
        
        return fact_df_deduplicated
    
    def _handle_duplicates_by_dimension_keys(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Handle duplicate race-driver combinations based on dimension keys.
        
        Args:
            fact_df: DataFrame with dimension keys mapped
            
        Returns:
            DataFrame with duplicates resolved based on dimension keys
        """
        initial_count = len(fact_df)
        
        # Debug: Check for any missing dimension keys
        missing_race_keys = fact_df['dim_race_race_key'].isna().sum()
        missing_driver_keys = fact_df['dim_driver_driver_key'].isna().sum()
        if missing_race_keys > 0 or missing_driver_keys > 0:
            self.logger.warning(f"Missing dimension keys: race={missing_race_keys}, driver={missing_driver_keys}")
        
        # Check for duplicates based on dimension keys (the actual constraint)
        duplicate_mask = fact_df.duplicated(subset=['dim_race_race_key', 'dim_driver_driver_key'], keep=False)
        duplicate_count = duplicate_mask.sum()
        
        # Debug: Show some dimension key combinations
        sample_combinations = fact_df[['dim_race_race_key', 'dim_driver_driver_key']].head(10)
        self.logger.info(f"Sample race-driver dimension key combinations:")
        for i, row in sample_combinations.iterrows():
            self.logger.info(f"  Row {i}: race_key={row['dim_race_race_key']}, driver_key={row['dim_driver_driver_key']}")
        
        if duplicate_count == 0:
            self.logger.info("No duplicate race-driver dimension key combinations found")
            return fact_df
        
        self.logger.warning(f"Found {duplicate_count:,} duplicate race-driver dimension key combinations")
        
        # Log details about duplicates
        duplicate_records = fact_df[duplicate_mask][['dim_race_race_key', 'dim_driver_driver_key', 'positionOrder', 'resultId']].copy()
        self.logger.info(f"First 5 duplicate records:")
        for i, row in duplicate_records.head(5).iterrows():
            self.logger.info(f"  Row {i}: race_key={row['dim_race_race_key']}, driver_key={row['dim_driver_driver_key']}, pos_order={row['positionOrder']}, result_id={row['resultId']}")
        
        unique_pairs = duplicate_records.groupby(['dim_race_race_key', 'dim_driver_driver_key']).size()
        self.logger.info(f"Duplicate dimension key pairs: {len(unique_pairs)} pairs with multiple entries")
        
        # Apply deduplication: keep first occurrence of each race-driver combination
        fact_df_deduplicated = fact_df.drop_duplicates(subset=['dim_race_race_key', 'dim_driver_driver_key'], keep='first')
        
        final_count = len(fact_df_deduplicated)
        removed_count = initial_count - final_count
        
        self.logger.info(f"Dimension key duplicate resolution complete:")
        self.logger.info(f"  - Original records: {initial_count:,}")
        self.logger.info(f"  - After deduplication: {final_count:,}")
        self.logger.info(f"  - Duplicates removed: {removed_count:,}")
        self.logger.info("  - Strategy: Kept first occurrence of each race-driver dimension key pair")
        
        return fact_df_deduplicated

    def _add_auto_increment_dimension_keys(self, fact_df: pd.DataFrame, dim_lookups: Dict[str, Dict[int, int]]) -> pd.DataFrame:
        """Add foreign keys for dimensional tables using AUTO_INCREMENT mappings.
        
        Args:
            fact_df: Race results fact DataFrame
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
        
        # Constructor keys - map CSV constructorId to AUTO_INCREMENT constructor_key
        if 'constructors' in dim_lookups:
            fact_df['dim_constructor_constructor_key'] = fact_df['constructorId'].map(dim_lookups['constructors'])
            unmapped_constructors = fact_df[fact_df['dim_constructor_constructor_key'].isna()]
            if len(unmapped_constructors) > 0:
                self.logger.error(f"Found {len(unmapped_constructors)} unmapped constructors after validation!")
                fact_df = fact_df.dropna(subset=['dim_constructor_constructor_key'])
            fact_df['dim_constructor_constructor_key'] = fact_df['dim_constructor_constructor_key'].astype(int)
        
        # Driver keys - map CSV driverId to AUTO_INCREMENT driver_key
        if 'drivers' in dim_lookups:
            fact_df['dim_driver_driver_key'] = fact_df['driverId'].map(dim_lookups['drivers'])
            unmapped_drivers = fact_df[fact_df['dim_driver_driver_key'].isna()]
            if len(unmapped_drivers) > 0:
                self.logger.error(f"Found {len(unmapped_drivers)} unmapped drivers after validation!")
                fact_df = fact_df.dropna(subset=['dim_driver_driver_key'])
            fact_df['dim_driver_driver_key'] = fact_df['dim_driver_driver_key'].astype(int)
        
        # Date keys (use calculated date_key directly)
        fact_df['dim_date_date_key'] = fact_df['date_key'].astype(int)
        
        return fact_df
    
    def _transform_race_result_measures(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Transform race result specific measures.
        
        Args:
            fact_df: DataFrame with race result data
            
        Returns:
            DataFrame with transformed measures
        """
        # Grid position
        fact_df['grid'] = pd.to_numeric(fact_df['grid'], errors='coerce').fillna(0).astype(int)
        fact_df['grid'] = fact_df['grid'].clip(lower=0, upper=255)  # TINYINT range
        
        # Finish position (numeric)
        fact_df['finish_position'] = pd.to_numeric(fact_df['position'], errors='coerce')
        fact_df['finish_position'] = fact_df['finish_position'].clip(lower=0, upper=255).astype('Int64')  # Nullable int
        
        # Position text (e.g., '1', 'R', 'DNF')
        fact_df['position_text'] = fact_df['positionText'].astype(str).str.strip()
        fact_df['position_text'] = fact_df['position_text'].where(fact_df['position_text'] != 'nan', None)
        
        # Position order
        fact_df['position_order'] = pd.to_numeric(fact_df['positionOrder'], errors='coerce')
        fact_df['position_order'] = fact_df['position_order'].clip(lower=0, upper=255).astype('Int64')  # Nullable int
        
        # Points
        fact_df['points'] = pd.to_numeric(fact_df['points'], errors='coerce').astype('float64')
        
        # Laps completed
        fact_df['laps'] = pd.to_numeric(fact_df['laps'], errors='coerce').fillna(0).astype(int)
        
        # Race time in milliseconds
        fact_df['race_time_ms'] = pd.to_numeric(fact_df['milliseconds'], errors='coerce').astype('Int64')  # Nullable int
        
        # Fastest lap number
        fact_df['fastest_lap_no'] = pd.to_numeric(fact_df['fastestLap'], errors='coerce').astype('Int64')  # Nullable int
        
        # Fastest lap rank
        fact_df['fastest_lap_rank'] = pd.to_numeric(fact_df['rank'], errors='coerce')
        fact_df['fastest_lap_rank'] = fact_df['fastest_lap_rank'].clip(lower=0, upper=255).astype('Int64')  # Nullable int
        
        # Fastest lap time in milliseconds
        fact_df['fastest_lap_time_ms'] = fact_df['fastestLapTime'].apply(self._time_string_to_milliseconds)
        
        # Fastest lap speed
        fact_df['fastest_lap_speed'] = pd.to_numeric(fact_df['fastestLapSpeed'], errors='coerce').astype('float64')
        
        return fact_df
    
    def _time_string_to_milliseconds(self, time_str: str) -> Optional[int]:
        """Convert time string to milliseconds.
        
        Args:
            time_str: Time string in format "M:SS.SSS"
            
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
                return None
                
        except (ValueError, AttributeError):
            return None
    
    def _add_business_measures(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Add business measures and indicators.
        
        Args:
            fact_df: DataFrame with basic race result data
            
        Returns:
            DataFrame with business measures added
        """
        # Add derived measures if needed
        # For example: performance indicators, championship points, etc.
        
        return fact_df
    
    def _finalize_race_result_fact_data(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Final cleanup and column selection for race result fact table.
        
        Args:
            fact_df: Transformed DataFrame
            
        Returns:
            Final DataFrame ready for database loading
        """
        # Select only the columns needed for the fact table
        fact_columns = [
            'dim_race_race_key',
            'dim_circuit_circuit_key',
            'dim_constructor_constructor_key',
            'dim_driver_driver_key',
            'dim_date_date_key',
            'grid',
            'finish_position',
            'position_text',
            'position_order',
            'points',
            'laps',
            'race_time_ms',
            'fastest_lap_no',
            'fastest_lap_rank',
            'fastest_lap_time_ms',
            'fastest_lap_speed',
            'status_text'
        ]
        
        # Ensure all required columns exist
        missing_columns = [col for col in fact_columns if col not in fact_df.columns]
        if missing_columns:
            self.logger.error(f"Missing required fact columns: {missing_columns}")
            raise ValueError(f"Missing required fact columns: {missing_columns}")
        
        # Select columns and ensure proper data types
        df_fact = fact_df[fact_columns].copy()
        
        # Ensure integer types for keys and required fields
        key_columns = ['dim_race_race_key', 'dim_circuit_circuit_key',
                      'dim_constructor_constructor_key', 'dim_driver_driver_key', 
                      'dim_date_date_key', 'grid', 'laps']
        for col in key_columns:
            df_fact[col] = df_fact[col].fillna(0).astype(int)
        
        # Handle nullable integer columns
        nullable_int_columns = ['finish_position', 'position_order', 'race_time_ms', 
                               'fastest_lap_no', 'fastest_lap_rank', 'fastest_lap_time_ms']
        for col in nullable_int_columns:
            if col in df_fact.columns:
                # Keep None values as None, convert valid values to int
                df_fact[col] = df_fact[col].apply(lambda x: int(x) if pd.notna(x) else None)
        
        # Handle nullable float columns
        nullable_float_columns = ['points', 'fastest_lap_speed']
        for col in nullable_float_columns:
            if col in df_fact.columns:
                df_fact[col] = df_fact[col].astype('float64')
        
        # Ensure string columns
        string_columns = ['position_text', 'status_text']
        for col in string_columns:
            if col in df_fact.columns:
                df_fact[col] = df_fact[col].astype(str)
                df_fact[col] = df_fact[col].where(df_fact[col] != 'nan', None)
        
        self.logger.info(f"Finalized {len(df_fact)} race result fact records for loading")
        return df_fact
    
    def _validate_data_quality(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Validate data quality and log warnings for unusual values.
        
        Args:
            fact_df: Final fact DataFrame
            
        Returns:
            Validated DataFrame
        """
        # Validate grid positions
        invalid_grid = fact_df[fact_df['grid'] > 30]  # More than 30 grid positions unusual
        if len(invalid_grid) > 0:
            self.logger.warning(f"Found {len(invalid_grid)} records with grid > 30")
        
        # Validate finish positions
        valid_positions = fact_df[fact_df['finish_position'].notna()]
        if len(valid_positions) > 0:
            invalid_finish = valid_positions[valid_positions['finish_position'] > 30]
            if len(invalid_finish) > 0:
                self.logger.warning(f"Found {len(invalid_finish)} records with finish_position > 30")
        
        # Validate points (should be non-negative)
        negative_points = fact_df[fact_df['points'] < 0]
        if len(negative_points) > 0:
            self.logger.warning(f"Found {len(negative_points)} records with negative points")
        
        # Validate laps completed
        if len(fact_df) > 0:
            max_laps = fact_df['laps'].max()
            if max_laps > 100:  # More than 100 laps unusual for F1
                high_laps = (fact_df['laps'] > 100).sum()
                self.logger.warning(f"Found {high_laps} records with laps > 100")
        
        return fact_df
    
    def get_fact_summary(self, fact_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for the race result fact table.
        
        Args:
            fact_df: Transformed fact DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        if fact_df.empty:
            return {'total_records': 0, 'note': 'Empty DataFrame'}
        
        summary = {
            'total_records': len(fact_df),
            'position_statistics': {
                'avg_grid_position': fact_df['grid'].mean(),
                'avg_finish_position': fact_df['finish_position'].mean() if fact_df['finish_position'].notna().any() else None,
                'max_laps': fact_df['laps'].max(),
                'avg_laps': fact_df['laps'].mean()
            },
            'points_statistics': {
                'total_points_awarded': fact_df['points'].sum(),
                'avg_points_per_driver': fact_df['points'].mean(),
                'max_points_single_race': fact_df['points'].max()
            },
            'status_distribution': fact_df['status_text'].value_counts().to_dict(),
            'data_quality': {
                'null_counts': fact_df.isnull().sum().to_dict(),
                'missing_finish_positions': fact_df['finish_position'].isnull().sum(),
                'missing_race_times': fact_df['race_time_ms'].isnull().sum(),
                'missing_fastest_lap_times': fact_df['fastest_lap_time_ms'].isnull().sum()
            }
        }
        
        return summary
