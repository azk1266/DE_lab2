"""CSV data extraction module for the F1 Qualifying ETL Pipeline."""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from datetime import datetime

from ..config.settings import settings
from ..utils.logging_config import get_logger, ETLProgressLogger


class CSVExtractor:
    """Handles extraction of data from F1 CSV source files."""
    
    def __init__(self):
        """Initialize the CSV extractor."""
        self.logger = get_logger(__name__)
        self.progress_logger = ETLProgressLogger(self.logger)
    
    def extract_circuits(self) -> pd.DataFrame:
        """Extract circuit data from CSV file.
        
        Returns:
            DataFrame with circuit information
            
        Raises:
            FileNotFoundError: If circuits.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.circuits_path
        self.logger.info(f"Extracting circuits data from {file_path}")
        
        try:
            # Read circuits CSV with specific data types
            df = pd.read_csv(
                file_path,
                dtype={
                    'circuitId': 'int64',
                    'circuitRef': 'string',
                    'name': 'string',
                    'location': 'string',
                    'country': 'string',
                    'lat': 'float64',
                    'lng': 'float64',
                    'alt': 'float64',
                    'url': 'string'
                },
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', '\\N']
            )
            
            # Validate required columns
            required_columns = ['circuitId', 'name', 'location', 'country']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in circuits.csv: {missing_columns}")
            
            # Basic data validation
            initial_count = len(df)
            df = df.dropna(subset=['circuitId', 'name'])
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows with missing circuit data")
            
            self.logger.info(f"Successfully extracted {len(df)} circuit records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Circuits file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Circuits file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading circuits file: {e}")
            raise
    
    def extract_constructors(self) -> pd.DataFrame:
        """Extract constructor data from CSV file.
        
        Returns:
            DataFrame with constructor information
            
        Raises:
            FileNotFoundError: If constructors.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.constructors_path
        self.logger.info(f"Extracting constructors data from {file_path}")
        
        try:
            # Read constructors CSV with specific data types
            df = pd.read_csv(
                file_path,
                dtype={
                    'constructorId': 'int64',
                    'constructorRef': 'string',
                    'name': 'string',
                    'nationality': 'string',
                    'url': 'string'
                },
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', '\\N']
            )
            
            # Validate required columns
            required_columns = ['constructorId', 'name']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in constructors.csv: {missing_columns}")
            
            # Basic data validation
            initial_count = len(df)
            df = df.dropna(subset=['constructorId', 'name'])
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows with missing constructor data")
            
            self.logger.info(f"Successfully extracted {len(df)} constructor records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Constructors file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Constructors file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading constructors file: {e}")
            raise
    
    def extract_drivers(self) -> pd.DataFrame:
        """Extract driver data from CSV file.
        
        Returns:
            DataFrame with driver information
            
        Raises:
            FileNotFoundError: If drivers.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.drivers_path
        self.logger.info(f"Extracting drivers data from {file_path}")
        
        try:
            # Read drivers CSV with specific data types
            df = pd.read_csv(
                file_path,
                dtype={
                    'driverId': 'int64',
                    'driverRef': 'string',
                    'number': 'string',
                    'code': 'string',
                    'forename': 'string',
                    'surname': 'string',
                    'dob': 'string',
                    'nationality': 'string',
                    'url': 'string'
                },
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', '\\N']
            )
            
            # Validate required columns
            required_columns = ['driverId', 'forename', 'surname']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in drivers.csv: {missing_columns}")
            
            # Basic data validation
            initial_count = len(df)
            df = df.dropna(subset=['driverId', 'forename', 'surname'])
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows with missing driver data")
            
            self.logger.info(f"Successfully extracted {len(df)} driver records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Drivers file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Drivers file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading drivers file: {e}")
            raise
    
    def extract_races(self) -> pd.DataFrame:
        """Extract race data from CSV file.
        
        Returns:
            DataFrame with race information
            
        Raises:
            FileNotFoundError: If races.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.races_path
        self.logger.info(f"Extracting races data from {file_path}")
        
        try:
            # Read races CSV with specific data types
            df = pd.read_csv(
                file_path,
                dtype={
                    'raceId': 'int64',
                    'year': 'int64',
                    'round': 'int64',
                    'circuitId': 'int64',
                    'name': 'string',
                    'date': 'string',
                    'time': 'string',
                    'url': 'string'
                },
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', '\\N']
            )
            
            # Validate required columns
            required_columns = ['raceId', 'year', 'circuitId', 'date']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in races.csv: {missing_columns}")
            
            # Basic data validation
            initial_count = len(df)
            df = df.dropna(subset=['raceId', 'date'])
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows with missing race data")
            
            self.logger.info(f"Successfully extracted {len(df)} race records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Races file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Races file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading races file: {e}")
            raise
    
    def extract_qualifying_batched(self, batch_size: Optional[int] = None, start_batch: int = 1):
        """Extract qualifying data in batches without loading all data into memory.
        
        This generator yields individual batches of qualifying data, providing true
        memory-efficient processing with position tracking.
        
        Args:
            batch_size: Number of rows per batch (uses settings.BATCH_SIZE if None)
            start_batch: Batch number to start from (1-based, for resumption)
            
        Yields:
            Tuple of (batch_number, start_row, end_row, batch_dataframe)
            
        Raises:
            FileNotFoundError: If qualifying.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.qualifying_path
        batch_size = batch_size or settings.BATCH_SIZE
        
        self.logger.info(f"Starting batched extraction from {file_path}")
        self.logger.info(f"Batch size: {batch_size:,}, Starting from batch: {start_batch}")
        
        try:
            # Get total number of rows for progress tracking
            total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract header row
            self.logger.info(f"Total qualifying records in file: {total_rows:,}")
            
            # Calculate skip rows for resumption (preserve header)
            skip_rows = None
            if start_batch > 1:
                # Skip only data rows, keep header (row 0)
                rows_to_skip = (start_batch - 1) * batch_size
                skip_rows = range(1, rows_to_skip + 1)  # Skip rows 1 to rows_to_skip, keep header
                self.logger.info(f"Skipping {rows_to_skip:,} data rows for batch {start_batch} (preserving header)")
            
            # Define data types for qualifying columns
            dtype_dict = {
                'qualifyId': 'int64',
                'raceId': 'int64',
                'driverId': 'int64',
                'constructorId': 'int64',
                'number': 'string',
                'position': 'float64',
                'q1': 'string',
                'q2': 'string',
                'q3': 'string'
            }
            
            # Create batch reader
            batch_reader = pd.read_csv(
                file_path,
                chunksize=batch_size,
                skiprows=skip_rows if skip_rows else None,
                dtype=dtype_dict,
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', '\\N'],
                low_memory=False
            )
            
            # Process batches one at a time
            current_batch = start_batch
            
            for batch_df in batch_reader:
                # Calculate row positions for this batch
                start_row = (current_batch - 1) * batch_size + 1
                end_row = start_row + len(batch_df) - 1
                
                # Skip completely empty batches
                if batch_df.empty:
                    self.logger.warning(f"SKIPPED empty batch {current_batch}: rows {start_row:,}-{end_row:,}")
                    current_batch += 1
                    continue
                
                # Yield the batch information
                yield current_batch, start_row, end_row, batch_df
                
                current_batch += 1
            
            self.logger.info(f"Completed batched extraction - processed {current_batch - start_batch} batches")
            
        except FileNotFoundError:
            self.logger.error(f"Qualifying file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Qualifying file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error in batched extraction: {e}")
            raise
    
    def get_total_rows(self) -> int:
        """Get the total number of rows in the qualifying CSV file.
        
        Returns:
            Total number of data rows (excluding header)
        """
        try:
            file_path = settings.qualifying_path
            total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract header row
            return total_rows
        except Exception as e:
            self.logger.error(f"Error counting rows in qualifying file: {e}")
            return 0
    
    def validate_source_files(self) -> Dict[str, bool]:
        """Validate that all required source files exist and are readable.
        
        Returns:
            Dictionary mapping file names to their validation status
        """
        self.logger.info("Validating source CSV files")
        
        validation_results = {}
        files_to_check = {
            'circuits': settings.circuits_path,
            'constructors': settings.constructors_path,
            'drivers': settings.drivers_path,
            'races': settings.races_path,
            'qualifying': settings.qualifying_path
        }
        
        for file_name, file_path in files_to_check.items():
            try:
                if not file_path.exists():
                    self.logger.error(f"{file_name}.csv not found: {file_path}")
                    validation_results[file_name] = False
                    continue
                
                # Try to read the first 10 rows to validate file structure
                df_sample = pd.read_csv(file_path, nrows=10)
                if df_sample.empty:
                    self.logger.error(f"{file_name}.csv is empty")
                    validation_results[file_name] = False
                else:
                    self.logger.info(f"{file_name}.csv validation passed ({len(df_sample.columns)} columns)")
                    validation_results[file_name] = True
                    
            except Exception as e:
                self.logger.error(f"Error validating {file_name}.csv: {e}")
                validation_results[file_name] = False
        
        return validation_results
    
    def extract_pit_stops(self) -> pd.DataFrame:
        """Extract pit stop data from CSV file.
        
        Returns:
            DataFrame with pit stop information
            
        Raises:
            FileNotFoundError: If pit_stops.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.pit_stops_path
        self.logger.info(f"Extracting pit stops data from {file_path}")
        
        try:
            # Read pit stops CSV with specific data types
            df = pd.read_csv(
                file_path,
                dtype={
                    'raceId': 'int64',
                    'driverId': 'int64',
                    'stop': 'int64',
                    'lap': 'int64',
                    'time': 'string',
                    'duration': 'string',
                    'milliseconds': 'int64'
                },
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', '\\N']
            )
            
            # Validate required columns
            required_columns = ['raceId', 'driverId', 'stop', 'lap', 'milliseconds']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in pit_stops.csv: {missing_columns}")
            
            # Basic data validation
            initial_count = len(df)
            df = df.dropna(subset=required_columns)
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows with missing pit stop data")
            
            self.logger.info(f"Successfully extracted {len(df)} pit stop records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Pit stops file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Pit stops file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading pit stops file: {e}")
            raise
    
    def extract_lap_times(self) -> pd.DataFrame:
        """Extract lap times data from CSV file.
        
        Returns:
            DataFrame with lap times information
            
        Raises:
            FileNotFoundError: If lap_times.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.lap_times_path
        self.logger.info(f"Extracting lap times data from {file_path}")
        
        try:
            # Read lap times CSV with specific data types
            df = pd.read_csv(
                file_path,
                dtype={
                    'raceId': 'int64',
                    'driverId': 'int64',
                    'lap': 'int64',
                    'position': 'int64',
                    'time': 'string',
                    'milliseconds': 'int64'
                },
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', '\\N']
            )
            
            # Validate required columns
            required_columns = ['raceId', 'driverId', 'lap', 'milliseconds']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in lap_times.csv: {missing_columns}")
            
            # Basic data validation
            initial_count = len(df)
            df = df.dropna(subset=required_columns)
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows with missing lap times data")
            
            self.logger.info(f"Successfully extracted {len(df)} lap times records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Lap times file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Lap times file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading lap times file: {e}")
            raise
    
    def extract_results(self) -> pd.DataFrame:
        """Extract race results data from CSV file.
        
        Returns:
            DataFrame with race results information
            
        Raises:
            FileNotFoundError: If results.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.results_path
        self.logger.info(f"Extracting race results data from {file_path}")
        
        try:
            # Read results CSV with specific data types (handling NA values)
            df = pd.read_csv(
                file_path,
                dtype={
                    'resultId': 'int64',
                    'raceId': 'int64',
                    'driverId': 'int64',
                    'constructorId': 'int64',
                    'number': 'string',
                    'grid': 'Int64',  # Nullable integer
                    'position': 'string',  # Can be 'R', 'DNF', etc.
                    'positionText': 'string',
                    'positionOrder': 'Int64',  # Nullable integer
                    'points': 'float64',
                    'laps': 'Int64',  # Nullable integer
                    'time': 'string',
                    'milliseconds': 'Int64',  # Nullable integer (column 12)
                    'fastestLap': 'Int64',  # Nullable integer
                    'rank': 'Int64',  # Nullable integer
                    'fastestLapTime': 'string',
                    'fastestLapSpeed': 'float64',
                    'statusId': 'int64'
                },
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', '\\N']
            )
            
            # Validate required columns
            required_columns = ['resultId', 'raceId', 'driverId', 'constructorId', 'statusId']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in results.csv: {missing_columns}")
            
            # Basic data validation
            initial_count = len(df)
            df = df.dropna(subset=required_columns)
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows with missing results data")
            
            self.logger.info(f"Successfully extracted {len(df)} race results records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Results file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Results file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading results file: {e}")
            raise
    
    def extract_status(self) -> pd.DataFrame:
        """Extract status data from CSV file.
        
        Returns:
            DataFrame with status information
            
        Raises:
            FileNotFoundError: If status.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.status_path
        self.logger.info(f"Extracting status data from {file_path}")
        
        try:
            # Read status CSV with specific data types
            df = pd.read_csv(
                file_path,
                dtype={
                    'statusId': 'int64',
                    'status': 'string'
                },
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', '\\N']
            )
            
            # Validate required columns
            required_columns = ['statusId', 'status']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in status.csv: {missing_columns}")
            
            # Basic data validation
            initial_count = len(df)
            df = df.dropna(subset=required_columns)
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows with missing status data")
            
            self.logger.info(f"Successfully extracted {len(df)} status records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Status file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Status file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading status file: {e}")
            raise

    def get_data_summary(self) -> Dict[str, Dict[str, any]]:
        """Get summary information about all source data files.
        
        Returns:
            Dictionary with summary statistics for each file
        """
        self.logger.info("Generating data summary")
        
        summary = {}
        
        try:
            # Circuits summary
            circuits_df = self.extract_circuits()
            summary['circuits'] = {
                'record_count': len(circuits_df),
                'columns': list(circuits_df.columns),
                'unique_circuits': circuits_df['circuitId'].nunique(),
                'countries': circuits_df['country'].nunique() if 'country' in circuits_df.columns else 0
            }
            
            # Constructors summary
            constructors_df = self.extract_constructors()
            summary['constructors'] = {
                'record_count': len(constructors_df),
                'columns': list(constructors_df.columns),
                'unique_constructors': constructors_df['constructorId'].nunique()
            }
            
            # Drivers summary
            drivers_df = self.extract_drivers()
            summary['drivers'] = {
                'record_count': len(drivers_df),
                'columns': list(drivers_df.columns),
                'unique_drivers': drivers_df['driverId'].nunique(),
                'nationalities': drivers_df['nationality'].nunique() if 'nationality' in drivers_df.columns else 0
            }
            
            # Races summary
            races_df = self.extract_races()
            summary['races'] = {
                'record_count': len(races_df),
                'columns': list(races_df.columns),
                'unique_races': races_df['raceId'].nunique(),
                'year_range': {
                    'min_year': races_df['year'].min(),
                    'max_year': races_df['year'].max()
                } if 'year' in races_df.columns else None
            }
            
            # Qualifying summary (sample only for performance)
            qualifying_sample = pd.read_csv(settings.qualifying_path, nrows=10)
            total_qualifying = sum(1 for _ in open(settings.qualifying_path)) - 1
            
            summary['qualifying'] = {
                'record_count': total_qualifying,
                'sample_size': len(qualifying_sample),
                'columns': list(qualifying_sample.columns),
                'unique_races': qualifying_sample['raceId'].nunique() if 'raceId' in qualifying_sample.columns else 0,
                'unique_drivers': qualifying_sample['driverId'].nunique() if 'driverId' in qualifying_sample.columns else 0
            }
            
            # Add pit stops summary
            try:
                pit_stops_df = self.extract_pit_stops()
                summary['pit_stops'] = {
                    'record_count': len(pit_stops_df),
                    'columns': list(pit_stops_df.columns),
                    'unique_races': pit_stops_df['raceId'].nunique(),
                    'unique_drivers': pit_stops_df['driverId'].nunique()
                }
            except Exception as e:
                summary['pit_stops'] = {'error': str(e)}
            
            # Add results summary
            try:
                results_df = self.extract_results()
                summary['results'] = {
                    'record_count': len(results_df),
                    'columns': list(results_df.columns),
                    'unique_races': results_df['raceId'].nunique(),
                    'unique_drivers': results_df['driverId'].nunique()
                }
            except Exception as e:
                summary['results'] = {'error': str(e)}
                
        except Exception as e:
            self.logger.error(f"Error generating data summary: {e}")
            summary['error'] = str(e)
        
        return summary
