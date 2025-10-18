#!/usr/bin/env python3
"""Main ETL Pipeline Script for F1 Qualifying Dimensional Modeling Project.

This script orchestrates the complete ETL process:
1. Extract data from CSV files
2. Transform data for dimensional model
3. Load data into MySQL database
4. Validate data integrity

Usage:
    python run_etl.py [options]
    
Options:
    --skip-validation: Skip data integrity validation
    --sample-size: Process only N qualifying records (for testing)
    --help: Show this help message
"""

import sys
import argparse
import time
from pathlib import Path
from typing import Dict, Any, Optional

# Add src directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.config.settings import settings
from src.utils.logging_config import setup_logging, ETLProgressLogger
from src.extractors.csv_extractor import CSVExtractor
from src.transformers.dimension_transformer import DimensionTransformer
from src.transformers.fact_transformer import FactTransformer
from src.loaders.mysql_loader import MySQLLoader



class F1ETL:
    """Main F1 ETL pipeline orchestrator supporting multiple schemas."""
    
    def __init__(self, schema_type: str = 'qualifying'):
        """Initialize the F1 ETL pipeline.
        
        Args:
            schema_type: Type of schema ('qualifying', 'pit_stop', 'race_results')
        """
        self.schema_type = schema_type
        
        # Setup logging with per-run log file
        self.logger = setup_logging()
        self.progress_logger = ETLProgressLogger(self.logger)
        
        # Initialize components
        self.extractor = CSVExtractor()
        self.dim_transformer = DimensionTransformer()
        self.loader = MySQLLoader()
        
        # Initialize schema-specific fact transformers
        self.fact_transformers = {
            'qualifying': FactTransformer(),
        }
        
        # Import and initialize additional transformers
        try:
            from src.transformers.pit_stop_transformer import PitStopFactTransformer
            from src.transformers.race_results_transformer import RaceResultsFactTransformer
            self.fact_transformers['pit_stop'] = PitStopFactTransformer()
            self.fact_transformers['race_results'] = RaceResultsFactTransformer()
        except ImportError as e:
            self.logger.warning(f"Could not import additional transformers: {e}")
        
        # Pipeline statistics
        self.stats = {
            'schema_type': schema_type,
            'start_time': None,
            'end_time': None,
            'total_records_processed': 0,
            'total_fact_records_loaded': 0,
            'total_dimension_records_loaded': 0,
            'total_records_skipped': 0,
            'errors': [],
            'warnings': []
        }
        
        # Store races data for date calculations
        self.races_df = None
        
        # Schema configuration
        self.schema_config = {
            'qualifying': {
                'database': 'f1_qlf_db',
                'fact_table': 'facts',
                'primary_data_source': 'qualifying',
                'date_offset_days': -1  # qualifying = race_date - 1
            },
            'pit_stop': {
                'database': 'f1_pit_db', 
                'fact_table': 'fact_pit_stop',
                'primary_data_source': 'pit_stops',
                'date_offset_days': 0  # pit_stop = race_date
            },
            'race_results': {
                'database': 'f1_res_db',
                'fact_table': 'fact_race_result', 
                'primary_data_source': 'results',
                'date_offset_days': 0  # race_results = race_date
            }
        }
        
        if schema_type not in self.schema_config:
            raise ValueError(f"Unsupported schema type: {schema_type}. Must be one of {list(self.schema_config.keys())}")
        
        self.current_config = self.schema_config[schema_type]
        
        # Set the database name in settings for this schema
        settings.DATABASE_NAME = self.current_config['database']
        self.logger.info(f"Initialized F1 ETL pipeline for {schema_type} schema")
        self.logger.info(f"Target database: {self.current_config['database']}")
    
    def run_pipeline(
        self,
        skip_validation: bool = False,
        sample_size: Optional[int] = None
    ) -> bool:
        """Run the complete F1 ETL pipeline.
        
        Args:
            skip_validation: Skip data integrity validation
            sample_size: Process only N qualifying records (for testing)
            
        Returns:
            True if pipeline completed successfully, False otherwise
        """
        self.stats['start_time'] = time.time()
        
        try:
            self.logger.info("=" * 80)
            self.logger.info(f"STARTING F1 {self.schema_type.upper()} ETL PIPELINE")
            self.logger.info("=" * 80)
            
            self.progress_logger.start_process("Complete F1 ETL Pipeline", 7)
            
            # Step 1: Validate source files
            if not self._validate_source_files():
                return False
            self.progress_logger.log_step("Source file validation completed")
            
            # Step 2: Setup database connection
            if not self._setup_database():
                return False
            self.progress_logger.log_step("Database setup completed")
            
            # Step 3: Extract dimension data
            dimension_data = self._extract_dimensions_data()
            if not dimension_data:
                return False
            self.progress_logger.log_step("Dimension data extraction completed")
            
            # Step 4: Transform dimensional data
            dimensions = self._transform_dimensions(dimension_data)
            if not dimensions:
                return False
            self.progress_logger.log_step("Dimension transformation completed")
            
            # Step 5: Load dimensions and get lookup tables
            lookup_tables = self._load_dimensions(dimensions)
            if not lookup_tables:
                return False
            self.progress_logger.log_step("Dimension loading completed")
            
            # Step 6: Process qualifying data in batches
            if not self._process_fact_data_batched(lookup_tables, sample_size):
                return False
            self.progress_logger.log_step("Fact data batch processing completed")
            
            # Step 7: Post-load operations
            if not self._post_load_operations(skip_validation):
                return False
            self.progress_logger.log_step("Post-load operations completed")
            
            self.progress_logger.complete_process("Complete F1 ETL Pipeline")
            
            # Generate final report
            self._generate_final_report()
            
            self.logger.info("=" * 80)
            self.logger.info("F1 ETL PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            self.logger.error(f"F1 ETL Pipeline failed with error: {e}")
            self.stats['errors'].append(str(e))
            return False
        
        finally:
            self.stats['end_time'] = time.time()
            self._cleanup()
    
    def _validate_source_files(self) -> bool:
        """Validate that all required source files exist and are readable."""
        self.logger.info("Validating source CSV files")
        
        # Check if files exist
        missing_files = settings.validate_file_paths()
        if missing_files:
            self.logger.error(f"Missing required files: {missing_files}")
            return False
        
        # Validate file structure
        validation_results = self.extractor.validate_source_files()
        failed_files = [name for name, status in validation_results.items() if not status]
        
        if failed_files:
            self.logger.error(f"File validation failed for: {failed_files}")
            return False
        
        self.logger.info("All source files validated successfully")
        return True
    
    def _setup_database(self) -> bool:
        """Setup database connection."""
        self.logger.info("Setting up database connection")
        
        # Connect to database
        if not self.loader.connect():
            self.logger.error("Failed to connect to database")
            return False
        
        # Truncate tables if configured
        if not self.loader.truncate_tables(schema_type=self.schema_type):
            self.logger.error("Failed to truncate tables")
            return False
        
        return True
    
    def _extract_dimensions_data(self) -> Dict[str, Any]:
        """Extract dimension data from CSV sources."""
        self.logger.info("Extracting dimension data from CSV files")
        
        try:
            raw_data = {}
            
            # Extract circuits
            raw_data['circuits'] = self.extractor.extract_circuits()
            self.logger.info(f"Extracted {len(raw_data['circuits'])} circuit records")
            
            # Extract constructors
            raw_data['constructors'] = self.extractor.extract_constructors()
            self.logger.info(f"Extracted {len(raw_data['constructors'])} constructor records")
            
            # Extract drivers
            raw_data['drivers'] = self.extractor.extract_drivers()
            self.logger.info(f"Extracted {len(raw_data['drivers'])} driver records")
            
            # Extract races (needed for date dimension)
            self.races_df = self.extractor.extract_races()
            raw_data['races'] = self.races_df
            self.logger.info(f"Extracted {len(raw_data['races'])} race records")
            
            # NOTE: We do NOT extract qualifying here to avoid loading large dataset into memory
            # Qualifying will be processed in batches directly from CSV
            
            return raw_data
            
        except Exception as e:
            self.logger.error(f"Dimension data extraction failed: {e}")
            return {}
    
    def _transform_dimensions(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform all dimensional data for the selected schema."""
        self.logger.info(f"Transforming dimensional data for {self.schema_type} schema")
        
        try:
            dimensions = {}
            
            # Transform circuits (returns tuple: df_for_loading, original_ids)
            dimensions['circuits'] = self.dim_transformer.transform_circuits(raw_data['circuits'])
            
            # Transform constructors (returns tuple: df_for_loading, original_ids)
            dimensions['constructors'] = self.dim_transformer.transform_constructors(raw_data['constructors'])
            
            # Transform drivers (returns tuple: df_for_loading, original_ids)
            dimensions['drivers'] = self.dim_transformer.transform_drivers(raw_data['drivers'])
            
            # Create date dimension from races with schema-specific logic
            dimensions['dates'] = self.dim_transformer.create_date_dimension_for_schema(
                raw_data['races'], self.schema_type
            )
            
            # Add race dimension for pit_stop and race_results schemas
            if self.schema_type in ['pit_stop', 'race_results']:
                dimensions['races'] = self.dim_transformer.transform_races(raw_data['races'])
                self.logger.info(f"Added race dimension for {self.schema_type} schema")
            
            # Log summary for dimensions that return tuples
            tuple_dimensions = ['circuits', 'constructors', 'drivers']
            if self.schema_type in ['pit_stop', 'race_results']:
                tuple_dimensions.append('races')
            
            for dim_name in tuple_dimensions:
                if dim_name in dimensions:
                    df_for_loading, _ = dimensions[dim_name]
                    self.logger.info(f"Dimension {dim_name}: {len(df_for_loading):,} records prepared for AUTO_INCREMENT loading")
            
            # Log date dimension separately
            if 'dates' in dimensions:
                self.logger.info(f"Dimension dates: {len(dimensions['dates']):,} records")
            
            return dimensions
            
        except Exception as e:
            self.logger.error(f"Dimension transformation failed: {e}")
            return {}
    
    def _load_dimensions(self, dimensions: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
        """Load dimensional data and return lookup tables."""
        self.logger.info(f"Loading dimensional data to database for {self.schema_type} schema")
        
        try:
            lookup_tables = self.loader.load_dimensions(dimensions, schema_type=self.schema_type)
            
            if not lookup_tables:
                self.logger.error("Failed to load dimensional data")
                return {}
            
            # Log lookup table sizes
            for dim_name, lookup_dict in lookup_tables.items():
                self.logger.info(f"Created lookup table for {dim_name}: {len(lookup_dict)} entries")
                self.stats['total_dimension_records_loaded'] += len(lookup_dict)
            
            return lookup_tables
            
        except Exception as e:
            self.logger.error(f"Dimension loading failed: {e}")
            return {}
    
    def _process_fact_data_batched(self, lookup_tables: Dict[str, Dict[str, int]], sample_size: Optional[int] = None) -> bool:
        """Process fact data based on schema type."""
        primary_source = self.current_config['primary_data_source']
        self.logger.info(f"Processing {primary_source} data for {self.schema_type} schema")
        
        try:
            if self.schema_type == 'qualifying':
                # For qualifying, use batch processing (large dataset)
                return self._process_qualifying_batched(lookup_tables, sample_size)
            elif self.schema_type == 'pit_stop':
                # For pit stops, load full datasets (smaller)
                return self._process_pit_stops_full(lookup_tables, sample_size)
            elif self.schema_type == 'race_results':
                # For race results, load full datasets (smaller)
                return self._process_race_results_full(lookup_tables, sample_size)
            else:
                self.logger.error(f"Unsupported schema type: {self.schema_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Fact data processing failed: {e}")
            return False
    
    def _process_qualifying_batched(self, lookup_tables: Dict[str, Dict[str, int]], sample_size: Optional[int] = None) -> bool:
        """Process qualifying data in batches for memory efficiency."""
        self.logger.info("Reading qualifying data directly from CSV in batches - no full dataset loading")
        
        try:
            # Get total rows for progress tracking
            total_rows = self.extractor.get_total_rows()
            
            # Adjust total rows if sample_size is specified
            if sample_size and sample_size < total_rows:
                total_rows = sample_size
                self.logger.info(f"Processing sample of {sample_size:,} records from {self.extractor.get_total_rows():,} total")
            
            # Process qualifying data in batches
            batch_count = 0
            total_records_processed = 0
            total_records_skipped = 0
            
            for batch_num, start_row, end_row, batch_df in self.extractor.extract_qualifying_batched():
                
                # Apply sample size limit if specified
                if sample_size and total_records_processed >= sample_size:
                    self.logger.info(f"Reached sample size limit of {sample_size:,} records")
                    break
                
                # If this batch would exceed sample_size, truncate it
                if sample_size and total_records_processed + len(batch_df) > sample_size:
                    remaining = sample_size - total_records_processed
                    batch_df = batch_df.head(remaining)
                    end_row = start_row + len(batch_df) - 1
                    self.logger.info(f"Truncating batch {batch_num} to {remaining:,} records to meet sample size")
                
                # Log batch progress
                percentage = (total_records_processed / total_rows) * 100 if total_rows > 0 else 0
                self.logger.info(f"Processing batch {batch_num}: rows {start_row:,}-{end_row:,} ({len(batch_df)} records) - {percentage:.1f}% complete")
                
                batch_start_time = time.time()
                
                try:
                    # Transform batch with validation and filtering
                    fact_transformer = self.fact_transformers['qualifying']
                    fact_batch = fact_transformer.transform_qualifying(
                        batch_df, lookup_tables, self.races_df, start_row
                    )
                    
                    # Count skipped records
                    records_skipped = len(batch_df) - len(fact_batch)
                    total_records_skipped += records_skipped
                    
                    if fact_batch.empty:
                        self.logger.warning(f"Batch {batch_num} transformation produced empty result - all records skipped")
                        continue
                    
                    # Load batch to database
                    success = self.loader.load_fact_data(fact_batch)
                    
                    if success:
                        # Update progress
                        batch_time = time.time() - batch_start_time
                        records_loaded = len(fact_batch)
                        records_per_sec = records_loaded / batch_time if batch_time > 0 else 0
                        
                        self.logger.info(f"Batch {batch_num} completed: rows {start_row:,}-{end_row:,} ({records_loaded} records processed in {batch_time:.2f}s - {records_per_sec:.0f} records/sec)")
                        
                        # Update statistics
                        batch_count += 1
                        total_records_processed += records_loaded
                        self.stats['total_fact_records_loaded'] += records_loaded
                        self.stats['total_records_skipped'] += records_skipped
                        
                    else:
                        # Log error but continue processing next batch
                        self.logger.error(f"Failed to load batch {batch_num} to database")
                        self.logger.warning(f"Skipping batch {batch_num} due to load failure - continuing with next batch")
                        
                except Exception as batch_error:
                    batch_time = time.time() - batch_start_time
                    self.logger.error(f"Batch {batch_num} processing error: {batch_error}")
                    self.logger.warning(f"Skipping batch {batch_num} due to processing error - continuing with next batch")
                    continue
            
            self.logger.info(f"Batch processing completed successfully:")
            self.logger.info(f"  Total batches processed: {batch_count}")
            self.logger.info(f"  Total records loaded: {total_records_processed:,}")
            self.logger.info(f"  Total records skipped: {total_records_skipped:,}")
            self.stats['total_records_processed'] = total_records_processed
            self.stats['total_records_skipped'] = total_records_skipped
            
            return True
            
        except Exception as e:
            self.logger.error(f"Batch processing failed: {e}")
            return False
    
    def _process_pit_stops_full(self, lookup_tables: Dict[str, Dict[str, int]], sample_size: Optional[int] = None) -> bool:
        """Process pit stop data by loading full datasets.
        
        Args:
            lookup_tables: Dimension lookup tables
            sample_size: Maximum number of records to process (for testing)
            
        Returns:
            True if processing successful, False otherwise
        """
        try:
            self.logger.info("Loading pit stops data")
            
            # Extract pit stops data only (no lap times needed)
            pit_stops_df = self.extractor.extract_pit_stops()
            
            self.logger.info(f"Extracted {len(pit_stops_df):,} pit stop records")
            
            # Apply sample size if specified
            if sample_size:
                pit_stops_df = pit_stops_df.head(sample_size)
                self.logger.info(f"Limited to {len(pit_stops_df):,} pit stop records due to sample size")
            
            # Transform pit stop data (simplified - no lap times joining)
            fact_transformer = self.fact_transformers['pit_stop']
            fact_df = fact_transformer.transform_pit_stops(
                pit_stops_df, lookup_tables, self.races_df
            )
            
            if fact_df.empty:
                self.logger.warning("Pit stop transformation produced no valid records")
                self.stats['total_records_processed'] = 0
                self.stats['total_records_skipped'] = len(pit_stops_df)
                return True
            
            # Load to database
            self.logger.info(f"Loading {len(fact_df):,} pit stop fact records")
            success = self.loader.load_fact_data(fact_df, schema_type=self.schema_type)
            
            if success:
                self.stats['total_records_processed'] = len(fact_df)
                self.stats['total_fact_records_loaded'] = len(fact_df)
                self.stats['total_records_skipped'] = len(pit_stops_df) - len(fact_df)
                self.logger.info("Pit stop data processing completed successfully")
                return True
            else:
                self.logger.error("Failed to load pit stop fact data")
                return False
                
        except Exception as e:
            self.logger.error(f"Pit stop processing failed: {e}")
            return False
    
    def _process_race_results_full(self, lookup_tables: Dict[str, Dict[str, int]], sample_size: Optional[int] = None) -> bool:
        """Process race results data by loading full datasets.
        
        Args:
            lookup_tables: Dimension lookup tables
            sample_size: Maximum number of records to process (for testing)
            
        Returns:
            True if processing successful, False otherwise
        """
        try:
            self.logger.info("Loading race results and status data")
            
            # Extract results and status data
            results_df = self.extractor.extract_results()
            status_df = self.extractor.extract_status()
            
            self.logger.info(f"Extracted {len(results_df):,} race result records")
            self.logger.info(f"Extracted {len(status_df):,} status records")
            
            # Apply sample size if specified
            if sample_size:
                results_df = results_df.head(sample_size)
                self.logger.info(f"Limited to {len(results_df):,} result records due to sample size")
            
            # Transform race results data
            fact_transformer = self.fact_transformers['race_results']
            fact_df = fact_transformer.transform_race_results(
                results_df, status_df, lookup_tables, self.races_df
            )
            
            if fact_df.empty:
                self.logger.warning("Race results transformation produced no valid records")
                self.stats['total_records_processed'] = 0
                self.stats['total_records_skipped'] = len(results_df)
                return True
            
            # Load to database
            self.logger.info(f"Loading {len(fact_df):,} race result fact records")
            success = self.loader.load_fact_data(fact_df, schema_type=self.schema_type)
            
            if success:
                self.stats['total_records_processed'] = len(fact_df)
                self.stats['total_fact_records_loaded'] = len(fact_df)
                self.stats['total_records_skipped'] = len(results_df) - len(fact_df)
                self.logger.info("Race results data processing completed successfully")
                return True
            else:
                self.logger.error("Failed to load race results fact data")
                return False
                
        except Exception as e:
            self.logger.error(f"Race results processing failed: {e}")
            return False

    def _post_load_operations(self, skip_validation: bool) -> bool:
        """Perform post-load operations like data validation."""
        self.logger.info("Performing post-load operations")
        
        try:
            # Validate data integrity
            if not skip_validation:
                validation_results = self.loader.validate_data_integrity()
                self._log_validation_results(validation_results)
            else:
                self.logger.info("Skipping data integrity validation")
            
            # Get final table counts
            table_counts = self.loader.get_dimension_counts()
            self.logger.info("Final table record counts:")
            for table, count in table_counts.items():
                self.logger.info(f"  {table}: {count:,} records")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Post-load operations failed: {e}")
            return False
    
    def _log_validation_results(self, validation_results: Dict[str, Any]):
        """Log data validation results."""
        self.logger.info("Data Integrity Validation Results:")
        
        # Check for orphaned records
        orphan_checks = [
            'orphaned_circuits', 'orphaned_constructors', 'orphaned_drivers', 'orphaned_dates'
        ]
        
        total_orphans = sum(validation_results.get(check, 0) for check in orphan_checks)
        if total_orphans > 0:
            self.logger.warning(f"Found {total_orphans} orphaned records")
            for check in orphan_checks:
                count = validation_results.get(check, 0)
                if count > 0:
                    self.logger.warning(f"  {check}: {count}")
        else:
            self.logger.info("No orphaned records found")
        
        # Check for data quality issues
        quality_issues = sum(validation_results.get(check, 0) for check in 
                           ['invalid_positions', 'extreme_q1_times', 'extreme_q2_times', 'extreme_q3_times',
                            'inconsistent_status_ok', 'inconsistent_status_dnq', 'inconsistent_status_dns'])
        
        if quality_issues > 0:
            self.logger.warning(f"Found {quality_issues} data quality issues")
            for check, count in validation_results.items():
                if check.startswith(('invalid_', 'extreme_', 'inconsistent_')) and count > 0:
                    self.logger.warning(f"  {check}: {count}")
        else:
            self.logger.info("No data quality issues found")
    
    def _generate_final_report(self):
        """Generate final ETL pipeline report."""
        # Handle case where end_time might not be set due to early failure
        if self.stats['end_time'] and self.stats['start_time']:
            total_time = self.stats['end_time'] - self.stats['start_time']
        else:
            total_time = 0
        
        # Schema-specific record counts
        primary_source = self.current_config['primary_data_source']
        total_records_processed = self.stats.get('total_records_processed', 
                                                self.stats.get('total_qualifying_processed', 0))
        
        self.logger.info("=" * 50)
        self.logger.info(f"F1 {self.schema_type.upper()} ETL PIPELINE SUMMARY REPORT")
        self.logger.info("=" * 50)
        self.logger.info(f"Schema: {self.schema_type}")
        self.logger.info(f"Primary data source: {primary_source}")
        self.logger.info(f"Total execution time: {total_time:.2f} seconds")
        self.logger.info(f"{primary_source.capitalize()} records processed: {total_records_processed:,}")
        self.logger.info(f"Dimension records loaded: {self.stats['total_dimension_records_loaded']:,}")
        self.logger.info(f"Fact records loaded: {self.stats['total_fact_records_loaded']:,}")
        self.logger.info(f"Total records loaded: {self.stats['total_dimension_records_loaded'] + self.stats['total_fact_records_loaded']:,}")
        self.logger.info(f"Total records skipped: {self.stats['total_records_skipped']:,}")
        
        if self.stats['warnings']:
            self.logger.info(f"Warnings: {len(self.stats['warnings'])}")
            for warning in self.stats['warnings']:
                self.logger.info(f"  - {warning}")
        
        if self.stats['errors']:
            self.logger.info(f"Errors: {len(self.stats['errors'])}")
            for error in self.stats['errors']:
                self.logger.info(f"  - {error}")
        
        # Performance metrics
        if total_time > 0 and total_records_processed > 0:
            records_per_sec = total_records_processed / total_time
            total_loaded = self.stats['total_dimension_records_loaded'] + self.stats['total_fact_records_loaded']
            total_load_rate = total_loaded / total_time
            self.logger.info(f"{primary_source.capitalize()} processing rate: {records_per_sec:.0f} records/sec")
            self.logger.info(f"Total loading rate: {total_load_rate:.0f} records/sec")
        
        self.logger.info("=" * 50)
    
    def _cleanup(self):
        """Cleanup resources."""
        if self.loader:
            self.loader.disconnect()


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='F1 Multi-Schema ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--schema',
        type=str,
        choices=['qualifying', 'pit_stop', 'race_results'],
        default='qualifying',
        help='Schema type to process (default: qualifying)'
    )
    
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip data integrity validation'
    )
    
    parser.add_argument(
        '--sample-size',
        type=int,
        help='Process only N records (for testing)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Create logs directory
    settings.create_log_directory()
    
    # Initialize ETL with selected schema
    try:
        etl = F1ETL(schema_type=args.schema)
        print(f"Initialized F1 ETL pipeline for {args.schema} schema")
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    
    # Run the pipeline
    success = etl.run_pipeline(
        skip_validation=args.skip_validation,
        sample_size=args.sample_size
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
