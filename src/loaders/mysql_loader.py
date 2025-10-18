"""MySQL database loading module for the F1 Qualifying ETL Pipeline."""

import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List, Optional, Any
import time

from ..config.settings import settings
from ..utils.logging_config import get_logger, ETLProgressLogger


class MySQLLoader:
    """Handles loading data into MySQL database tables.
    
    This class focuses purely on data loading operations including:
    - Loading dimensional data with smart duplicate handling
    - Loading fact table data in batches
    - Data integrity validation
    
    Note: Database schema and index creation is handled externally.
    """
    
    def __init__(self):
        """Initialize the MySQL loader."""
        self.logger = get_logger(__name__)
        self.progress_logger = ETLProgressLogger(self.logger)
        self.engine = None
        self.session_factory = None
        
    def connect(self) -> bool:
        """Establish connection to MySQL database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.logger.info("Connecting to MySQL database")
            
            # Create SQLAlchemy engine
            self.engine = create_engine(
                settings.database_url,
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,   # Recycle connections after 1 hour
                echo=False           # Set to True for SQL debugging
            )
            
            # Test the connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            
            # Create session factory
            self.session_factory = sessionmaker(bind=self.engine)
            
            self.logger.info("Successfully connected to MySQL database")
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")
    
    def check_table_has_data(self, table_name: str) -> bool:
        """Check if a table has any data records.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table has data, False if empty or doesn't exist
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.fetchone()[0]
                has_data = count > 0
                
                self.logger.debug(f"Table {table_name} has {count:,} records")
                return has_data
                
        except Exception as e:
            # Table might not exist or other error - assume no data
            self.logger.debug(f"Could not check table {table_name}: {e}")
            return False
    
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'append',
        batch_size: Optional[int] = None
    ) -> bool:
        """Load a DataFrame into a database table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: What to do if table exists ('append', 'replace', 'fail')
            batch_size: Number of records to insert per batch
            
        Returns:
            True if loading successful, False otherwise
        """
        if df.empty:
            self.logger.warning(f"Empty DataFrame provided for table {table_name}")
            return True
        
        batch_size = batch_size or settings.BATCH_SIZE
        total_records = len(df)
        
        self.logger.info(f"Loading {total_records:,} records into table: {table_name}")
        self.progress_logger.start_process(f"Loading {table_name}", 0)
        
        try:
            start_time = time.time()
            
            # Load data in batches
            records_loaded = 0
            batch_num = 1
            
            for i in range(0, total_records, batch_size):
                batch_df = df.iloc[i:i + batch_size]
                batch_start_time = time.time()
                
                batch_df.to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists=if_exists if i == 0 else 'append',
                    index=False,
                    method='multi'
                )
                
                records_loaded += len(batch_df)
                batch_time = time.time() - batch_start_time
                records_per_second = len(batch_df) / batch_time if batch_time > 0 else 0
                
                self.progress_logger.log_step(
                    f"Loaded batch {batch_num} ({records_loaded:,}/{total_records:,} records) "
                    f"- {records_per_second:.0f} records/sec",
                    len(batch_df)
                )
                
                batch_num += 1
            
            total_time = time.time() - start_time
            avg_records_per_second = total_records / total_time if total_time > 0 else 0
            
            self.progress_logger.complete_process(
                f"Loading {table_name}",
                total_records
            )
            self.logger.info(
                f"Successfully loaded {total_records:,} records in {total_time:.2f} seconds "
                f"({avg_records_per_second:.0f} records/sec average)"
            )
            
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error loading data into {table_name}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error loading data into {table_name}: {e}")
            return False
    
    def load_dimensions(self, dimensions: Dict[str, tuple], schema_type: str = 'qualifying') -> Dict[str, Dict[str, int]]:
        """Load all dimensional data and create CSV ID → AUTO_INCREMENT key mappings.
        
        Args:
            dimensions: Dictionary of dimension name -> (DataFrame_for_loading, original_ids_df)
            schema_type: Type of schema ('qualifying', 'pit_stop', 'race_results')
            
        Returns:
            Dictionary of lookup tables mapping CSV IDs to AUTO_INCREMENT keys
        """
        self.logger.info(f"Loading dimensional data with AUTO_INCREMENT keys for {schema_type} schema")
        self.progress_logger.start_process("Dimension Loading", len(dimensions))
        
        lookup_tables = {}
        
        # Define the loading order based on schema type
        if schema_type in ['pit_stop', 'race_results']:
            dimension_order = ['circuits', 'constructors', 'drivers', 'dates', 'races']
        else:
            dimension_order = ['circuits', 'constructors', 'drivers', 'dates']
        
        for dim_name in dimension_order:
            if dim_name not in dimensions:
                self.logger.warning(f"Dimension {dim_name} not found in input data")
                continue
            
            # Handle different return types from transformers
            if dim_name == 'dates':
                # Date dimension returns only DataFrame (no original IDs)
                df_for_loading = dimensions[dim_name]
                original_ids_df = None
            else:
                # Other dimensions return (DataFrame, original_ids_df)
                df_for_loading, original_ids_df = dimensions[dim_name]
            
            table_name = f"dim_{dim_name[:-1]}" if dim_name.endswith('s') else f"dim_{dim_name}"
            
            # Special handling for dimension table names
            table_mapping = {
                'circuits': 'dim_circuit',
                'constructors': 'dim_constructor',
                'drivers': 'dim_driver',
                'dates': 'dim_date',
                'races': 'dim_race'
            }
            table_name = table_mapping.get(dim_name, table_name)
            
            # Smart loading: check if table already has data
            if self.check_table_has_data(table_name):
                self.logger.info(f"Table {table_name} already contains data - creating lookup from existing data")
                lookup_tables[dim_name] = self._create_auto_increment_lookup(table_name, dim_name, original_ids_df)
                self.progress_logger.log_step(f"Reused existing dimension: {dim_name}", 0)
            else:
                # Table is empty, load new data and create mapping
                self.logger.info(f"Table {table_name} is empty - inserting {len(df_for_loading):,} records")
                success = self.load_dataframe(df_for_loading, table_name)
                
                if success:
                    # Create lookup table mapping CSV IDs to AUTO_INCREMENT keys
                    lookup_tables[dim_name] = self._create_auto_increment_lookup(table_name, dim_name, original_ids_df)
                    self.progress_logger.log_step(f"Loaded new dimension: {dim_name}", len(df_for_loading))
                else:
                    self.logger.error(f"Failed to load dimension: {dim_name}")
                    return {}
        
        total_records = sum(len(df_for_loading) if isinstance(dimensions[dim], tuple) else len(dimensions[dim])
                           for dim in dimension_order if dim in dimensions)
        self.progress_logger.complete_process("Dimension Loading", total_records)
        return lookup_tables
    
    def load_fact_data(self, fact_df: pd.DataFrame, schema_type: str = 'qualifying') -> bool:
        """Load fact table data based on schema type.
        
        Args:
            fact_df: DataFrame containing fact data
            schema_type: Type of schema ('qualifying', 'pit_stop', 'race_results')
            
        Returns:
            True if loading successful, False otherwise
        """
        # Map schema type to table name
        table_mapping = {
            'qualifying': 'facts',
            'pit_stop': 'fact_pit_stop',
            'race_results': 'fact_race_result'
        }
        
        table_name = table_mapping.get(schema_type, 'facts')
        self.logger.info(f"Loading fact data for {schema_type} schema into table: {table_name}")
        
        return self.load_dataframe(fact_df, table_name)
    
    def _create_auto_increment_lookup(self, table_name: str, dimension_name: str, original_ids_df: Optional[pd.DataFrame]) -> Dict[int, int]:
        """Create a lookup dictionary mapping CSV IDs to AUTO_INCREMENT keys.
        
        Args:
            table_name: Database table name
            dimension_name: Logical dimension name
            original_ids_df: DataFrame with original CSV IDs (in insertion order)
            
        Returns:
            Dictionary mapping CSV IDs to AUTO_INCREMENT keys
        """
        try:
            if dimension_name == 'dates':
                # Date dimension uses date_key as both CSV and DB key
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT date_key FROM {table_name} ORDER BY date_key"))
                    date_keys = [row[0] for row in result.fetchall()]
                    # For dates, CSV key = DB key
                    lookup_dict = {key: key for key in date_keys}
                
                self.logger.info(f"Created date lookup with {len(lookup_dict)} entries")
                return lookup_dict
            
            if original_ids_df is None:
                self.logger.warning(f"No original IDs provided for {dimension_name}")
                return {}
            
            # Get AUTO_INCREMENT keys in insertion order
            key_column_mapping = {
                'circuits': 'circuit_key',
                'constructors': 'constructor_key',
                'drivers': 'driver_key',
                'races': 'race_key'
            }
            
            key_column = key_column_mapping.get(dimension_name)
            if not key_column:
                self.logger.warning(f"No key column mapping for dimension: {dimension_name}")
                return {}
            
            # Query AUTO_INCREMENT keys in insertion order
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT {key_column} FROM {table_name} ORDER BY {key_column}"))
                auto_increment_keys = [row[0] for row in result.fetchall()]
            
            # Map CSV IDs to AUTO_INCREMENT keys based on insertion order
            original_id_column = {
                'circuits': 'circuitId',
                'constructors': 'constructorId',
                'drivers': 'driverId',
                'races': 'raceId'
            }[dimension_name]
            
            csv_ids = original_ids_df[original_id_column].tolist()
            
            if len(csv_ids) != len(auto_increment_keys):
                self.logger.error(f"Mismatch in {dimension_name}: {len(csv_ids)} CSV IDs vs {len(auto_increment_keys)} DB keys")
                return {}
            
            # Create mapping: CSV_ID → AUTO_INCREMENT_KEY
            lookup_dict = dict(zip(csv_ids, auto_increment_keys))
            
            self.logger.info(f"Created AUTO_INCREMENT lookup for {dimension_name} with {len(lookup_dict)} entries")
            return lookup_dict
            
        except Exception as e:
            self.logger.error(f"Error creating AUTO_INCREMENT lookup for {dimension_name}: {e}")
            return {}
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for a database table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table statistics
        """
        try:
            stats = {}
            
            with self.engine.connect() as conn:
                # Get row count
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                stats['row_count'] = result.fetchone()[0]
                
                # Get table size information
                size_query = """
                SELECT 
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                FROM information_schema.TABLES 
                WHERE table_schema = DATABASE() AND table_name = :table_name
                """
                result = conn.execute(text(size_query), {"table_name": table_name})
                size_row = result.fetchone()
                stats['size_mb'] = size_row[0] if size_row else 0
                
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting stats for table {table_name}: {e}")
            return {}
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """Validate data integrity across all F1 tables.
        
        Returns:
            Dictionary with validation results
        """
        self.logger.info("Validating F1 data integrity")
        
        validation_results = {}
        
        try:
            with self.engine.connect() as conn:
                # Check for orphaned records in fact table
                orphan_queries = {
                    'orphaned_circuits': """
                        SELECT COUNT(*) FROM facts f 
                        LEFT JOIN dim_circuit c ON f.dim_circuit_circuit_key = c.circuit_key 
                        WHERE c.circuit_key IS NULL
                    """,
                    'orphaned_constructors': """
                        SELECT COUNT(*) FROM facts f 
                        LEFT JOIN dim_constructor c ON f.dim_constructor_constructor_key = c.constructor_key 
                        WHERE c.constructor_key IS NULL
                    """,
                    'orphaned_drivers': """
                        SELECT COUNT(*) FROM facts f 
                        LEFT JOIN dim_driver d ON f.dim_driver_driver_key = d.driver_key 
                        WHERE d.driver_key IS NULL
                    """,
                    'orphaned_dates': """
                        SELECT COUNT(*) FROM facts f 
                        LEFT JOIN dim_date dt ON f.dim_date_date_key = dt.date_key 
                        WHERE dt.date_key IS NULL
                    """
                }
                
                for check_name, query in orphan_queries.items():
                    result = conn.execute(text(query))
                    validation_results[check_name] = result.fetchone()[0]
                
                # Check for data quality issues
                quality_queries = {
                    'invalid_positions': "SELECT COUNT(*) FROM facts WHERE position < 0 OR position > 30",
                    'extreme_q1_times': "SELECT COUNT(*) FROM facts WHERE q1_ms < 60000 OR q1_ms > 600000",
                    'extreme_q2_times': "SELECT COUNT(*) FROM facts WHERE q2_ms < 60000 OR q2_ms > 600000", 
                    'extreme_q3_times': "SELECT COUNT(*) FROM facts WHERE q3_ms < 60000 OR q3_ms > 600000",
                    'inconsistent_status_ok': """
                        SELECT COUNT(*) FROM facts 
                        WHERE status = 'OK' AND (q1_ms IS NULL OR q2_ms IS NULL OR q3_ms IS NULL)
                    """,
                    'inconsistent_status_dnq': """
                        SELECT COUNT(*) FROM facts 
                        WHERE status = 'DNQ' AND (q1_ms IS NULL OR q2_ms IS NOT NULL)
                    """,
                    'inconsistent_status_dns': """
                        SELECT COUNT(*) FROM facts 
                        WHERE status = 'DNS' AND q1_ms IS NOT NULL
                    """
                }
                
                for check_name, query in quality_queries.items():
                    result = conn.execute(text(query))
                    validation_results[check_name] = result.fetchone()[0]
                
            self.logger.info("Data integrity validation completed")
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error validating data integrity: {e}")
            return {}
    
    def get_dimension_counts(self) -> Dict[str, int]:
        """Get record counts for all dimension tables.
        
        Returns:
            Dictionary with table names and their record counts
        """
        tables = ['dim_circuit', 'dim_constructor', 'dim_driver', 'dim_date', 'facts']
        counts = {}
        
        try:
            with self.engine.connect() as conn:
                for table in tables:
                    try:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        counts[table] = result.fetchone()[0]
                    except Exception as e:
                        self.logger.warning(f"Could not get count for {table}: {e}")
                        counts[table] = 0
            
            return counts
            
        except Exception as e:
            self.logger.error(f"Error getting dimension counts: {e}")
            return {}
    
    def truncate_tables(self, schema_type: str = 'qualifying') -> bool:
        """Truncate all tables for fresh data load.
        
        Args:
            schema_type: Type of schema to determine which fact table to truncate
        
        Returns:
            True if successful, False otherwise
        """
        if not settings.TRUNCATE_TABLES:
            self.logger.info("Table truncation disabled in settings")
            return True
        
        self.logger.info(f"Truncating all tables for fresh data load ({schema_type} schema)")
        
        # Map schema to fact table name
        fact_table_mapping = {
            'qualifying': 'facts',
            'pit_stop': 'fact_pit_stop',
            'race_results': 'fact_race_result'
        }
        
        fact_table = fact_table_mapping.get(schema_type, 'facts')
        
        # Order matters due to foreign key constraints - fact table first
        tables_to_truncate = [fact_table, 'dim_race', 'dim_date', 'dim_driver', 'dim_constructor', 'dim_circuit']
        
        # For qualifying schema, don't try to truncate dim_race (doesn't exist)
        if schema_type == 'qualifying':
            tables_to_truncate.remove('dim_race')
        
        try:
            with self.engine.connect() as conn:
                # Disable foreign key checks temporarily
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
                
                for table in tables_to_truncate:
                    try:
                        conn.execute(text(f"TRUNCATE TABLE {table}"))
                        self.logger.info(f"Truncated table: {table}")
                    except Exception as e:
                        self.logger.warning(f"Could not truncate {table}: {e}")
                
                # Re-enable foreign key checks
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
                conn.commit()
            
            self.logger.info("Table truncation completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error truncating tables: {e}")
            return False
