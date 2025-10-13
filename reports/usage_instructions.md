# F1 Qualifying ETL Pipeline - Usage Instructions

## Prerequisites

### System Requirements
- Python 3.8 or higher
- MySQL 5.7 or higher (or MySQL 8.0)
- Minimum 4GB RAM (8GB recommended for full dataset)
- 2GB free disk space

### Required Software
- MySQL Server running and accessible
- MySQL Workbench (optional, for database management)
- Python package manager (pip)

## Installation and Setup

### 1. Database Setup

#### Create Database Schema
```bash
# Connect to MySQL and create the database schema
mysql -u azalia2 -p < create_databade_schema.sql
```

This will create:
- Database schema `f1_qlf_db`
- All required tables (`dim_circuit`, `dim_constructor`, `dim_driver`, `dim_date`, `facts`)
- Proper indexes and foreign key constraints

#### Verify Database Setup
```sql
-- Connect to MySQL and verify tables
USE f1_qlf_db;
SHOW TABLES;
DESCRIBE facts;
```

### 2. Python Environment Setup

#### Install Dependencies
```bash
# Install required Python packages
pip install -r requirements.txt
```

#### Configure Environment
The pipeline uses the provided `.env` file for configuration. Default settings:

```bash
# Database Connection
DATABASE_HOST=127.0.0.1
DATABASE_PORT=3306
DATABASE_USER=azalia2
DATABASE_PASSWORD=123456
DATABASE_NAME=f1_qlf_db

# Processing Configuration  
BATCH_SIZE=10000
LOG_LEVEL=INFO
TRUNCATE_TABLES=true
```

**Important**: Ensure your MySQL credentials match the `.env` file settings.

### 3. Data Preparation

#### Verify Data Files
Ensure all required CSV files are present in the `data/` directory:
- `circuits.csv`
- `constructors.csv` 
- `drivers.csv`
- `races.csv`
- `qualifying.csv`

#### Validate Data Files
```bash
# Test file validation
python run_etl.py --help
```

## Running the Pipeline

### Basic Usage

#### Full Pipeline Execution
```bash
# Run complete ETL pipeline with all data
python run_etl.py
```

This will:
1. Validate all source CSV files
2. Connect to MySQL database
3. Extract and transform dimensional data
4. Load dimensions and create lookup tables
5. Process qualifying data in batches
6. Validate data integrity
7. Generate processing report

#### Sample Data Processing (Testing)
```bash
# Process only first 1000 qualifying records
python run_etl.py --sample-size 1000

# Process first 100 records for quick testing
python run_etl.py --sample-size 100
```

#### Skip Validation (Faster Processing)
```bash
# Skip data integrity validation for faster processing
python run_etl.py --skip-validation
```

### Advanced Usage

#### Resumable Processing

The pipeline supports resumable processing for large datasets:

```bash
# Check current processing state
python run_etl.py --show-state

# Start fresh (ignore previous state)
python run_etl.py --fresh-start

# Resume from specific row position
python run_etl.py --reset-position --start-row 50000
```

#### Combined Options
```bash
# Sample processing with fresh start
python run_etl.py --fresh-start --sample-size 5000

# Resume processing without validation
python run_etl.py --skip-validation
```

## Configuration Options

### Environment Variables

#### Database Configuration
```bash
DATABASE_HOST=127.0.0.1      # MySQL server host
DATABASE_PORT=3306           # MySQL server port  
DATABASE_USER=azalia2        # MySQL username
DATABASE_PASSWORD=123456     # MySQL password
DATABASE_NAME=f1_qlf_db      # Target database name
```

#### Processing Configuration
```bash
BATCH_SIZE=10000            # Records per batch (adjust for hardware)
LOG_LEVEL=INFO              # Logging verbosity (DEBUG, INFO, WARNING, ERROR)
TRUNCATE_TABLES=true        # Clear tables before loading
```

#### Performance Tuning
- **Small Systems**: `BATCH_SIZE=1000` (1K records per batch)
- **Medium Systems**: `BATCH_SIZE=10000` (10K records per batch) 
- **Large Systems**: `BATCH_SIZE=50000` (50K records per batch)

### File Path Configuration
```bash
DATA_PATH=data/             # Directory containing CSV files
CIRCUITS_FILE=circuits.csv  # Circuit data file
CONSTRUCTORS_FILE=constructors.csv
DRIVERS_FILE=drivers.csv
RACES_FILE=races.csv
QUALIFYING_FILE=qualifying.csv
```

## Monitoring and Troubleshooting

### Log Files

#### Real-time Monitoring
```bash
# Monitor pipeline progress in real-time
tail -f logs/f1_etl_run_YYYYMMDD_HHMMSS.log
```

#### Log Locations
- **Per-run logs**: `logs/f1_etl_run_YYYYMMDD_HHMMSS.log`
- **General logs**: `logs/etl_pipeline.log`
- **State file**: `f1_etl_state.json`

### Progress Tracking

#### Check Processing State
```bash
python run_etl.py --show-state
```

Output example:
```
Current F1 ETL Batch Processing State:
========================================
Status              : processing
Run ID              : 20241005_143022
Last Batch          : 15
Last Row            : 150,000
Batch Size          : 10,000
Total Rows          : 1,000,000
Records Processed   : 145,230
Records Skipped     : 4,770
Progress            : 15.0%
```

### Common Issues and Solutions

#### Database Connection Issues
```bash
# Error: "Access denied for user"
# Solution: Verify credentials in .env file
# Check: MySQL user permissions

# Error: "Can't connect to MySQL server"  
# Solution: Ensure MySQL server is running
# Check: Host and port configuration
```

#### Memory Issues
```bash
# Error: "Memory error" or system slowdown
# Solution: Reduce batch size in .env
BATCH_SIZE=1000

# For very limited systems:
BATCH_SIZE=500
```

#### Data Validation Errors
```bash
# Error: "Missing required columns"
# Solution: Verify CSV file format and column names

# Error: "File not found"
# Solution: Check data/ directory and file names
```

#### Processing Interruption
```bash
# If pipeline is interrupted:
# 1. Check state
python run_etl.py --show-state

# 2. Resume from last position
python run_etl.py

# 3. Or start fresh if needed
python run_etl.py --fresh-start
```

## Data Quality and Validation

### Automatic Validation

The pipeline performs comprehensive validation:

#### Input Validation
- File existence and readability
- Required column presence
- Data type validation
- Basic data quality checks

#### Processing Validation
- Foreign key reference validation
- Time format validation (MM:SS.SSS)
- Position range validation (1-30)
- Status consistency validation

#### Output Validation
- Orphaned record detection
- Data integrity checks
- Business rule validation
- Statistical outlier detection

### Manual Validation

#### Database Queries
```sql
-- Check dimension counts
SELECT 'circuits' as table_name, COUNT(*) as count FROM dim_circuit
UNION ALL
SELECT 'constructors', COUNT(*) FROM dim_constructor  
UNION ALL
SELECT 'drivers', COUNT(*) FROM dim_driver
UNION ALL
SELECT 'dates', COUNT(*) FROM dim_date
UNION ALL
SELECT 'facts', COUNT(*) FROM facts;

-- Check status distribution
SELECT status, COUNT(*) as count 
FROM facts 
GROUP BY status 
ORDER BY count DESC;

-- Check for data quality issues
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN q1_ms IS NOT NULL THEN 1 ELSE 0 END) as has_q1,
    SUM(CASE WHEN q2_ms IS NOT NULL THEN 1 ELSE 0 END) as has_q2,
    SUM(CASE WHEN q3_ms IS NOT NULL THEN 1 ELSE 0 END) as has_q3
FROM facts;
```

## Performance Expectations

### Processing Times (Approximate)

#### Full Dataset (~10,000 qualifying records)
- **Small System** (4GB RAM): 15-30 minutes
- **Medium System** (8GB RAM): 5-15 minutes  
- **Large System** (16GB+ RAM): 2-5 minutes

#### Sample Processing (1,000 records)
- **Any System**: 1-3 minutes

### Resource Usage
- **Memory**: 500MB - 2GB (depending on batch size)
- **Disk**: 100MB for logs and state files
- **Network**: Minimal (local database connections)

## Troubleshooting Guide

### Performance Issues

#### Slow Processing
1. **Reduce batch size**: Set `BATCH_SIZE=1000` in `.env`
2. **Check database performance**: Ensure MySQL has adequate resources
3. **Monitor system resources**: Check RAM and CPU usage

#### Memory Issues
1. **Lower batch size**: Use `BATCH_SIZE=500` for limited systems
2. **Close other applications**: Free up system memory
3. **Check available RAM**: Ensure minimum 4GB available

### Data Issues

#### High Skip Rates
```bash
# Check log files for skip reasons
grep "Skipped.*invalid records" logs/f1_etl_run_*.log

# Common causes:
# - Missing driver/constructor references
# - Invalid race date references  
# - Corrupted CSV data
```

#### Validation Failures
```bash
# Run with detailed logging
LOG_LEVEL=DEBUG python run_etl.py --sample-size 100

# Check specific validation results
python run_etl.py --sample-size 1000
# Review validation section in logs
```

### Recovery Procedures

#### Complete Reset
```bash
# Clear all data and start fresh
python run_etl.py --fresh-start

# Truncate database tables manually if needed
mysql -u azalia2 -p mydb -e "
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE facts;
TRUNCATE TABLE dim_date;
TRUNCATE TABLE dim_driver;
TRUNCATE TABLE dim_constructor;
TRUNCATE TABLE dim_circuit;
SET FOREIGN_KEY_CHECKS = 1;
"
```

#### Partial Recovery
```bash
# Resume from specific position
python run_etl.py --reset-position --start-row 25000

# Process remaining data only
python run_etl.py
```

## Support and Maintenance

### Regular Maintenance
1. **Log Cleanup**: Periodically clean old log files from `logs/` directory
2. **State Cleanup**: Remove old state files after successful runs
3. **Database Maintenance**: Regular MySQL optimization and backup

### Monitoring
1. **Check log files** for warnings and errors
2. **Monitor processing times** for performance degradation
3. **Validate data quality** after each run

### Updates and Modifications
1. **Configuration changes**: Update `.env` file as needed
2. **Code modifications**: Follow modular architecture for changes
3. **Schema changes**: Update both SQL schema and Python code

For additional support or questions, refer to the pipeline logs and validation reports generated during execution.