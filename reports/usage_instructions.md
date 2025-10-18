# F1 Multi-Schema ETL Pipeline - Usage Instructions

## Overview

This ETL pipeline supports three F1 dimensional modeling schemas:
- **Qualification Sessions** (`qualifying`) - Driver performance during qualifying rounds
- **Pit Stops** (`pit_stop`) - Pit stop performance analysis  
- **Race Final Positions** (`race_results`) - Race results and championship points

## Prerequisites

### System Requirements
- Python 3.8 or higher
- MySQL 5.7 or higher (or MySQL 8.0)

### Required Software
- MySQL Server running and accessible
- MySQL Workbench (optional, for database management)
- Python package manager (pip)

## Installation and Setup

### 1. Database Schema Setup

#### Create All Three Schema Databases
```bash
# Set up qualification sessions schema
mysql -u azalia2 -p < schemas/qualification_sessions_schema.sql

# Set up pit stops schema  
mysql -u azalia2 -p < schemas/pit_stop_schema.sql

# Set up race results schema
mysql -u azalia2 -p < schemas/race_final_positions_schema.sql
```

This creates three separate databases:
- `f1_qlf_db` - Qualification sessions analysis
- `f1_pit_db` - Pit stop performance analysis
- `f1_res_db` - Race final positions analysis

#### Verify Database Setup
```sql
-- Check all databases exist
SHOW DATABASES LIKE 'f1_%';

-- Verify qualification tables
USE f1_qlf_db; SHOW TABLES;

-- Verify pit stop tables  
USE f1_pit_db; SHOW TABLES;

-- Verify race results tables
USE f1_res_db; SHOW TABLES;
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
# Database Connection (dynamically switched per schema)
DATABASE_HOST=127.0.0.1
DATABASE_PORT=3306
DATABASE_USER=azalia2
DATABASE_PASSWORD=123456

# Processing Configuration  
BATCH_SIZE=10000
LOG_LEVEL=INFO
TRUNCATE_TABLES=true
```

**Important**: Database name is automatically selected based on schema type.

### 3. Data Preparation

#### Required Data Files
Ensure all CSV files are present in the `data/` directory:

**Core Files (used by all schemas):**
- `circuits.csv` - Circuit information
- `constructors.csv` - Constructor data
- `drivers.csv` - Driver information
- `races.csv` - Race metadata

**Schema-Specific Files:**
- `qualifying.csv` - For qualification sessions schema
- `pit_stops.csv` - For pit stop schema
- `results.csv` + `status.csv` - For race results schema

## Running the Pipeline

### Schema Selection

The pipeline supports three schemas via the `--schema` parameter:

```bash
# Process qualification sessions (default)
python run_etl.py --schema qualifying

# Process pit stop data
python run_etl.py --schema pit_stop

# Process race results
python run_etl.py --schema race_results
```

### Basic Usage Examples

#### Full Processing
```bash
# Process complete qualifying dataset
python run_etl.py --schema qualifying

# Process all pit stop data
python run_etl.py --schema pit_stop

# Process all race results
python run_etl.py --schema race_results
```

#### Sample Processing (Recommended for Testing)
```bash
# Test with small samples
python run_etl.py --schema qualifying --sample-size 100
python run_etl.py --schema pit_stop --sample-size 50  
python run_etl.py --schema race_results --sample-size 50

# Medium samples for validation
python run_etl.py --schema qualifying --sample-size 1000
python run_etl.py --schema pit_stop --sample-size 500
python run_etl.py --schema race_results --sample-size 500
```

#### Skip Validation (Faster Processing)
```bash
# Skip data integrity validation
python run_etl.py --schema qualifying --skip-validation
python run_etl.py --schema pit_stop --skip-validation
python run_etl.py --schema race_results --skip-validation
```

### Advanced Usage

#### Combined Options
```bash
# Sample processing without validation (fastest)
python run_etl.py --schema pit_stop --sample-size 1000 --skip-validation

# Test all schemas with small samples
python test_schemas.py
```

## Command Line Reference

### Available Arguments

```bash
python run_etl.py [options]
```

**Required:**
- `--schema {qualifying,pit_stop,race_results}` - Schema type to process (default: qualifying)

**Optional:**
- `--sample-size N` - Process only N records for testing
- `--skip-validation` - Skip data integrity validation
- `--help` - Show help message

### Examples

```bash
# Basic usage (qualification schema)
python run_etl.py

# Specify schema explicitly  
python run_etl.py --schema pit_stop

# Test with sample
python run_etl.py --schema race_results --sample-size 100

# Production run without validation
python run_etl.py --schema qualifying --skip-validation
```

## Schema-Specific Information

### 1. Qualification Sessions Schema

**Target Database:** `f1_qlf_db`  
**Primary Data:** `qualifying.csv`  
**Focus:** Driver performance in qualifying rounds (Q1, Q2, Q3)

**Key Features:**
- Qualifying time analysis (Q1/Q2/Q3)
- Status derivation (OK/DNQ/DNS/DSQ)
- Date dimension uses qualifying date (race date - 1 day)

```bash
# Examples
python run_etl.py --schema qualifying --sample-size 1000
python run_etl.py --schema qualifying --skip-validation
```

### 2. Pit Stop Schema

**Target Database:** `f1_pit_db`  
**Primary Data:** `pit_stops.csv`  
**Focus:** Pit stop performance and timing analysis

**Key Features:**
- Pit stop duration analysis
- Stop number and lap tracking
- Simplified schema (no position/lap time data)

```bash
# Examples  
python run_etl.py --schema pit_stop --sample-size 500
python run_etl.py --schema pit_stop --skip-validation
```

### 3. Race Results Schema

**Target Database:** `f1_res_db`  
**Primary Data:** `results.csv` + `status.csv`  
**Focus:** Final race positions and championship points

**Key Features:**
- Race final positions
- Championship points analysis
- Status text (Finished, DNF, etc.)
- Fastest lap statistics

```bash
# Examples
python run_etl.py --schema race_results --sample-size 500
python run_etl.py --schema race_results --skip-validation
```


## Monitoring and Troubleshooting

### Log Files

#### Real-time Monitoring
```bash
# Monitor pipeline progress
tail -f logs/f1_etl_run_YYYYMMDD_HHMMSS.log
```

#### Log Locations
- **Per-run logs**: `logs/f1_etl_run_YYYYMMDD_HHMMSS.log`
- **General logs**: `logs/etl_pipeline.log`

### Performance Monitoring

#### Typical Performance Metrics
- **Dimension Loading**: 2,000-7,000 records/sec
- **Fact Loading**: 300-1,500 records/sec (varies by schema)
- **Memory Usage**: 200MB-1GB (depending on batch size)

#### Sample Output
```
==================================================
F1 PIT_STOP ETL PIPELINE SUMMARY REPORT
==================================================
Schema: pit_stop
Primary data source: pit_stops
Total execution time: 2.45 seconds
Pit_stops records processed: 1,000
Dimension records loaded: 3,400
Fact records loaded: 1,000
Total records loaded: 4,400
Total records skipped: 0
Pit_stops processing rate: 408 records/sec
Total loading rate: 1,796 records/sec
==================================================
```

### Common Issues and Solutions

#### Database Connection Issues
```bash
# Error: "Access denied for user"
# Solution: Verify credentials in .env file

# Error: Database 'f1_xxx_db' doesn't exist  
# Solution: Run schema creation scripts
mysql -u azalia2 -p < schemas/[schema_name]_schema.sql
```

#### Schema-Specific Issues
```bash
# Error: Table doesn't exist
# Solution: Verify schema was created for target database

# Error: Duplicate entry constraint violation
# Solution: Check for existing data, use clean database
```

#### Memory Issues
```bash
# For qualifying schema: Reduce BATCH_SIZE in .env
BATCH_SIZE=1000

# For pit_stop/race_results: Use smaller sample sizes
python run_etl.py --schema pit_stop --sample-size 100
```

## Testing and Validation

### Comprehensive Testing

#### Test All Schemas
```bash
# Run automated test suite
python test_schemas.py
```

Expected output:
```
============================================================
TEST SUMMARY
============================================================
qualifying     : ✅ PASSED
pit_stop       : ✅ PASSED  
race_results   : ✅ PASSED

Total: 3/3 schemas passed
🎉 All schemas working correctly!
```

#### Individual Schema Testing
```bash
# Test each schema individually with small samples
python run_etl.py --schema qualifying --sample-size 50
python run_etl.py --schema pit_stop --sample-size 25
python run_etl.py --schema race_results --sample-size 25
```

### Data Quality Validation

#### Validate Results
```sql
-- Check qualification data
USE f1_qlf_db;
SELECT status, COUNT(*) FROM facts GROUP BY status;

-- Check pit stop data  
USE f1_pit_db;
SELECT COUNT(*) as total_pit_stops FROM fact_pit_stop;

-- Check race results
USE f1_res_db;
SELECT status_text, COUNT(*) FROM fact_race_result GROUP BY status_text LIMIT 10;
```

## Configuration Reference

### Environment Variables

```bash
# Database Configuration (auto-switched per schema)
DATABASE_HOST=127.0.0.1
DATABASE_PORT=3306
DATABASE_USER=azalia2
DATABASE_PASSWORD=123456

# Processing Configuration
BATCH_SIZE=10000            # Qualifying batch size
LOG_LEVEL=INFO              # Logging detail level
TRUNCATE_TABLES=true        # Clear tables before loading

# File Paths (auto-detected)
DATA_PATH=data/
```
