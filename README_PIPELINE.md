# F1 Qualifying ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for analyzing Formula 1 qualifying session performance using dimensional modeling techniques.

## Project Overview

This project implements a dimensional model to support analysis of F1 driver performance during qualifying sessions. The pipeline processes historical F1 data (1950-2024) and loads it into a MySQL database optimized for analytical queries.

## Quick Start

### 1. Prerequisites
- Python 3.8+
- MySQL 5.7+ or MySQL 8.0+
- 4GB+ RAM (8GB recommended)

### 2. Setup Database
```bash
# Create database schema
mysql -u azalia2 -p < qualification_sessions_schema.sql
```

### 3. Install Dependencies
```bash
# Install Python packages
pip install -r requirements.txt
```

### 4. Run Pipeline
```bash
# Test with sample data
python run_etl.py --sample-size 100

# Run full pipeline
python run_etl.py
```

## Project Structure

```
lab2/
├── .env                                 # Configuration file
├── requirements.txt                     # Python dependencies
├── run_etl.py                           # Main pipeline script
├── schemas/
│   ├── qualification_sessions_schema.sql  # Database schema for qualification sessions
│   ├── pit_stop_schema.sql
│   ├── race_final_positions_schema.sql
├── data/                                # Source CSV files
│   ├── circuits.csv
│   ├── constructors.csv
│   ├── drivers.csv
│   ├── races.csv
│   └── qualifying.csv
├── src/                                # Source code
│   ├── config/                         # Configuration management
│   ├── extractors/                     # CSV data extraction
│   ├── transformers/                   # Data transformation
│   ├── loaders/                        # Database loading
│   └── utils/                          # Utilities and logging
├── logs/                               # Processing logs (auto-created)
└── reports/                            # Documentation
    ├── analytical_objectives.md
    ├── pipeline_description.md
    └── usage_instructions.md
```

## Key Features

### 🚀 Performance Optimized
- **Batch Processing**: Memory-efficient processing of large datasets
- **Resumable Operations**: Continue from interruption point
- **Configurable Batch Sizes**: Adapt to different hardware capabilities

### 🔍 Data Quality Focused
- **Strict Validation**: Skip records with missing critical data
- **Comprehensive Logging**: Detailed processing and error logs
- **Data Integrity Checks**: Validate relationships and business rules

### 🏎️ F1-Specific Logic
- **Time Conversion**: Convert "1:26.572" format to milliseconds
- **Status Derivation**: Determine qualifying outcomes (OK/DNQ/DNS/DSQ)
- **Date Calculation**: Qualifying date = race date - 1 day

### 🛠️ Production Ready
- **Error Recovery**: Graceful handling of data issues
- **State Management**: Track progress for large datasets
- **Configuration Management**: Environment-based settings

## Database Schema

### Dimensional Model
- **dim_circuit**: Circuit information (name, location, coordinates)
- **dim_constructor**: Constructor/team data
- **dim_driver**: Driver biographical information
- **dim_date**: Time dimension with qualifying dates
- **facts**: Central fact table with qualifying results

### Key Relationships
```sql
facts.dim_circuit_circuit_key → dim_circuit.circuit_key
facts.dim_constructor_constructor_key → dim_constructor.constructor_key  
facts.dim_driver_driver_key → dim_driver.driver_key
facts.dim_date_date_key → dim_date.date_key
```

## Usage Examples

### Basic Operations
```bash
# Run with default settings
python run_etl.py

# Process sample for testing
python run_etl.py --sample-size 1000

# Skip validation for faster processing
python run_etl.py --skip-validation
```

### State Management
```bash
# Check current processing state
python run_etl.py --show-state

# Start fresh (ignore previous state)
python run_etl.py --fresh-start

# Resume from specific position
python run_etl.py --reset-position --start-row 50000
```

### Configuration
```bash
# Adjust batch size for your system
# Edit .env file:
BATCH_SIZE=5000    # For smaller systems
BATCH_SIZE=20000   # For larger systems
```

## Data Processing Logic

### Record Validation (Skip Rules)
- **Missing Driver**: Skip entire qualifying record
- **Missing Constructor**: Skip entire qualifying record
- **Missing Race/Date**: Skip entire qualifying record

### Default Value Handling
- **Missing Circuit**: Use "Unknown Circuit" (key=999)
- **Missing Times**: Preserve as NULL in database
- **Missing Position**: Default to 0

### Time Conversion Algorithm
```python
# "1:26.572" → 86572 milliseconds
minutes = 1
seconds = 26  
milliseconds = 572
total_ms = (minutes * 60 + seconds) * 1000 + milliseconds
```

### Status Derivation
- **'OK'**: Has Q1, Q2, and Q3 times
- **'DNQ'**: Has Q1, missing Q2/Q3 (Did Not Qualify)
- **'DNS'**: No times recorded (Did Not Start)
- **'DSQ'**: Invalid/penalty times (Disqualified)

## Monitoring and Logs

### Log Files
- **Per-run logs**: `logs/f1_etl_run_YYYYMMDD_HHMMSS.log`
- **State file**: `f1_etl_state.json`

### Progress Monitoring
```bash
# Real-time log monitoring
tail -f logs/f1_etl_run_*.log

# Check processing statistics
grep "records processed" logs/f1_etl_run_*.log
```

## Performance Expectations

### Processing Times (Full Dataset ~10K records)
- **Small System** (4GB RAM): 15-30 minutes
- **Medium System** (8GB RAM): 5-15 minutes
- **Large System** (16GB+ RAM): 2-5 minutes

### Resource Usage
- **Memory**: 500MB - 2GB (depending on batch size)
- **Disk**: 100MB for logs and temporary files
- **Database**: ~50MB for complete F1 dataset

## Troubleshooting

### Common Issues

#### Memory Problems
```bash
# Reduce batch size
# Edit .env: BATCH_SIZE=1000
```

#### Database Connection
```bash
# Verify MySQL is running
systemctl status mysql

# Test connection
mysql -u azalia2 -p -e "SELECT 1"
```

#### High Skip Rates
```bash
# Check validation logs
grep "Skipped.*invalid records" logs/f1_etl_run_*.log
```

### Recovery Options
```bash
# Complete reset
python run_etl.py --fresh-start

# Resume processing
python run_etl.py

# Start from specific position
python run_etl.py --reset-position --start-row 25000
```

## Analytical Capabilities

Once loaded, the dimensional model supports analysis of:

### Driver Performance
- Qualifying position trends over time
- Circuit-specific performance patterns
- Session completion rates (Q1/Q2/Q3 advancement)

### Constructor Analysis
- Team competitiveness evolution
- Constructor performance by circuit type
- Nationality-based performance patterns

### Circuit Analysis
- Lap time distributions by circuit
- Circuit difficulty analysis
- Geographic performance patterns

### Historical Trends
- Performance evolution over decades
- Era-based comparisons
- Regulation impact analysis

## Sample Queries

```sql
-- Top 10 drivers by average qualifying position
SELECT 
    d.name,
    AVG(f.position) as avg_position,
    COUNT(*) as sessions
FROM facts f
JOIN dim_driver d ON f.dim_driver_driver_key = d.driver_key
WHERE f.position > 0
GROUP BY d.driver_key, d.name
ORDER BY avg_position
LIMIT 10;

-- Qualifying status distribution by year
SELECT 
    dt.year,
    f.status,
    COUNT(*) as count
FROM facts f
JOIN dim_date dt ON f.dim_date_date_key = dt.date_key
GROUP BY dt.year, f.status
ORDER BY dt.year, f.status;

-- Fastest Q1 times by circuit
SELECT 
    c.name as circuit_name,
    MIN(f.q1_ms) as fastest_q1_ms,
    MIN(f.q1_ms) / 1000.0 as fastest_q1_seconds
FROM facts f
JOIN dim_circuit c ON f.dim_circuit_circuit_key = c.circuit_key
WHERE f.q1_ms IS NOT NULL
GROUP BY c.circuit_key, c.name
ORDER BY fastest_q1_ms;
```

## Documentation

- **[Analytical Objectives](reports/analytical_objectives.md)**: Business goals and KPIs
- **[Pipeline Description](reports/pipeline_description.md)**: Technical implementation details
- **[Usage Instructions](reports/usage_instructions.md)**: Detailed setup and usage guide

## Support

For issues or questions:
1. Check the log files in `logs/` directory
2. Review the troubleshooting section in usage instructions
3. Examine the state file `f1_etl_state.json` for processing status

## License

This project is designed for educational purposes as part of a Data Engineering course assignment.