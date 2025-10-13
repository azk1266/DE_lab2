# F1 Qualifying ETL Pipeline - Technical Description

## Overview

This document provides a comprehensive technical description of the F1 Qualifying ETL (Extract, Transform, Load) pipeline designed to populate the dimensional model for analyzing Formula 1 qualifying session performance.

## Pipeline Architecture

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   EXTRACT       │    │   TRANSFORM     │    │     LOAD        │
│                 │    │                 │    │                 │
│ • circuits.csv  │───▶│ • Data Cleaning │───▶│ • MySQL Database│
│ • constructors  │    │ • Field Mapping │    │ • Batch Loading │
│ • drivers.csv   │    │ • Time Convert  │    │ • Validation    │
│ • races.csv     │    │ • Status Derive │    │ • Error Handle  │
│ • qualifying    │    │ • Date Calc     │    │ • State Mgmt    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Component Architecture

The pipeline follows a modular architecture with clear separation of concerns:

1. **Configuration Layer** (`src/config/`)
   - Environment-based configuration management
   - Database connection parameters
   - Processing options and batch sizes

2. **Extraction Layer** (`src/extractors/`)
   - CSV file reading with data type enforcement
   - Memory-efficient batch processing
   - File validation and error handling

3. **Transformation Layer** (`src/transformers/`)
   - Dimensional data transformation
   - Fact data transformation with business logic
   - Data quality validation and cleansing

4. **Loading Layer** (`src/loaders/`)
   - Database connection management
   - Batch loading with transaction handling
   - Data integrity validation

5. **Utilities Layer** (`src/utils/`)
   - Logging and progress tracking
   - Resumable batch state management
   - Error handling and recovery

## Data Processing Flow

### Phase 1: Dimension Processing

#### Step 1: Extract Dimension Data
- **circuits.csv**: Circuit information (name, location, coordinates)
- **constructors.csv**: Constructor/team information
- **drivers.csv**: Driver biographical data
- **races.csv**: Race information for date calculations

#### Step 2: Transform Dimensions
- **Circuits**: Map `location` → `city`, add unknown circuit (key=999)
- **Constructors**: Clean names and nationalities
- **Drivers**: Combine `forename` + `surname` → `name`, map nationality to country
- **Dates**: Calculate qualifying dates (race date - 1 day), add time buckets

#### Step 3: Load Dimensions
- Load dimension tables with duplicate detection
- Create lookup tables for foreign key resolution
- Validate dimensional data integrity

### Phase 2: Fact Processing (Batched)

#### Step 1: Batch Extraction
- Read qualifying.csv in configurable batches (default: 10,000 records)
- Preserve memory efficiency for large datasets
- Support resumable processing with state management

#### Step 2: Record Validation and Filtering
**Critical Validation (Skip Records)**:
- Missing or invalid `driverId` → Skip entire record
- Missing or invalid `constructorId` → Skip entire record  
- Missing or invalid `raceId` → Skip entire record

**Default Value Handling**:
- Missing `circuitId` → Use unknown circuit (key=999)
- Missing qualifying times → Preserve as NULL
- Missing position → Default to 0

#### Step 3: Data Transformation

**Time Conversion**:
```python
# Convert "1:26.572" to milliseconds
def time_to_milliseconds(time_str):
    # Parse MM:SS.SSS format
    minutes, seconds_ms = time_str.split(':')
    seconds, milliseconds = seconds_ms.split('.')
    
    total_ms = (int(minutes) * 60 + int(seconds)) * 1000 + int(milliseconds)
    return total_ms
```

**Status Derivation Logic**:
```python
def derive_status(q1_ms, q2_ms, q3_ms):
    has_q1 = q1_ms is not None and q1_ms > 0
    has_q2 = q2_ms is not None and q2_ms > 0  
    has_q3 = q3_ms is not None and q3_ms > 0
    
    if has_q1 and has_q2 and has_q3:
        return 'OK'     # Completed all sessions
    elif has_q1 and not has_q2:
        return 'DNQ'    # Did not qualify for Q2
    elif not has_q1:
        return 'DNS'    # Did not start
    else:
        return 'DSQ'    # Disqualified or invalid
```

**Date Key Calculation**:
```python
# Calculate qualifying date from race date
qualifying_date = race_date - timedelta(days=1)
date_key = int(qualifying_date.strftime('%Y%m%d'))
```

#### Step 4: Foreign Key Resolution
- Map `circuitId` → `dim_circuit_circuit_key`
- Map `constructorId` → `dim_constructor_constructor_key`
- Map `driverId` → `dim_driver_driver_key`
- Map calculated date → `dim_date_date_key`

#### Step 5: Batch Loading
- Load transformed batches to `facts` table
- Maintain transaction integrity
- Track processing progress and errors

## Technical Implementation Details

### Memory Management
- **Batch Processing**: Process qualifying data in configurable batches
- **Streaming**: Read CSV files without loading entire datasets into memory
- **State Management**: Track progress to enable resumable processing

### Error Handling Strategy

#### Record-Level Validation
1. **Strict Validation**: Skip records with missing critical references
2. **Graceful Degradation**: Use defaults for non-critical missing data
3. **Detailed Logging**: Log all skipped records with reasons

#### Batch-Level Recovery
1. **Continue Processing**: Failed batches don't stop the entire pipeline
2. **State Persistence**: Save progress after each successful batch
3. **Resumable Operations**: Restart from last successful batch

### Data Quality Assurance

#### Input Validation
- File existence and readability checks
- Column presence validation
- Data type enforcement during CSV reading

#### Transformation Validation
- Time format validation (MM:SS.SSS pattern)
- Reasonable time range checks (60 seconds to 10 minutes)
- Position range validation (1-30)
- Status consistency validation

#### Output Validation
- Foreign key integrity checks
- Orphaned record detection
- Data range validation
- Business rule consistency checks

### Performance Optimization

#### Database Operations
- Batch loading with configurable batch sizes
- Connection pooling and recycling
- Transaction management for data consistency

#### Processing Efficiency
- Memory-efficient streaming processing
- Parallel-ready architecture (single-threaded implementation)
- Optimized data type usage

## Configuration Management

### Environment Variables
All configuration is managed through environment variables in `.env`:

```bash
# Database Configuration
DATABASE_HOST=127.0.0.1
DATABASE_PORT=3306
DATABASE_USER=azalia2
DATABASE_PASSWORD=123456
DATABASE_NAME=mydb

# Processing Configuration
BATCH_SIZE=10000
LOG_LEVEL=INFO
TRUNCATE_TABLES=true
```

### Configurable Parameters
- **Batch Size**: Adjustable for different hardware capabilities
- **Logging Level**: Configurable verbosity (DEBUG, INFO, WARNING, ERROR)
- **Table Management**: Optional table truncation for fresh loads
- **Validation**: Optional data integrity validation

## Monitoring and Logging

### Progress Tracking
- Real-time batch processing progress
- Performance metrics (records/second)
- Memory usage monitoring
- Error and warning counts

### Detailed Logging
- Per-run log files with timestamps
- Batch-level processing details
- Record-level validation results
- Performance statistics

### State Management
- Persistent state for resumable processing
- Batch position tracking
- Error recovery information
- Processing statistics

## Scalability Considerations

### Horizontal Scaling
- Batch-based processing enables parallel execution
- State management supports distributed processing
- Modular architecture allows component scaling

### Vertical Scaling
- Configurable batch sizes for different hardware
- Memory-efficient processing for large datasets
- Optimized database operations

### Data Volume Handling
- Designed for historical F1 data (1950-2024)
- Efficient processing of 10,000+ qualifying records
- Scalable to future data growth

## Error Recovery and Resumption

### Automatic Recovery
- Failed batches are skipped with detailed logging
- Processing continues with subsequent batches
- State is preserved for manual intervention

### Manual Recovery
- Command-line tools for state inspection
- Position reset capabilities
- Fresh start options

### Data Consistency
- Transaction-based loading ensures consistency
- Foreign key validation prevents orphaned records
- Rollback capabilities for failed operations

This pipeline design ensures robust, scalable, and maintainable processing of F1 qualifying data while providing comprehensive analytical capabilities for performance analysis.