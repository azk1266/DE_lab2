# F1 ETL Pipeline Diagrams

This document contains sequence and flow diagrams for the F1 ETL pipeline implemented in `run_etl.py`.

## Sequence Diagram

The sequence diagram shows the detailed interaction between components during the ETL process:

```mermaid
sequenceDiagram
    participant Main as Main Function
    participant F1ETL as F1ETL Class
    participant CSVExtractor as CSV Extractor
    participant DimTransformer as Dimension Transformer
    participant FactTransformer as Fact Transformer
    participant MySQLLoader as MySQL Loader
    participant Logger as Logger

    Main->>F1ETL: Initialize(schema_type)
    F1ETL->>Logger: Setup logging
    F1ETL->>CSVExtractor: Initialize
    F1ETL->>DimTransformer: Initialize
    F1ETL->>MySQLLoader: Initialize
    F1ETL->>FactTransformer: Initialize transformers

    Main->>F1ETL: run_pipeline(skip_validation, sample_size)
    
    Note over F1ETL: Step 1: Validate source files
    F1ETL->>CSVExtractor: validate_source_files()
    CSVExtractor-->>F1ETL: validation_results
    
    Note over F1ETL: Step 2: Setup database
    F1ETL->>MySQLLoader: connect()
    F1ETL->>MySQLLoader: truncate_tables()
    
    Note over F1ETL: Step 3: Extract dimension data
    F1ETL->>CSVExtractor: extract_circuits()
    CSVExtractor-->>F1ETL: circuits_data
    F1ETL->>CSVExtractor: extract_constructors()
    CSVExtractor-->>F1ETL: constructors_data
    F1ETL->>CSVExtractor: extract_drivers()
    CSVExtractor-->>F1ETL: drivers_data
    F1ETL->>CSVExtractor: extract_races()
    CSVExtractor-->>F1ETL: races_data
    
    Note over F1ETL: Step 4: Transform dimensions
    F1ETL->>DimTransformer: transform_circuits()
    DimTransformer-->>F1ETL: circuits_df, original_ids
    F1ETL->>DimTransformer: transform_constructors()
    DimTransformer-->>F1ETL: constructors_df, original_ids
    F1ETL->>DimTransformer: transform_drivers()
    DimTransformer-->>F1ETL: drivers_df, original_ids
    F1ETL->>DimTransformer: create_date_dimension_for_schema()
    DimTransformer-->>F1ETL: dates_df
    
    Note over F1ETL: Step 5: Load dimensions
    F1ETL->>MySQLLoader: load_dimensions(dimensions)
    MySQLLoader-->>F1ETL: lookup_tables
    
    Note over F1ETL: Step 6: Process fact data
    alt Schema: qualifying
        loop For each batch
            F1ETL->>CSVExtractor: extract_qualifying_batched()
            CSVExtractor-->>F1ETL: batch_df
            F1ETL->>FactTransformer: transform_qualifying()
            FactTransformer-->>F1ETL: fact_batch
            F1ETL->>MySQLLoader: load_fact_data()
        end
    else Schema: pit_stop
        F1ETL->>CSVExtractor: extract_pit_stops()
        CSVExtractor-->>F1ETL: pit_stops_df
        F1ETL->>FactTransformer: transform_pit_stops()
        FactTransformer-->>F1ETL: fact_df
        F1ETL->>MySQLLoader: load_fact_data()
    else Schema: race_results
        F1ETL->>CSVExtractor: extract_results()
        CSVExtractor-->>F1ETL: results_df
        F1ETL->>CSVExtractor: extract_status()
        CSVExtractor-->>F1ETL: status_df
        F1ETL->>FactTransformer: transform_race_results()
        FactTransformer-->>F1ETL: fact_df
        F1ETL->>MySQLLoader: load_fact_data()
    end
    
    Note over F1ETL: Step 7: Post-load operations
    alt skip_validation = False
        F1ETL->>MySQLLoader: validate_data_integrity()
        MySQLLoader-->>F1ETL: validation_results
    end
    F1ETL->>MySQLLoader: get_dimension_counts()
    MySQLLoader-->>F1ETL: table_counts
    
    F1ETL->>Logger: generate_final_report()
    F1ETL->>MySQLLoader: disconnect()
    
    F1ETL-->>Main: success/failure
```

## Flow Diagram

The flow diagram provides a high-level overview of the entire ETL process:

```mermaid
flowchart TD
    A[Start: Main Function] --> B[Parse Command Line Arguments]
    B --> C[Create Logs Directory]
    C --> D[Initialize F1ETL with Schema Type]
    
    D --> E[Setup Logging & Components]
    E --> F[Configure Schema Settings]
    F --> G[Run ETL Pipeline]
    
    G --> H[Step 1: Validate Source Files]
    H --> I{All Files Valid?}
    I -->|No| Z[Log Error & Exit]
    I -->|Yes| J[Step 2: Setup Database]
    
    J --> K[Connect to MySQL]
    K --> L[Truncate Tables]
    L --> M[Step 3: Extract Dimension Data]
    
    M --> N[Extract Circuits]
    N --> O[Extract Constructors]
    O --> P[Extract Drivers]
    P --> Q[Extract Races]
    Q --> R[Step 4: Transform Dimensions]
    
    R --> S[Transform Circuits]
    S --> T[Transform Constructors]
    T --> U[Transform Drivers]
    U --> V[Create Date Dimension]
    V --> W{Schema Type?}
    
    W -->|pit_stop or race_results| X[Transform Races]
    W -->|qualifying| Y[Step 5: Load Dimensions]
    X --> Y
    
    Y --> AA[Load All Dimensions to DB]
    AA --> BB[Create Lookup Tables]
    BB --> CC[Step 6: Process Fact Data]
    
    CC --> DD{Schema Type?}
    
    DD -->|qualifying| EE[Process Qualifying Batched]
    DD -->|pit_stop| FF[Process Pit Stops Full]
    DD -->|race_results| GG[Process Race Results Full]
    
    EE --> HH[For Each Batch]
    HH --> II[Extract Batch from CSV]
    II --> JJ[Transform Batch]
    JJ --> KK[Load Batch to DB]
    KK --> LL{More Batches?}
    LL -->|Yes| HH
    LL -->|No| MM[Step 7: Post-Load Operations]
    
    FF --> NN[Extract Pit Stops]
    NN --> OO[Transform Pit Stops]
    OO --> PP[Load to DB]
    PP --> MM
    
    GG --> QQ[Extract Results & Status]
    QQ --> RR[Transform Race Results]
    RR --> SS[Load to DB]
    SS --> MM
    
    MM --> TT{Skip Validation?}
    TT -->|No| UU[Validate Data Integrity]
    TT -->|Yes| VV[Get Table Counts]
    UU --> VV
    
    VV --> WW[Generate Final Report]
    WW --> XX[Cleanup Resources]
    XX --> YY[Return Success/Failure]
    
    YY --> ZZ[Exit with Status Code]
    
    style A fill:#e1f5fe
    style ZZ fill:#c8e6c9
    style Z fill:#ffcdd2
    style G fill:#fff3e0
    style H fill:#f3e5f5
    style M fill:#f3e5f5
    style R fill:#f3e5f5
    style Y fill:#f3e5f5
    style CC fill:#f3e5f5
    style MM fill:#f3e5f5
```

## Key Features of the F1 ETL Pipeline

### Architecture
- **Multi-Schema Support**: Handles three different F1 data schemas (qualifying, pit_stop, race_results)
- **Dimension-Fact Architecture**: Implements proper dimensional modeling with separate dimension and fact tables
- **Batch Processing**: For large datasets (qualifying), processes data in batches to manage memory efficiently

### Process Flow
1. **Validation**: Validates source CSV files before processing
2. **Database Setup**: Connects to MySQL and prepares tables
3. **Dimension Processing**: Extracts, transforms, and loads dimension data
4. **Fact Processing**: Processes fact data based on schema type (batched for qualifying, full for others)
5. **Post-Load Operations**: Validates data integrity and generates reports

### Error Handling
- Comprehensive error handling with detailed logging
- Graceful degradation for batch processing failures
- Validation of data integrity with optional skipping

### Performance Features
- Memory-efficient batch processing for large datasets
- Progress tracking and performance metrics
- Configurable sample sizes for testing

### Command Line Options
- `--schema`: Choose between qualifying, pit_stop, or race_results
- `--skip-validation`: Skip data integrity validation
- `--sample-size`: Process only N records for testing
