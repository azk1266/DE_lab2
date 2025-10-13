# F1 Qualifying Performance Analysis - Analytical Objectives

## Project Overview

This dimensional model is designed to support comprehensive analysis of Formula 1 driver performance during qualifying sessions. The model enables stakeholders to analyze qualifying performance patterns, compare drivers and constructors across different circuits and time periods, and identify factors that influence qualifying outcomes.

## Business Context

Formula 1 qualifying sessions determine the starting grid positions for races, making them crucial for race strategy and outcomes. Understanding qualifying performance patterns helps teams, drivers, and analysts make data-driven decisions about:

- Driver performance optimization
- Constructor competitiveness analysis
- Circuit-specific performance patterns
- Historical performance trends
- Strategic qualifying session planning

## Analytical Goals

### 1. Driver Performance Analysis
**Objective**: Analyze individual driver performance across different qualifying sessions, circuits, and time periods.

**Key Questions**:
- Which drivers consistently perform well in qualifying sessions?
- How does driver performance vary across different circuits?
- What is the trend of driver performance over time?
- Which drivers excel in specific qualifying conditions (Q1, Q2, Q3)?

**Dimensional Model Support**:
- `dim_driver` provides driver information and nationality
- `dim_date` enables time-based analysis
- `dim_circuit` allows circuit-specific performance analysis
- `facts.status` indicates qualifying session outcomes
- `facts.q1_ms`, `facts.q2_ms`, `facts.q3_ms` provide detailed timing data

### 2. Constructor Competitiveness Analysis
**Objective**: Evaluate constructor (team) performance and competitiveness in qualifying sessions.

**Key Questions**:
- Which constructors build the fastest qualifying cars?
- How has constructor competitiveness evolved over time?
- Which constructors perform better on specific types of circuits?
- What is the performance gap between top and bottom constructors?

**Dimensional Model Support**:
- `dim_constructor` provides constructor information and nationality
- `dim_circuit` enables circuit-type analysis
- `dim_date` supports temporal analysis
- `facts.position` indicates qualifying performance ranking
- Timing measures enable detailed performance comparison

### 3. Circuit Performance Patterns
**Objective**: Understand how different circuits affect qualifying performance and identify circuit characteristics that influence outcomes.

**Key Questions**:
- Which circuits produce the closest qualifying times?
- How do circuit characteristics (altitude, location) affect performance?
- Which circuits favor certain types of cars or driving styles?
- What are the typical qualifying time ranges for each circuit?

**Dimensional Model Support**:
- `dim_circuit` provides circuit characteristics (altitude, location, country)
- `facts.q1_ms`, `facts.q2_ms`, `facts.q3_ms` enable time distribution analysis
- `facts.status` shows qualifying completion rates by circuit
- `dim_date` allows analysis across different years at the same circuit

### 4. Qualifying Session Dynamics
**Objective**: Analyze the dynamics of qualifying sessions and understand progression through Q1, Q2, and Q3.

**Key Questions**:
- What percentage of drivers advance from Q1 to Q2 to Q3?
- How do qualifying times improve from Q1 to Q3?
- Which drivers show the biggest improvement in Q3?
- What factors lead to DNQ (Did Not Qualify) outcomes?

**Dimensional Model Support**:
- `facts.status` categorizes qualifying outcomes (OK, DNQ, DNS, DSQ)
- Individual session times (Q1, Q2, Q3) enable progression analysis
- `facts.position` shows final qualifying ranking
- Time-based analysis through `dim_date`

### 5. Historical Trends and Evolution
**Objective**: Track the evolution of F1 qualifying performance over decades and identify long-term trends.

**Key Questions**:
- How have qualifying times evolved over the years?
- Which eras produced the most competitive qualifying sessions?
- How has the performance gap between drivers changed over time?
- What impact did regulation changes have on qualifying performance?

**Dimensional Model Support**:
- `dim_date` provides comprehensive time-based analysis capabilities
- Year, month, and time bucket attributes enable trend analysis
- All performance measures can be analyzed across time periods
- Driver and constructor evolution can be tracked over careers

## Key Performance Indicators (KPIs)

### Primary KPIs
1. **Average Qualifying Position** - Overall driver/constructor competitiveness
2. **Q3 Advancement Rate** - Percentage of sessions reaching Q3
3. **Pole Position Count** - Number of P1 qualifying positions
4. **DNQ Rate** - Percentage of sessions not advancing from Q1
5. **Average Qualifying Time Gap** - Time difference from pole position

### Secondary KPIs
1. **Circuit-Specific Performance** - Performance ranking by circuit type
2. **Qualifying Consistency** - Standard deviation of qualifying positions
3. **Session Progression** - Time improvement from Q1 to Q3
4. **Seasonal Performance Trends** - Performance changes within seasons
5. **Constructor Competitiveness Index** - Relative constructor performance

## Dimensional Model Justification

### Star Schema Design
The star schema design is optimal for this analytical use case because:

1. **Query Performance**: Simplified joins between fact and dimension tables
2. **Business User Friendly**: Intuitive structure matching business concepts
3. **Aggregation Efficiency**: Fast calculation of summary statistics
4. **Scalability**: Efficient handling of large historical datasets

### Dimension Table Design

#### dim_circuit
- **Purpose**: Provides circuit characteristics for location-based analysis
- **Key Attributes**: Name, location, country, altitude, coordinates
- **Business Value**: Enables analysis of how circuit characteristics affect performance

#### dim_constructor
- **Purpose**: Represents F1 teams/constructors
- **Key Attributes**: Name, nationality
- **Business Value**: Supports team performance analysis and nationality-based insights

#### dim_driver
- **Purpose**: Represents F1 drivers with biographical information
- **Key Attributes**: Name, nationality, birthdate, country
- **Business Value**: Enables driver career analysis and demographic insights

#### dim_date
- **Purpose**: Provides comprehensive time-based analysis capabilities
- **Key Attributes**: Year, month, day, day of week, time bucket
- **Business Value**: Supports temporal analysis and seasonal pattern identification
- **Special Logic**: Qualifying date = race date - 1 day (matches F1 weekend structure)

### Fact Table Design

#### facts
- **Purpose**: Central fact table storing qualifying session results
- **Grain**: One record per driver per qualifying session
- **Key Measures**: Q1/Q2/Q3 times in milliseconds, position, status
- **Business Value**: Provides detailed qualifying performance data for analysis

### Status Dimension Logic
The `status` field captures qualifying session outcomes:
- **'OK'**: Driver completed all three qualifying sessions
- **'DNQ'**: Driver participated in Q1 but did not advance to Q2
- **'DNS'**: Driver did not start the qualifying session
- **'DSQ'**: Driver was disqualified or had invalid times

This categorization enables analysis of qualifying session completion rates and performance patterns.

## Data Quality Considerations

### Data Validation Rules
1. **Referential Integrity**: All fact records must have valid dimension references
2. **Time Validation**: Qualifying times must be within reasonable ranges (60s - 10min)
3. **Position Validation**: Qualifying positions must be between 1-30
4. **Status Consistency**: Status must align with available timing data

### Missing Data Handling
1. **Critical Missing Data**: Skip records with missing drivers, constructors, or race dates
2. **Circuit Defaults**: Use "Unknown Circuit" for missing circuit references
3. **Time Data**: NULL values preserved for missing qualifying times
4. **Position Data**: Default to 0 for missing position information

This approach ensures data quality while maximizing the analytical value of the available data.