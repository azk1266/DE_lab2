#!/bin/bash

# F1 Qualifying ETL Pipeline -  Database Setup Script

echo "Setting up F1 Qualifying database..."

# Check if MySQL is running
if ! systemctl is-active --quiet mysql; then
    echo "Error: MySQL service is not running. Please start MySQL first."
    echo "Run: sudo systemctl start mysql"
    exit 1
fi

# Create database schema
echo "Creating database schema..."
mysql -u azalia2 -p123456 < qualification_sessions_schema.sql

if [ $? -eq 0 ]; then
    echo "✅ Database schema created successfully!"
    echo ""
    echo "Database: f1_qlf_db"
    echo "Tables created:"
    echo "  - dim_circuit"
    echo "  - dim_constructor" 
    echo "  - dim_driver"
    echo "  - dim_date"
    echo "  - facts"
    echo ""
    echo "You can now run the ETL pipeline:"
    echo "  python run_etl.py --sample-size 100"
else
    echo "❌ Error creating database schema. Please check the error messages above."
    exit 1
fi