#!/bin/bash
# Restart script for F1 ETL Pipeline

echo "Restarting F1 ETL pipeline..."

# remove state file
rm f1_etl_state.json

# Load environment variables from .env
set -a
source .env
set +a

# Recreate database schema
echo "Recreating database schema..."
mysql -u "$DATABASE_USER" -p"$DATABASE_PASSWORD" "$DATABASE_NAME" < qualification_sessions_schema.sql

if [ $? -eq 0 ]; then
    echo "✅ Database schema recreated successfully!"
else
    echo "❌ Error while recreating schema!"
    exit 1
fi

# Run the ETL pipeline again
echo "Running ETL..."
python run_etl.py --sample-size 100

# mysql -u azalia2 -p123456  < pit_stop_schema.sql