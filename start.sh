#!/bin/bash
set -e

# Function to check if PostgreSQL is ready
function wait_for_postgres() {
  echo "Waiting for PostgreSQL to be ready..."
  
  for i in {1..60}; do
    pg_isready -h $DB_HOST -p $DB_PORT -U $DB_USER && echo "PostgreSQL is ready!" && return 0
    echo "PostgreSQL is not ready yet (attempt $i of 60)... waiting 1 second..."
    sleep 1
  done
  
  echo "Failed to connect to PostgreSQL after 60 attempts."
  return 1
}

# Wait for PostgreSQL to be ready
wait_for_postgres

# Run the initialization script
echo "Initializing database..."
python /app/scripts/init_btree.py

# Start the application
echo "Starting Flask application..."
python -m app.main