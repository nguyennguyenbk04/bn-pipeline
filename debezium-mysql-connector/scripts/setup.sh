#!/bin/bash

# Set environment variables from .env file
if [ -f ../.env ]; then
    export $(grep -v '^#' ../.env | xargs)
fi

# Start the Docker containers
docker-compose up -d

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
until docker exec mysql-container mysqladmin ping -h"mysql" --silent; do
    sleep 1
done

# Create the Debezium connector
echo "Creating Debezium connector..."
curl -X POST -H "Content-Type: application/json" --data @../config/connector.json http://localhost:8083/connectors

echo "Setup completed."