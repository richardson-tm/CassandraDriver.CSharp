#!/bin/bash

# setup-test-cluster.sh
# Complete setup script for Cassandra test cluster

set -e

echo "=== Cassandra Test Cluster Setup ==="
echo "Using Cassandra 5.0"
echo ""

# Check if docker is running
if ! docker info >/dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Stop any existing cluster
echo "Stopping any existing Cassandra containers..."
docker compose down -v 2>/dev/null || true

# Start the cluster
echo "Starting 3-node Cassandra cluster..."
docker compose up -d

# Wait for cluster to be ready
echo ""
./scripts/wait-for-cassandra.sh

# Create test data
echo ""
echo "Creating test keyspace and sample data..."
docker compose exec -T cassandra-seed cqlsh < scripts/create-test-data.cql

echo ""
echo "=== Setup Complete! ==="
echo ""
echo "Cluster endpoints:"
echo "  - Seed node: localhost:9042"
echo "  - Node 1: localhost:9043"
echo "  - Node 2: localhost:9044"
echo ""
echo "Test keyspace: test_keyspace"
echo "Sample tables: users, products, events, time_series"
echo ""
echo "You can now:"
echo "  1. Run integration tests: dotnet test --filter \"Category=Integration\""
echo "  2. Connect with cqlsh: docker compose exec cassandra-seed cqlsh"
echo "  3. Check cluster status: docker compose exec cassandra-seed nodetool status"
echo ""