#!/bin/bash

# wait-for-cassandra.sh
# Waits for Cassandra cluster to be fully ready

echo "Waiting for Cassandra cluster to be ready..."

# Function to check if a node is ready
check_node() {
    local container=$1
    local port=$2
    
    # Check if container is running
    if ! docker compose ps | grep -q "$container.*Up"; then
        return 1
    fi
    
    # Check if CQL port is responding
    if ! docker compose exec -T $container cqlsh -e "DESC KEYSPACES;" >/dev/null 2>&1; then
        return 1
    fi
    
    return 0
}

# Wait for seed node
echo -n "Waiting for cassandra-seed to be ready..."
while ! check_node "cassandra-seed" "9042"; do
    echo -n "."
    sleep 5
done
echo " Ready!"

# Wait for node1
echo -n "Waiting for cassandra-node1 to be ready..."
while ! check_node "cassandra-node1" "9042"; do
    echo -n "."
    sleep 5
done
echo " Ready!"

# Wait for node2
echo -n "Waiting for cassandra-node2 to be ready..."
while ! check_node "cassandra-node2" "9042"; do
    echo -n "."
    sleep 5
done
echo " Ready!"

# Verify cluster status
echo ""
echo "Checking cluster status..."
docker compose exec cassandra-seed nodetool status

echo ""
echo "Cassandra cluster is ready for testing!"
echo "Seed node: localhost:9042"
echo "Node 1: localhost:9043"
echo "Node 2: localhost:9044"