# Cassandra Docker Testing Environment

This guide helps you quickly spin up a local Cassandra 5.0 cluster for testing the C# driver.

> **Note**: This configuration uses Cassandra 5.0, the latest major version with significant improvements including:
> - Better performance and storage engine (Trie-based memtables)
> - Improved streaming and repair operations
> - Vector search capabilities (experimental)
> - Dynamic data masking
> - Better support for large partitions

> **Important**: The `docker-compose.yml` is configured for sequential node startup to prevent token collisions:
> - `cassandra-seed` starts first
> - `cassandra-node1` depends on `cassandra-seed` being healthy
> - `cassandra-node2` depends on `cassandra-node1` being healthy

## Prerequisites

- Docker installed and running
- Docker Compose (comes with Docker Desktop)
- .NET 8 SDK installed

## Quick Start

### Automated Setup (Recommended)

```bash
# Run the complete setup script
./scripts/setup-test-cluster.sh

# This will:
# 1. Stop any existing containers
# 2. Start a fresh 3-node cluster
# 3. Wait for all nodes to be ready
# 4. Create test keyspace and sample data
# 5. Display connection information
```

### Manual Setup

```bash
# Start a 3-node Cassandra cluster
docker compose up -d

# Wait for cluster to be ready (2-3 minutes for Cassandra 5.0)
./scripts/wait-for-cassandra.sh

# Create test data
docker compose exec -T cassandra-seed cqlsh < scripts/create-test-data.cql

# Check cluster status
docker compose exec cassandra-seed nodetool status
```

### 2. Verify Cluster is Ready

```bash
# Check that all 3 nodes are up
docker compose exec cassandra-seed cqlsh -e "SELECT peer, data_center, host_id FROM system.peers;"

# Should see 2 peers (node1 and node2) plus the seed node
```

## Running Integration Tests

### Quick Test Connection

```bash
# Test cluster connectivity with sample queries
dotnet run --project scripts/TestClusterConnection.csproj
```

### Run Integration Tests

```bash
# Ensure cluster is running and ready
./scripts/setup-test-cluster.sh

# Run all tests including integration
dotnet test

# Or run only integration tests
dotnet test --filter "Category=Integration"
```

### Test with Fresh Data

```bash
# Recreate test data
docker compose exec -T cassandra-seed cqlsh < scripts/create-test-data.cql

# Run tests
dotnet test --filter "Category=Integration"
```

## Sample CQL Operations

### Connect to Cassandra

```bash
# Connect to the seed node
docker compose exec cassandra-seed cqlsh

# Or connect from host machine (requires cqlsh installed)
cqlsh localhost 9042
```

### Create Test Keyspace and Tables

```sql
-- Create a test keyspace
CREATE KEYSPACE IF NOT EXISTS test_keyspace 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
};

USE test_keyspace;

-- Create a users table
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY,
    username TEXT,
    email TEXT,
    created_at TIMESTAMP
);

-- Create a products table
CREATE TABLE IF NOT EXISTS products (
    product_id UUID PRIMARY KEY,
    name TEXT,
    price DECIMAL,
    category TEXT,
    in_stock BOOLEAN
) WITH COMMENT = 'Products catalog table';

-- Create an events table with clustering
CREATE TABLE IF NOT EXISTS events (
    user_id UUID,
    event_id TIMEUUID,
    event_type TEXT,
    event_data TEXT,
    PRIMARY KEY (user_id, event_id)
) WITH CLUSTERING ORDER BY (event_id DESC);
```

### Insert Sample Data

```sql
-- Insert users
INSERT INTO users (user_id, username, email, created_at) 
VALUES (uuid(), 'john_doe', 'john@example.com', toTimestamp(now()));

INSERT INTO users (user_id, username, email, created_at) 
VALUES (uuid(), 'jane_smith', 'jane@example.com', toTimestamp(now()));

-- Insert products
INSERT INTO products (product_id, name, price, category, in_stock) 
VALUES (uuid(), 'Laptop', 999.99, 'Electronics', true);

INSERT INTO products (product_id, name, price, category, in_stock) 
VALUES (uuid(), 'Coffee Maker', 79.99, 'Appliances', true);

-- Insert events
INSERT INTO events (user_id, event_id, event_type, event_data) 
VALUES (522b1fe0-2e36-11ee-be56-0242ac120002, now(), 'login', '{"ip": "192.168.1.1"}');
```

### Query Data

```sql
-- Select all users
SELECT * FROM users;

-- Query with filtering (requires ALLOW FILTERING or index)
SELECT * FROM products WHERE category = 'Electronics' ALLOW FILTERING;

-- Time-based queries
SELECT * FROM events 
WHERE user_id = 522b1fe0-2e36-11ee-be56-0242ac120002 
LIMIT 10;
```

## Testing with C# Driver

### Update Configuration

Update your `appsettings.json` or test configuration:

```json
{
  "Cassandra": {
    "Seeds": ["localhost:9042", "localhost:9043", "localhost:9044"],
    "Keyspace": "test_keyspace",
    "User": null,
    "Password": null
  }
}
```

### Sample Test Code

```csharp
// Simple connectivity test
var config = new CassandraConfiguration
{
    Seeds = new List<string> { "localhost:9042" },
    Keyspace = "test_keyspace"
};

var options = Options.Create(config);
var logger = new LoggerFactory().CreateLogger<CassandraService>();
var cassandraService = new CassandraService(options, logger);

await cassandraService.StartAsync(CancellationToken.None);

// Execute a query
var result = await cassandraService.ExecuteAsync("SELECT * FROM users LIMIT 1");
Console.WriteLine($"Found {result.Count()} users");
```

## Monitoring the Cluster

### Check Node Status

```bash
# Overall cluster status
docker compose exec cassandra-seed nodetool status

# Ring information
docker compose exec cassandra-seed nodetool ring

# Compaction stats
docker compose exec cassandra-seed nodetool compactionstats

# Table stats
docker compose exec cassandra-seed nodetool tablestats test_keyspace
```

### View Logs

```bash
# View logs from all nodes
docker compose logs -f

# View logs from specific node
docker compose logs -f cassandra-seed

# Check for errors
docker compose logs | grep ERROR
```

## Performance Testing

### Create Larger Dataset

```bash
# Generate test data
docker compose exec cassandra-seed cqlsh -e "
USE test_keyspace;
INSERT INTO users (user_id, username, email, created_at) 
SELECT uuid(), 'user_' || toString(uuid()), 'user_' || toString(uuid()) || '@example.com', toTimestamp(now())
FROM system.local;
"

# Run multiple times to increase data
for i in {1..100}; do
  docker compose exec cassandra-seed cqlsh -e "
  USE test_keyspace;
  INSERT INTO products (product_id, name, price, category, in_stock) 
  VALUES (uuid(), 'Product_$i', $(($RANDOM % 1000)).99, 'Category_$(($RANDOM % 10))', true);
  "
done
```

### Stress Testing

```bash
# Use cassandra-stress tool
docker compose exec cassandra-seed cassandra-stress write n=10000 -rate threads=10

# Custom stress test
docker compose exec cassandra-seed cassandra-stress user \
  profile=/opt/cassandra/stress-profiles/example.yaml \
  n=10000 \
  ops(insert=1) \
  -rate threads=10
```

## Cleanup

### Stop Cluster (Keep Data)

```bash
# Stop all containers
docker compose stop

# Start again later
docker compose start
```

### Stop and Remove Everything

```bash
# Stop and remove containers, networks, and volumes
docker compose down -v

# Remove all data
docker volume prune -f
```

## Troubleshooting

### Node Won't Start

```bash
# Check logs
docker compose logs cassandra-seed

# Common issues:
# - Not enough memory: Increase Docker memory allocation
# - Port conflicts: Check ports 9042-9044 are free
# - Data corruption: Remove volumes and start fresh
```

### Connection Refused

```bash
# Ensure nodes are up
docker compose ps

# Check network connectivity
docker compose exec cassandra-seed ping cassandra-node1

# Verify CQL port is listening
docker compose exec cassandra-seed netstat -tlnp | grep 9042
```

### Slow Performance

```bash
# Check resource usage
docker stats

# Increase heap size in docker-compose.yml:
# MAX_HEAP_SIZE=1G
# HEAP_NEWSIZE=256M

# Restart cluster
docker compose restart
```

### Bootstrap Token Collision

If you encounter "Bootstrap Token collision" errors when starting nodes:

```bash
# Error: Bootstrap Token collision between nodes
# Solution: Ensure sequential startup using Docker dependencies

# The docker-compose.yml is configured for sequential startup:
# 1. cassandra-seed starts first
# 2. cassandra-node1 waits for seed to be healthy
# 3. cassandra-node2 waits for node1 to be healthy

# If issues persist, clean and restart:
docker compose down -v
docker compose up -d

# Monitor startup progress:
docker compose logs -f
```

## Advanced Configuration

### Multi-Datacenter Setup

Create a `docker-compose.multi-dc.yml` for testing multi-datacenter scenarios:

```yaml
version: '3.8'

services:
  # DC1 nodes...
  cassandra-dc1-seed:
    environment:
      - CASSANDRA_DC=dc1
      
  # DC2 nodes...
  cassandra-dc2-seed:
    environment:
      - CASSANDRA_DC=dc2
      - CASSANDRA_SEEDS=cassandra-dc1-seed
```

### SSL/TLS Configuration

See `docker-compose.ssl.yml` for an example with SSL enabled (requires certificate generation).

## Cassandra 5.0 Features

### Vector Search (Experimental)

```sql
-- Create a table with vector column
CREATE TABLE IF NOT EXISTS test_keyspace.embeddings (
    id UUID PRIMARY KEY,
    content TEXT,
    embedding VECTOR<FLOAT, 3>
);

-- Insert vector data
INSERT INTO test_keyspace.embeddings (id, content, embedding) 
VALUES (uuid(), 'sample text', [0.1, 0.2, 0.3]);

-- Vector similarity search
SELECT * FROM test_keyspace.embeddings 
ORDER BY embedding ANN OF [0.15, 0.25, 0.35] 
LIMIT 10;
```

### Dynamic Data Masking

```sql
-- Create a table with masked columns
CREATE TABLE IF NOT EXISTS test_keyspace.sensitive_data (
    id UUID PRIMARY KEY,
    ssn TEXT MASKED WITH DEFAULT,
    email TEXT MASKED WITH (INNER 3..3 '-'),
    salary DECIMAL MASKED WITH DEFAULT
);
```

### Storage Attached Indexes (SAI)

```sql
-- Create SAI indexes for better performance
CREATE CUSTOM INDEX IF NOT EXISTS idx_category 
ON test_keyspace.products (category) 
USING 'StorageAttachedIndex';

-- Now queries are more efficient
SELECT * FROM test_keyspace.products WHERE category = 'Electronics';
```

## Performance Considerations

### Cassandra 5.0 Improvements

1. **Trie-based Memtables**: Better memory efficiency and faster writes
2. **Unified Compaction Strategy**: Improved read/write balance
3. **Zero-copy Streaming**: Faster repairs and node additions
4. **Better Large Partition Support**: Handles large partitions more efficiently

### Monitoring Performance

```bash
# Check table statistics
docker compose exec cassandra-seed nodetool tablestats test_keyspace

# Monitor compactions
docker compose exec cassandra-seed nodetool compactionstats -H

# Check garbage collection
docker compose exec cassandra-seed nodetool gcstats
```

## Next Steps

1. Run the integration tests to verify the driver works correctly
2. Experiment with Cassandra 5.0's new features
3. Test failover scenarios by stopping nodes
4. Monitor performance with the new storage engine
5. Try vector search capabilities
6. Implement dynamic data masking for sensitive data

Happy testing with Cassandra 5.0! 🚀