-- create-test-data.cql
-- Creates test keyspace and sample data for integration testing

-- Create test keyspace with replication factor 3
CREATE KEYSPACE IF NOT EXISTS test_keyspace 
WITH REPLICATION = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
} AND DURABLE_WRITES = true;

USE test_keyspace;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS time_series;

-- Create users table
CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    username TEXT,
    email TEXT,
    created_at TIMESTAMP,
    is_active BOOLEAN,
    metadata MAP<TEXT, TEXT>
) WITH COMMENT = 'User accounts table'
  AND default_time_to_live = 0
  AND gc_grace_seconds = 864000;

-- Create index on username for demo queries
CREATE INDEX IF NOT EXISTS idx_username ON users (username);

-- Create products table with multiple columns
CREATE TABLE products (
    product_id UUID PRIMARY KEY,
    name TEXT,
    description TEXT,
    price DECIMAL,
    category TEXT,
    tags SET<TEXT>,
    in_stock BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) WITH COMMENT = 'Products catalog'
  AND compression = {'class': 'LZ4Compressor'};

-- Create events table with compound primary key
CREATE TABLE events (
    user_id UUID,
    event_id TIMEUUID,
    event_type TEXT,
    event_data TEXT,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY (user_id, event_id)
) WITH CLUSTERING ORDER BY (event_id DESC)
  AND COMMENT = 'User activity events'
  AND default_time_to_live = 2592000; -- 30 days TTL

-- Create time series table for metrics
CREATE TABLE time_series (
    metric_name TEXT,
    timestamp TIMESTAMP,
    value DOUBLE,
    tags MAP<TEXT, TEXT>,
    PRIMARY KEY (metric_name, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND COMMENT = 'Time series metrics data'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 1
  };

-- Insert sample users
INSERT INTO users (user_id, username, email, created_at, is_active, metadata) 
VALUES (550e8400-e29b-41d4-a716-446655440001, 'john_doe', 'john@example.com', toTimestamp(now()), true, {'role': 'admin', 'department': 'IT'});

INSERT INTO users (user_id, username, email, created_at, is_active, metadata) 
VALUES (550e8400-e29b-41d4-a716-446655440002, 'jane_smith', 'jane@example.com', toTimestamp(now()), true, {'role': 'user', 'department': 'Sales'});

INSERT INTO users (user_id, username, email, created_at, is_active, metadata) 
VALUES (550e8400-e29b-41d4-a716-446655440003, 'bob_johnson', 'bob@example.com', toTimestamp(now()), false, {'role': 'user', 'department': 'Marketing'});

-- Insert sample products
INSERT INTO products (product_id, name, description, price, category, tags, in_stock, created_at, updated_at) 
VALUES (
    uuid(), 
    'MacBook Pro 16"', 
    'High-performance laptop for professionals', 
    2499.99, 
    'Electronics', 
    {'laptop', 'apple', 'professional'}, 
    true, 
    toTimestamp(now()), 
    toTimestamp(now())
);

INSERT INTO products (product_id, name, description, price, category, tags, in_stock, created_at, updated_at) 
VALUES (
    uuid(), 
    'Coffee Maker Deluxe', 
    'Premium coffee maker with grinder', 
    299.99, 
    'Appliances', 
    {'coffee', 'kitchen', 'premium'}, 
    true, 
    toTimestamp(now()), 
    toTimestamp(now())
);

INSERT INTO products (product_id, name, description, price, category, tags, in_stock, created_at, updated_at) 
VALUES (
    uuid(), 
    'Wireless Mouse', 
    'Ergonomic wireless mouse', 
    49.99, 
    'Electronics', 
    {'mouse', 'wireless', 'ergonomic'}, 
    false, 
    toTimestamp(now()), 
    toTimestamp(now())
);

-- Insert sample events
INSERT INTO events (user_id, event_id, event_type, event_data, ip_address, user_agent, created_at) 
VALUES (
    550e8400-e29b-41d4-a716-446655440001, 
    now(), 
    'login', 
    '{"success": true, "method": "password"}', 
    '192.168.1.100', 
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 
    toTimestamp(now())
);

INSERT INTO events (user_id, event_id, event_type, event_data, ip_address, user_agent, created_at) 
VALUES (
    550e8400-e29b-41d4-a716-446655440001, 
    now(), 
    'page_view', 
    '{"page": "/products", "duration": 45}', 
    '192.168.1.100', 
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 
    toTimestamp(now())
);

-- Insert sample time series data
INSERT INTO time_series (metric_name, timestamp, value, tags) 
VALUES ('cpu_usage', toTimestamp(now()), 45.2, {'host': 'server1', 'datacenter': 'dc1'});

INSERT INTO time_series (metric_name, timestamp, value, tags) 
VALUES ('memory_usage', toTimestamp(now()), 78.5, {'host': 'server1', 'datacenter': 'dc1'});

INSERT INTO time_series (metric_name, timestamp, value, tags) 
VALUES ('request_count', toTimestamp(now()), 1523, {'endpoint': '/api/users', 'method': 'GET'});

-- Display summary
SELECT COUNT(*) AS user_count FROM users;
SELECT COUNT(*) AS product_count FROM products;
SELECT COUNT(*) AS event_count FROM events;
SELECT COUNT(*) AS metric_count FROM time_series;