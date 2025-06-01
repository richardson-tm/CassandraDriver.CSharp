# CassandraDriver API Reference

This document provides a comprehensive API reference for the CassandraDriver library.

## Table of Contents

- [Core Services](#core-services)
- [Configuration](#configuration)
- [Mapping Attributes](#mapping-attributes)
- [Query Builders](#query-builders)
- [LINQ Provider](#linq-provider)
- [Resilience](#resilience)
- [Migrations](#migrations)
- [Telemetry](#telemetry)

## Core Services

### CassandraService

The main service for interacting with Cassandra.

```csharp
public class CassandraService : IHostedService, IDisposable
{
    // Properties
    public ISession Session { get; }
    public ICluster Cluster { get; }
    
    // Core Methods
    Task<RowSet> ExecuteAsync(IStatement statement);
    Task<RowSet> ExecuteAsync(string cql, params object[] values);
    Task<RowSet> ExecuteAsync(IStatement statement, ConsistencyLevel? consistencyLevel, ConsistencyLevel? serialConsistencyLevel);
    Task<RowSet> ExecuteAsync(string cql, ConsistencyLevel? consistencyLevel, ConsistencyLevel? serialConsistencyLevel, params object[] values);
    
    // Generic CRUD Methods
    Task<T?> GetAsync<T>(params object[] primaryKeyComponents) where T : class, new();
    Task<LwtResult<T>> InsertAsync<T>(T entity, bool ifNotExists = false, int? ttl = null, ConsistencyLevel? consistencyLevel = null, ConsistencyLevel? serialConsistencyLevel = null) where T : class, new();
    Task<LwtResult<T>> UpdateAsync<T>(T entity, int? ttl = null, string? ifCondition = null, ConsistencyLevel? consistencyLevel = null, ConsistencyLevel? serialConsistencyLevel = null) where T : class, new();
    Task DeleteAsync<T>(params object[] primaryKeyComponents) where T : class;
    Task DeleteAsync<T>(T entity, ConsistencyLevel? consistencyLevel = null) where T : class;
    
    // Query Builder
    SelectQueryBuilder<T> Query<T>() where T : class, new();
}
```

### CassandraMapperService

High-level service for simplified CRUD operations.

```csharp
public class CassandraMapperService
{
    Task<T?> GetAsync<T>(params object[] keys) where T : class, new();
    Task<LwtResult<T>> InsertAsync<T>(T entity, bool ifNotExists = false, int? ttl = null) where T : class, new();
    Task<LwtResult<T>> UpdateAsync<T>(T entity, int? ttl = null, string? ifCondition = null) where T : class, new();
    Task DeleteAsync<T>(params object[] keys) where T : class;
    Task DeleteAsync<T>(T entity) where T : class;
}
```

## Configuration

### CassandraConfiguration

```csharp
public class CassandraConfiguration
{
    // Connection
    public List<string> Seeds { get; set; }
    public string? Keyspace { get; set; }
    public string? User { get; set; }
    public string? Password { get; set; }
    public int? ProtocolVersion { get; set; }
    
    // Consistency
    public ConsistencyLevel? DefaultConsistencyLevel { get; set; }
    public ConsistencyLevel? DefaultSerialConsistencyLevel { get; set; }
    
    // Resilience
    public RetryPolicyConfiguration RetryPolicy { get; set; }
    public CircuitBreakerConfiguration CircuitBreaker { get; set; }
    
    // Performance
    public bool SpeculativeExecutionEnabled { get; set; }
    public double SpeculativeRetryPercentile { get; set; }
    public int MaxSpeculativeExecutions { get; set; }
    
    // Connection Pooling
    public PoolingOptionsConfiguration? Pooling { get; set; }
    public QueryOptionsConfiguration? QueryOptions { get; set; }
    
    // SSL/TLS
    public SslConfiguration? Truststore { get; set; }
    public SslConfiguration? Keystore { get; set; }
    
    // Migrations
    public MigrationConfiguration Migrations { get; set; }
}
```

### RetryPolicyConfiguration

```csharp
public class RetryPolicyConfiguration
{
    public bool Enabled { get; set; } = true;
    public int MaxRetries { get; set; } = 3;
    public int DelayMilliseconds { get; set; } = 100;
    public int MaxDelayMilliseconds { get; set; } = 2000;
    public string[] RetryableExceptions { get; set; }
}
```

### CircuitBreakerConfiguration

```csharp
public class CircuitBreakerConfiguration
{
    public bool Enabled { get; set; } = false;
    public double FailureThreshold { get; set; } = 0.5;
    public int SamplingDuration { get; set; } = 10;
    public int MinimumThroughput { get; set; } = 10;
    public int BreakDuration { get; set; } = 30;
}
```

## Mapping Attributes

### [Table]

Specifies the Cassandra table name for an entity.

```csharp
[Table("users")]
public class User { }
```

### [PartitionKey]

Marks a property as a partition key.

```csharp
[PartitionKey(0)] // Optional order parameter for composite keys
public Guid Id { get; set; }
```

### [ClusteringKey]

Marks a property as a clustering key.

```csharp
[ClusteringKey(0, ClusteringOrder.Descending)]
public DateTime CreatedAt { get; set; }
```

### [Column]

Maps a property to a specific column name and optionally specifies the Cassandra type.

```csharp
[Column("email_address", TypeName = "text")]
public string Email { get; set; }
```

### [Computed]

Specifies a computed column using a CQL expression.

```csharp
[Computed("toTimestamp(created_at)")]
public long CreatedTimestamp { get; set; }
```

### [Ignore]

Excludes a property from mapping.

```csharp
[Ignore]
public string TempData { get; set; }
```

### [Udt]

Maps a property to a User-Defined Type.

```csharp
[Udt("address")]
public Address HomeAddress { get; set; }
```

## Query Builders

### SelectQueryBuilder<T>

```csharp
public class SelectQueryBuilder<T>
{
    // Column Selection
    SelectQueryBuilder<T> Select(params string[] columns);
    SelectQueryBuilder<T> Select<TProperty>(Expression<Func<T, TProperty>> propertyExpression);
    
    // Filtering
    SelectQueryBuilder<T> Where(string condition, params object[] values);
    SelectQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertyExpression, QueryOperator op, TProperty value);
    SelectQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertyExpression, QueryOperator op, IEnumerable<TProperty> values);
    
    // Ordering
    SelectQueryBuilder<T> OrderBy(string column, bool ascending = true);
    SelectQueryBuilder<T> OrderBy<TProperty>(Expression<Func<T, TProperty>> propertyExpression, bool ascending = true);
    
    // Limiting
    SelectQueryBuilder<T> Limit(int count);
    SelectQueryBuilder<T> PageSize(int size);
    
    // Execution
    Task<List<T>> ToListAsync();
    Task<T?> FirstOrDefaultAsync();
    IAsyncEnumerable<T> ToAsyncEnumerable(CancellationToken cancellationToken = default);
    SimpleStatement BuildStatement();
}
```

### InsertQueryBuilder<T>

```csharp
public class InsertQueryBuilder<T>
{
    InsertQueryBuilder<T> Into(string tableName);
    InsertQueryBuilder<T> Value<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value);
    (string Query, List<object> Parameters) Build();
}
```

### UpdateQueryBuilder<T>

```csharp
public class UpdateQueryBuilder<T>
{
    UpdateQueryBuilder<T> Table(string tableName);
    UpdateQueryBuilder<T> Set<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value);
    UpdateQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value);
    UpdateQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertySelector, string comparisonOperator, TProperty value);
    (string Query, List<object> Parameters) Build();
}
```

### DeleteQueryBuilder<T>

```csharp
public class DeleteQueryBuilder<T>
{
    DeleteQueryBuilder<T> From(string tableName);
    DeleteQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value);
    DeleteQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertySelector, string comparisonOperator, TProperty value);
    string Build();
}
```

## LINQ Provider

### Creating a Queryable

```csharp
var users = cassandraService.CreateQueryable<User>();
```

### Supported Operations

```csharp
// Where
var activeUsers = users.Where(u => u.IsActive);

// Select (projection)
var userNames = users.Select(u => u.Name);

// OrderBy/OrderByDescending
var sortedUsers = users.OrderBy(u => u.CreatedAt);

// Take (limit)
var topUsers = users.Take(10);

// Combined
var result = await users
    .Where(u => u.IsActive && u.CreatedAt > cutoffDate)
    .OrderByDescending(u => u.CreatedAt)
    .Take(50)
    .ToListAsync();
```

## Resilience

### RetryPolicyFactory

```csharp
public static class RetryPolicyFactory
{
    public static IAsyncPolicy<RowSet> Create(RetryPolicyConfiguration config, ILogger logger);
}
```

### CircuitBreakerPolicyFactory

```csharp
public static class CircuitBreakerPolicyFactory
{
    public static IAsyncPolicy<RowSet> Create(CircuitBreakerConfiguration config, ILogger logger);
}
```

## Migrations

### Migration Base Class

```csharp
public abstract class Migration
{
    public abstract int Version { get; }
    public abstract string Name { get; }
    public abstract string GetCql();
}
```

### CassandraMigrationRunner

```csharp
public class CassandraMigrationRunner
{
    Task ApplyMigrationsAsync();
    Task<List<Migration>> GetPendingMigrationsAsync();
}
```

## Telemetry

### DriverMetrics

```csharp
public static class DriverMetrics
{
    // Counters
    public static readonly Counter<long> QueriesStarted;
    public static readonly Counter<long> QueriesSucceeded;
    public static readonly Counter<long> QueriesFailed;
    
    // Histogram
    public static readonly Histogram<double> QueryDurationMilliseconds;
    
    // Prepared Statement Metrics
    public static readonly Counter<long> PreparedStatementsCached;
    public static readonly Counter<long> PreparedStatementsEvicted;
    
    // Tag Keys
    public static class TagKeys
    {
        public const string CqlOperation = "cql.operation";
        public const string ExceptionType = "exception.type";
    }
}
```

## Extension Methods

### ServiceCollectionExtensions

```csharp
public static class ServiceCollectionExtensions
{
    // Configuration from IConfiguration
    public static IServiceCollection AddCassandra(this IServiceCollection services, IConfiguration configuration);
    
    // Configuration with action
    public static IServiceCollection AddCassandra(this IServiceCollection services, Action<CassandraConfiguration> configureOptions);
}
```

### RxCassandraExtensions

```csharp
public static class RxCassandraExtensions
{
    public static IObservable<T> ToObservable<T>(this SelectQueryBuilder<T> queryBuilder) where T : class, new();
}
```

## Results

### LwtResult<T>

Result of a Lightweight Transaction operation.

```csharp
public class LwtResult<T> where T : class
{
    public bool Applied { get; }
    public RowSet RowSet { get; }
    public T? Entity { get; }
}
```

## Health Checks

### CassandraHealthCheck

```csharp
public class CassandraHealthCheck : IHealthCheck
{
    Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default);
}
```

## Schema Management

### SchemaGenerator

```csharp
public class SchemaGenerator
{
    string GenerateCreateTable(Type entityType);
    string GenerateCreateKeyspace(string keyspace, string strategyClass, Dictionary<string, int>? dataCenterReplication = null, int? replicationFactor = null);
}
```

### SchemaManager

```csharp
public class SchemaManager
{
    Task CreateKeyspaceIfNotExistsAsync(string keyspace, string strategyClass, Dictionary<string, int>? dataCenterReplication = null, int? replicationFactor = null);
    Task CreateTableIfNotExistsAsync(Type entityType);
    Task SynchronizeSchemaAsync(params Type[] entityTypes);
}
```

---

For more examples and detailed usage, see the [README.md](README.md) and test projects.