# CassandraDriver for C#

[![NuGet](https://img.shields.io/nuget/v/CassandraDriver.svg)](https://www.nuget.org/packages/CassandraDriver/)
[![Build Status](https://github.com/richardson-tm/CassandraDriver.CSharp/actions/workflows/dotnet-build.yml/badge.svg)](https://github.com/richardson-tm/CassandraDriver.CSharp/actions)
[![Tests](https://img.shields.io/badge/tests-150%2B-brightgreen.svg)]()
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A comprehensive, production-ready C# Cassandra driver with advanced features including mapping, LINQ support, migrations, resilience patterns, and health monitoring. Originally ported from the battle-tested Java Ratpack Cassandra module and significantly enhanced.

## 🚀 Features

### Core Features
- **🔌 Connection Management**: Automatic pooling, reconnection, and session lifecycle management
- **🗺️ Object Mapping**: Attribute-based entity mapping with support for all Cassandra types
- **🔍 Query Builders**: Type-safe, fluent API for CRUD operations
- **⚖️ Smart Load Balancing**: Token-aware and datacenter-aware routing for optimal performance
- **🔒 SSL/TLS Support**: Secure connections with mutual TLS authentication
- **🏥 Health Monitoring**: Built-in ASP.NET Core health checks with detailed diagnostics
- **💉 Dependency Injection**: First-class Microsoft.Extensions.DependencyInjection integration

### Advanced Features
- **🔄 Schema Migrations**: Automatic schema versioning and migration support
- **📊 LINQ Provider**: Write queries in C# with full IntelliSense support
- **💪 Resilience**: Polly-based retry policies and circuit breakers
- **📈 Telemetry**: OpenTelemetry metrics for monitoring and alerting
- **⚡ Speculative Execution**: Reduce p99 latencies with configurable retry policies
- **🎯 Lightweight Transactions**: Support for compare-and-set operations with TTL
- **🔷 User-Defined Types**: Full support for complex custom types
- **📡 Reactive Extensions**: Rx.NET support for reactive programming
- **🚀 Statement Caching**: Automatic prepared statement management
- **☁️ Cloud Ready**: AWS EC2 multi-region support with address translation

## 📦 Installation

### Via NuGet
```bash
dotnet add package CassandraDriver
```

### From Source
```bash
git clone https://github.com/richardson-tm/CassandraDriver.CSharp.git
cd CassandraDriver.CSharp
dotnet build
```

## 🏃 Quick Start

### 1. Add Configuration

Add to your `appsettings.json`:

```json
{
  "Cassandra": {
    "Seeds": ["localhost:9042"],
    "Keyspace": "my_keyspace",
    "User": "cassandra",
    "Password": "cassandra",
    "Migrations": {
      "Enabled": true,
      "KeyspaceClass": "SimpleStrategy",
      "ReplicationFactor": 1
    }
  }
}
```

### 2. Register Services

In `Program.cs`:

```csharp
using CassandraDriver.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Add Cassandra services with all features
builder.Services.AddCassandra(builder.Configuration);

// Add health checks
builder.Services.AddHealthChecks();

var app = builder.Build();

// Map health endpoint
app.MapHealthChecks("/health");

app.Run();
```

### 3. Define Your Entities

```csharp
using CassandraDriver.Mapping.Attributes;

[Table("users")]
public class User
{
    [PartitionKey]
    public Guid Id { get; set; }
    
    [ClusteringKey(0)]
    public DateTime CreatedAt { get; set; }
    
    [Column("user_name")]
    public string Username { get; set; } = string.Empty;
    
    [Column("email_address")]
    public string Email { get; set; } = string.Empty;
    
    [Column("is_active")]
    public bool IsActive { get; set; }
    
    [Computed("toTimestamp(created_at)")]
    public long CreatedTimestamp { get; set; }
    
    [Ignore]
    public string TempData { get; set; } = string.Empty;
}
```

### 4. Use the CassandraMapper

```csharp
public class UserService
{
    private readonly CassandraMapperService _mapper;
    
    public UserService(CassandraMapperService mapper)
    {
        _mapper = mapper;
    }
    
    // Simple CRUD operations
    public async Task<User?> GetUserAsync(Guid id, DateTime createdAt)
    {
        return await _mapper.GetAsync<User>(id, createdAt);
    }
    
    public async Task CreateUserAsync(User user)
    {
        // With optional TTL and if-not-exists
        var result = await _mapper.InsertAsync(user, 
            ifNotExists: true, 
            ttl: 3600);
            
        if (!result.Applied)
        {
            // User already exists
            var existingUser = result.Entity;
        }
    }
    
    public async Task UpdateUserAsync(User user)
    {
        // With lightweight transaction
        var result = await _mapper.UpdateAsync(user,
            ifCondition: "is_active = true");
            
        if (!result.Applied)
        {
            // Condition failed
        }
    }
    
    public async Task DeleteUserAsync(Guid id, DateTime createdAt)
    {
        await _mapper.DeleteAsync<User>(id, createdAt);
    }
}
```

### 5. Use Query Builders

```csharp
public class UserRepository
{
    private readonly CassandraService _cassandra;
    
    public UserRepository(CassandraService cassandra)
    {
        _cassandra = cassandra;
    }
    
    public async Task<List<User>> GetActiveUsersAsync(int limit = 100)
    {
        return await _cassandra.Query<User>()
            .Where(u => u.IsActive, QueryOperator.Equals, true)
            .OrderBy(u => u.CreatedAt, ascending: false)
            .Limit(limit)
            .ToListAsync();
    }
    
    public async IAsyncEnumerable<User> StreamAllUsersAsync()
    {
        await foreach (var user in _cassandra.Query<User>()
            .PageSize(1000)
            .ToAsyncEnumerable())
        {
            yield return user;
        }
    }
    
    public async Task<User?> FindByEmailAsync(string email)
    {
        // Using secondary index or materialized view
        return await _cassandra.Query<User>()
            .Where("email_address = ?", email)
            .FirstOrDefaultAsync();
    }
}
```

### 6. Use LINQ Provider

```csharp
public class UserAnalytics
{
    private readonly IQueryable<User> _users;
    
    public UserAnalytics(CassandraService cassandra)
    {
        // Create LINQ queryable
        _users = cassandra.CreateQueryable<User>();
    }
    
    public async Task<List<User>> GetRecentActiveUsersAsync()
    {
        var cutoffDate = DateTime.UtcNow.AddDays(-7);
        
        return await _users
            .Where(u => u.IsActive && u.CreatedAt > cutoffDate)
            .OrderByDescending(u => u.CreatedAt)
            .Take(50)
            .ToListAsync();
    }
}
```

## 🔧 Advanced Configuration

### Complete Configuration Example

```json
{
  "Cassandra": {
    "Seeds": ["node1:9042", "node2:9042", "node3:9042"],
    "Keyspace": "production",
    "User": "app_user",
    "Password": "secure_password",
    "ProtocolVersion": 4,
    "DefaultConsistencyLevel": "LocalQuorum",
    "DefaultSerialConsistencyLevel": "Serial",
    
    "RetryPolicy": {
      "Enabled": true,
      "MaxRetries": 3,
      "DelayMilliseconds": 100,
      "MaxDelayMilliseconds": 2000,
      "RetryableExceptions": [
        "OperationTimedOutException",
        "UnavailableException",
        "ReadTimeoutException",
        "WriteTimeoutException"
      ]
    },
    
    "CircuitBreaker": {
      "Enabled": true,
      "FailureThreshold": 0.5,
      "SamplingDuration": 10,
      "MinimumThroughput": 10,
      "BreakDuration": 30
    },
    
    "Pooling": {
      "CoreConnectionsPerHostLocal": 2,
      "MaxConnectionsPerHostLocal": 8,
      "CoreConnectionsPerHostRemote": 1,
      "MaxConnectionsPerHostRemote": 2,
      "MaxRequestsPerConnection": 2048,
      "HeartbeatIntervalMillis": 30000
    },
    
    "QueryOptions": {
      "DefaultPageSize": 5000,
      "PrepareStatementsOnAllHosts": true,
      "ReprepareStatementsOnUp": true
    },
    
    "SpeculativeExecutionEnabled": true,
    "SpeculativeRetryPercentile": 99.0,
    "MaxSpeculativeExecutions": 3,
    
    "Migrations": {
      "Enabled": true,
      "MigrationsPath": "Migrations",
      "KeyspaceClass": "NetworkTopologyStrategy",
      "DataCenterReplication": {
        "dc1": 3,
        "dc2": 2
      }
    },
    
    "Truststore": {
      "Path": "/certs/truststore.pfx",
      "Password": "truststore_password"
    },
    "Keystore": {
      "Path": "/certs/keystore.pfx",
      "Password": "keystore_password"
    }
  }
}
```

## 📊 Schema Migrations

Create migration files in your project:

```csharp
// Migrations/001_CreateUsersTable.cs
public class CreateUsersTable : Migration
{
    public override int Version => 1;
    public override string Name => "Create users table";
    
    public override string GetCql()
    {
        return @"
            CREATE TABLE IF NOT EXISTS users (
                id UUID,
                created_at TIMESTAMP,
                user_name TEXT,
                email_address TEXT,
                is_active BOOLEAN,
                PRIMARY KEY (id, created_at)
            ) WITH CLUSTERING ORDER BY (created_at DESC)
              AND default_time_to_live = 0
              AND gc_grace_seconds = 864000;
            
            CREATE INDEX IF NOT EXISTS ON users (email_address);
        ";
    }
}
```

Migrations run automatically on startup when enabled, or manually:

```csharp
public class MigrationController : ControllerBase
{
    private readonly CassandraMigrationRunner _migrationRunner;
    
    [HttpPost("migrate")]
    public async Task<IActionResult> RunMigrations()
    {
        await _migrationRunner.ApplyMigrationsAsync();
        return Ok("Migrations completed");
    }
}
```

## 🛡️ Resilience Patterns

### Retry Policy

```csharp
// Configure custom retry policy
services.AddCassandra(options =>
{
    options.RetryPolicy = new RetryPolicyConfiguration
    {
        Enabled = true,
        MaxRetries = 5,
        DelayMilliseconds = 200,
        RetryableExceptions = new[]
        {
            nameof(OperationTimedOutException),
            nameof(OverloadedException)
        }
    };
});
```

### Circuit Breaker

```csharp
// Circuit breaker prevents cascading failures
services.AddCassandra(options =>
{
    options.CircuitBreaker = new CircuitBreakerConfiguration
    {
        Enabled = true,
        FailureThreshold = 0.25, // 25% failure rate
        SamplingDuration = 60,   // Over 60 seconds
        BreakDuration = 30       // Break for 30 seconds
    };
});
```

## 📈 Metrics and Monitoring

The driver automatically exports OpenTelemetry metrics:

```csharp
// Configure OpenTelemetry
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics
            .AddMeter("CassandraDriver")
            .AddPrometheusExporter();
    });
```

Available metrics:
- `cassandra_queries_started_total`
- `cassandra_queries_succeeded_total`
- `cassandra_queries_failed_total`
- `cassandra_query_duration_ms`
- `cassandra_prepared_statements_cached`
- `cassandra_prepared_statements_evicted`

## 🔄 Reactive Extensions

```csharp
using CassandraDriver.Reactive;

public class ReactiveUserService
{
    private readonly CassandraService _cassandra;
    
    public IObservable<User> GetUsersStream()
    {
        return _cassandra.Query<User>()
            .Where(u => u.IsActive, QueryOperator.Equals, true)
            .ToObservable()
            .Buffer(TimeSpan.FromSeconds(1))
            .SelectMany(batch => batch);
    }
}
```

## 🧪 Testing

### Unit Testing with Mocks

```csharp
[Fact]
public async Task GetUser_ReturnsUser_WhenExists()
{
    // Arrange
    var mockCassandra = new Mock<CassandraService>();
    var expectedUser = new User { Id = Guid.NewGuid() };
    
    mockCassandra
        .Setup(c => c.Query<User>())
        .Returns(new TestQueryBuilder<User>(expectedUser));
    
    var service = new UserService(mockCassandra.Object);
    
    // Act
    var user = await service.GetUserAsync(expectedUser.Id);
    
    // Assert
    Assert.Equal(expectedUser.Id, user.Id);
}
```

### Integration Testing

```bash
# Start test cluster
./scripts/setup-test-cluster.sh

# Run all tests
dotnet test

# Run only integration tests
dotnet test --filter "Category=Integration"
```

## 🐛 Troubleshooting

### Enable Debug Logging

```json
{
  "Logging": {
    "LogLevel": {
      "CassandraDriver": "Debug",
      "Cassandra": "Debug"
    }
  }
}
```

### Common Issues

1. **Connection Timeouts**: Increase `SocketOptions.ConnectTimeoutMillis`
2. **Prepared Statement Cache**: Monitor eviction metrics
3. **Memory Usage**: Tune `DefaultPageSize` and connection pool settings

## 🤝 Contributing

See [CHANGELOG.md](CHANGELOG.md) for version history and [TODO.md](TODO.md) for planned features.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Original Java Ratpack Cassandra module contributors
- DataStax C# Driver team
- .NET community

---

<p align="center">
  Made with ❤️ for the C# and Cassandra communities
</p>