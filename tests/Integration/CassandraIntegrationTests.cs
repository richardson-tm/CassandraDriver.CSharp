using Cassandra;
using CassandraDriver.Configuration;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace CassandraDriver.Tests.Integration;

/// <summary>
/// Integration tests that require a running Cassandra instance.
/// These tests are marked with a "Integration" trait and can be excluded from regular test runs.
/// 
/// To run these tests:
/// 1. Start a Cassandra instance (e.g., using Docker: docker run -d -p 9042:9042 cassandra:3.11)
/// 2. Run tests with: dotnet test --filter "Category=Integration"
/// </summary>
[Trait("Category", "Integration")]
public class CassandraIntegrationTests : IAsyncLifetime
{
    private CassandraService? _cassandraService;
    private readonly ILogger<CassandraService> _logger;

    public CassandraIntegrationTests()
    {
        _logger = new LoggerFactory().CreateLogger<CassandraService>();
    }

    public async Task InitializeAsync()
    {
        var configuration = new CassandraConfiguration
        {
            Seeds = new List<string> { "127.0.0.1" },
            Keyspace = null // Connect without keyspace initially
        };

        var options = Options.Create(configuration);
        _cassandraService = new CassandraService(options, _logger);
        
        await _cassandraService.StartAsync(CancellationToken.None);
        
        // Create test keyspace
        await CreateTestKeyspace();
    }

    public async Task DisposeAsync()
    {
        if (_cassandraService != null)
        {
            // Drop test keyspace
            await DropTestKeyspace();
            await _cassandraService.StopAsync(CancellationToken.None);
            _cassandraService.Dispose();
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ExecuteAsync_CanExecuteSimpleQuery()
    {
        // Arrange
        var query = "SELECT release_version FROM system.local";

        // Act
        var result = await _cassandraService!.ExecuteAsync(query);

        // Assert
        Assert.NotNull(result);
        var row = result.FirstOrDefault();
        Assert.NotNull(row);
        Assert.NotNull(row.GetValue<string>("release_version"));
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ExecuteAsync_CanCreateAndQueryTable()
    {
        // Arrange
        await _cassandraService!.ExecuteAsync("USE test_keyspace");
        
        // Create table
        await _cassandraService.ExecuteAsync(@"
            CREATE TABLE IF NOT EXISTS users (
                user_id UUID PRIMARY KEY,
                username TEXT,
                email TEXT,
                created_at TIMESTAMP
            )");

        var userId = Guid.NewGuid();
        var username = "testuser";
        var email = "test@example.com";
        var createdAt = DateTimeOffset.UtcNow;

        // Act - Insert data
        await _cassandraService.ExecuteAsync(
            "INSERT INTO users (user_id, username, email, created_at) VALUES (?, ?, ?, ?)",
            userId, username, email, createdAt);

        // Act - Query data
        var result = await _cassandraService.ExecuteAsync(
            "SELECT * FROM users WHERE user_id = ?", userId);

        // Assert
        var row = result.FirstOrDefault();
        Assert.NotNull(row);
        Assert.Equal(userId, row.GetValue<Guid>("user_id"));
        Assert.Equal(username, row.GetValue<string>("username"));
        Assert.Equal(email, row.GetValue<string>("email"));
        // DateTimeOffset is a value type, so we just verify it can be read
        var retrievedCreatedAt = row.GetValue<DateTimeOffset>("created_at");
        Assert.True(retrievedCreatedAt > DateTimeOffset.MinValue);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ExecuteAsync_CanUsePreparedStatements()
    {
        // Arrange
        await _cassandraService!.ExecuteAsync("USE test_keyspace");
        
        // Drop and recreate table to ensure clean state
        await _cassandraService.ExecuteAsync("DROP TABLE IF EXISTS products");
        await _cassandraService.ExecuteAsync(@"
            CREATE TABLE products (
                product_id UUID PRIMARY KEY,
                name TEXT,
                price DECIMAL
            )");

        var preparedStatement = await _cassandraService.Session.PrepareAsync(
            "INSERT INTO products (product_id, name, price) VALUES (?, ?, ?)");

        // Act - Insert multiple products using prepared statement
        var products = new[]
        {
            (Guid.NewGuid(), "Product 1", 10.99m),
            (Guid.NewGuid(), "Product 2", 20.99m),
            (Guid.NewGuid(), "Product 3", 30.99m)
        };

        foreach (var (id, name, price) in products)
        {
            var boundStatement = preparedStatement.Bind(id, name, price);
            await _cassandraService.ExecuteAsync(boundStatement);
        }

        // Query all products
        var result = await _cassandraService.ExecuteAsync("SELECT COUNT(*) as count FROM products");

        // Assert
        var row = result.FirstOrDefault();
        Assert.NotNull(row);
        Assert.Equal(3L, row.GetValue<long>("count"));
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ExecuteAsync_CanUseBatchStatements()
    {
        // Arrange
        await _cassandraService!.ExecuteAsync("USE test_keyspace");
        
        await _cassandraService.ExecuteAsync(@"
            CREATE TABLE IF NOT EXISTS events (
                event_id UUID PRIMARY KEY,
                event_type TEXT,
                timestamp TIMESTAMP
            )");

        var batch = new BatchStatement();
        var eventIds = new List<Guid>();

        // Act - Create batch of insert statements
        for (int i = 0; i < 5; i++)
        {
            var eventId = Guid.NewGuid();
            eventIds.Add(eventId);
            batch.Add(new SimpleStatement(
                "INSERT INTO events (event_id, event_type, timestamp) VALUES (?, ?, ?)",
                eventId, $"event_type_{i}", DateTimeOffset.UtcNow));
        }

        await _cassandraService.ExecuteAsync(batch);

        // Query events
        var result = await _cassandraService.ExecuteAsync(
            "SELECT * FROM events WHERE event_id IN ?", eventIds);

        // Assert
        Assert.Equal(5, result.Count());
    }

    private async Task CreateTestKeyspace()
    {
        await _cassandraService!.ExecuteAsync(@"
            CREATE KEYSPACE IF NOT EXISTS test_keyspace 
            WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }");
    }

    private async Task DropTestKeyspace()
    {
        try
        {
            await _cassandraService!.ExecuteAsync("DROP KEYSPACE IF EXISTS test_keyspace");
        }
        catch
        {
            // Ignore errors during cleanup
        }
    }
}