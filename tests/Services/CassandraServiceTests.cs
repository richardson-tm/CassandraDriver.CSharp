using Cassandra;
using CassandraDriver.Configuration;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace CassandraDriver.Tests.Services;

public class CassandraServiceTests : IDisposable
{
    private readonly Mock<IOptions<CassandraConfiguration>> _mockOptions;
    private readonly Mock<ILogger<CassandraService>> _mockLogger;
    private readonly CassandraConfiguration _configuration;
    private CassandraService? _service;

    public CassandraServiceTests()
    {
        _mockOptions = new Mock<IOptions<CassandraConfiguration>>();
        _mockLogger = new Mock<ILogger<CassandraService>>();
        _configuration = new CassandraConfiguration();
        _mockOptions.Setup(x => x.Value).Returns(_configuration);
    }

    [Fact]
    public void Constructor_InitializesWithConfiguration()
    {
        // Arrange & Act
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Assert
        Assert.NotNull(_service);
    }

    [Fact]
    public void Session_ThrowsWhenNotInitialized()
    {
        // Arrange
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => _service.Session);
        Assert.Contains("Session is not initialized", exception.Message);
    }

    [Fact]
    public void Cluster_ThrowsWhenNotInitialized()
    {
        // Arrange
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => _service.Cluster);
        Assert.Contains("Cluster is not initialized", exception.Message);
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsWhenSessionNotInitialized()
    {
        // Arrange
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);
        var statement = new SimpleStatement("SELECT * FROM test");

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => _service.ExecuteAsync(statement));
    }

    [Fact]
    public async Task ExecuteAsync_WithCql_ThrowsWhenSessionNotInitialized()
    {
        // Arrange
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => _service.ExecuteAsync("SELECT * FROM test"));
    }

    [Fact]
    public void Dispose_DoesNotThrowWhenNotInitialized()
    {
        // Arrange
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Act & Assert (should not throw)
        _service.Dispose();
    }

    [Fact]
    public async Task StopAsync_DoesNotThrowWhenNotInitialized()
    {
        // Arrange
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Act & Assert (should not throw)
        await _service.StopAsync(CancellationToken.None);
    }

    [Theory]
    [InlineData("127.0.0.1")]
    [InlineData("localhost:9042")]
    [InlineData("192.168.1.1:9142")]
    public void Configuration_ParsesSeedsCorrectly(string seed)
    {
        // Arrange
        _configuration.Seeds = new List<string> { seed };
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Assert
        Assert.Contains(seed, _configuration.Seeds);
    }

    [Fact]
    public void Configuration_HandlesMultipleSeeds()
    {
        // Arrange
        var seeds = new List<string> { "node1", "node2:9042", "node3:9142" };
        _configuration.Seeds = seeds;
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Assert
        Assert.Equal(3, _configuration.Seeds.Count);
        foreach (var seed in seeds)
        {
            Assert.Contains(seed, _configuration.Seeds);
        }
    }

    [Fact]
    public void Configuration_SetsAuthenticationWhenProvided()
    {
        // Arrange
        _configuration.User = "testuser";
        _configuration.Password = "testpass";
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Assert
        Assert.Equal("testuser", _configuration.User);
        Assert.Equal("testpass", _configuration.Password);
    }

    [Fact]
    public void Configuration_SetsProtocolVersionWhenProvided()
    {
        // Arrange
        _configuration.ProtocolVersion = 4;
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Assert
        Assert.Equal(4, _configuration.ProtocolVersion);
    }

    [Fact]
    public void Configuration_EnablesSpeculativeExecution()
    {
        // Arrange
        _configuration.SpeculativeExecutionEnabled = true;
        _configuration.SpeculativeRetryPercentile = 95.0;
        _configuration.MaxSpeculativeExecutions = 2;
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Assert
        Assert.True(_configuration.SpeculativeExecutionEnabled);
        Assert.Equal(95.0, _configuration.SpeculativeRetryPercentile);
        Assert.Equal(2, _configuration.MaxSpeculativeExecutions);
    }

    [Fact]
    public void Configuration_SetsKeyspaceWhenProvided()
    {
        // Arrange
        _configuration.Keyspace = "test_keyspace";
        _service = new CassandraService(_mockOptions.Object, _mockLogger.Object);

        // Assert
        Assert.Equal("test_keyspace", _configuration.Keyspace);
    }

    public void Dispose()
    {
        _service?.Dispose();
    }
}