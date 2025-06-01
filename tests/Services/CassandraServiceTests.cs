using Cassandra;
using CassandraDriver.Configuration;
using CassandraDriver.Services;
using CassandraDriver.Mapping;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Cassandra.Exceptions; // Added for exception types

namespace CassandraDriver.Tests.Services;

public class CassandraServiceTests : IDisposable
{
    private readonly Mock<IOptions<CassandraConfiguration>> _mockOptions;
    private readonly Mock<ILogger<CassandraService>> _mockLogger;
    private readonly CassandraConfiguration _configuration;
    private readonly Mock<TableMappingResolver> _mockMappingResolver;
    private readonly Mock<ILoggerFactory> _mockLoggerFactory;
    private TestCassandraService? _service; // Changed to TestCassandraService
    private Mock<ISession> _mockSession;
    private Mock<ICluster> _mockCluster;

    // Helper class for testing
    private class TestCassandraService : CassandraService
    {
        public Mock<ISession> MockSession { get; }
        public Mock<ICluster> MockCluster { get; }

        public TestCassandraService(
            IOptions<CassandraConfiguration> configuration,
            ILogger<CassandraService> logger,
            TableMappingResolver mappingResolver,
            ILoggerFactory loggerFactory,
            Mock<ISession> mockSession,
            Mock<ICluster> mockCluster)
            : base(configuration, logger, mappingResolver, loggerFactory)
        {
            MockSession = mockSession;
            MockCluster = mockCluster;
        }

        public override ISession Session => MockSession.Object;
        public override ICluster Cluster => MockCluster.Object;
        
        // Helper to access configuration for tests
        public CassandraConfiguration GetConfigurationForTests() => base._configuration;
    }

    public CassandraServiceTests()
    {
        _mockOptions = new Mock<IOptions<CassandraConfiguration>>();
        _mockLogger = new Mock<ILogger<CassandraService>>();
        _configuration = new CassandraConfiguration();
        _configuration.RetryPolicy ??= new RetryPolicyConfiguration();
        _mockOptions.Setup(x => x.Value).Returns(_configuration);
        
        _mockMappingResolver = new Mock<TableMappingResolver>();
        _mockLoggerFactory = new Mock<ILoggerFactory>();
        _mockLoggerFactory.Setup(x => x.CreateLogger(It.IsAny<string>())).Returns(new Mock<ILogger>().Object);
        
        _mockSession = new Mock<ISession>();
        _mockCluster = new Mock<ICluster>();

        // Setup default behavior for session execute
        _mockSession.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
            .ReturnsAsync(new RowSet());

        _service = new TestCassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object, _mockSession, _mockCluster);
    }

    [Fact]
    public void Constructor_InitializesWithConfiguration()
    {
        // Service is already initialized in constructor. This test can verify non-null or specific config.
        Assert.NotNull(_service);
        Assert.NotNull(_service!.GetConfigurationForTests());
    }

    [Fact]
    public void Session_ThrowsWhenNotInitialized()
    {
        // Arrange
        var serviceWithoutStart = new CassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => serviceWithoutStart.Session);
        Assert.Contains("Session is not initialized", exception.Message);
    }

    [Fact]
    public void Cluster_ThrowsWhenNotInitialized()
    {
        // Arrange
        var serviceWithoutStart = new CassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => serviceWithoutStart.Cluster);
        Assert.Contains("Cluster is not initialized", exception.Message);
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsWhenSessionNotInitialized()
    {
        // Arrange
        var serviceWithoutStart = new CassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object);
        var statement = new SimpleStatement("SELECT * FROM test");

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => serviceWithoutStart.ExecuteAsync(statement));
    }

    [Fact]
    public async Task ExecuteAsync_WithCql_ThrowsWhenSessionNotInitialized()
    {
        // Arrange
        var serviceWithoutStart = new CassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => serviceWithoutStart.ExecuteAsync("SELECT * FROM test"));
    }

    [Fact]
    public void Dispose_DoesNotThrowWhenNotInitialized()
    {
        // Arrange
        var serviceWithoutStart = new CassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object);

        // Act & Assert (should not throw)
        serviceWithoutStart.Dispose();
    }

    [Fact]
    public async Task StopAsync_DoesNotThrowWhenNotInitialized()
    {
        // Arrange
        var serviceWithoutStart = new CassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object);

        // Act & Assert (should not throw)
        await serviceWithoutStart.StopAsync(CancellationToken.None);
    }

    [Theory]
    [InlineData("127.0.0.1")]
    [InlineData("localhost:9042")]
    [InlineData("192.168.1.1:9142")]
    public void Configuration_ParsesSeedsCorrectly(string seed)
    {
        // Arrange
        _configuration.Seeds = new List<string> { seed };

        // Assert
        Assert.Contains(seed, _configuration.Seeds);
    }

    [Fact]
    public void Configuration_HandlesMultipleSeeds()
    {
        // Arrange
        var seeds = new List<string> { "node1", "node2:9042", "node3:9142" };
        _configuration.Seeds = seeds;

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

        // Assert
        Assert.Equal("testuser", _configuration.User);
        Assert.Equal("testpass", _configuration.Password);
    }

    [Fact]
    public void Configuration_SetsProtocolVersionWhenProvided()
    {
        // Arrange
        _configuration.ProtocolVersion = 4;

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

        // Assert
        Assert.Equal("test_keyspace", _configuration.Keyspace);
    }

    public void Dispose()
    {
        _service?.Dispose();
    }

    [Fact]
    public async Task ExecuteAsync_Statement_UsesPerQueryConsistencyLevel()
    {
        // Arrange
        var statement = new SimpleStatement("SELECT * FROM test");
        var perQueryConsistencyLevel = ConsistencyLevel.EachQuorum;
        _configuration.DefaultConsistencyLevel = ConsistencyLevel.One; // Set a default to ensure per-query overrides it

        // Act
        await _service!.ExecuteAsync(statement, perQueryConsistencyLevel, null);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.ConsistencyLevel == perQueryConsistencyLevel)), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Statement_UsesDefaultConsistencyLevel_WhenPerQueryIsNull()
    {
        // Arrange
        var statement = new SimpleStatement("SELECT * FROM test");
        var defaultConsistencyLevel = ConsistencyLevel.Quorum;
        _configuration.DefaultConsistencyLevel = defaultConsistencyLevel;

        // Act
        await _service!.ExecuteAsync(statement, null, null);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.ConsistencyLevel == defaultConsistencyLevel)), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Statement_UsesDriverDefaultConsistencyLevel_WhenNoLevelSet()
    {
        // Arrange
        var statement = new SimpleStatement("SELECT * FROM test");
        // Ensure no default is set in configuration
        _configuration.DefaultConsistencyLevel = null;

        // Act
        await _service!.ExecuteAsync(statement, null, null);

        // Assert
        // The driver's default is not setting the ConsistencyLevel property on the statement.
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.ConsistencyLevel == null)), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Statement_UsesPerQuerySerialConsistencyLevel()
    {
        // Arrange
        var statement = new SimpleStatement("SELECT * FROM test");
        var perQuerySerialConsistencyLevel = ConsistencyLevel.Serial;
        _configuration.DefaultSerialConsistencyLevel = ConsistencyLevel.LocalSerial; // Set a default to ensure per-query overrides it

        // Act
        await _service!.ExecuteAsync(statement, null, perQuerySerialConsistencyLevel);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.SerialConsistencyLevel == perQuerySerialConsistencyLevel)), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Statement_UsesDefaultSerialConsistencyLevel_WhenPerQueryIsNull()
    {
        // Arrange
        var statement = new SimpleStatement("SELECT * FROM test");
        var defaultSerialConsistencyLevel = ConsistencyLevel.Serial;
        _configuration.DefaultSerialConsistencyLevel = defaultSerialConsistencyLevel;

        // Act
        await _service!.ExecuteAsync(statement, null, null);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.SerialConsistencyLevel == defaultSerialConsistencyLevel)), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Statement_UsesDriverDefaultSerialConsistencyLevel_WhenNoLevelSet()
    {
        // Arrange
        var statement = new SimpleStatement("SELECT * FROM test");
        _configuration.DefaultSerialConsistencyLevel = null;

        // Act
        await _service!.ExecuteAsync(statement, null, null);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.SerialConsistencyLevel == null)), Times.Once);
    }

    // Tests for ExecuteAsync(string cql, ...) overload

    [Fact]
    public async Task ExecuteAsync_Cql_UsesPerQueryConsistencyLevel()
    {
        // Arrange
        var cql = "SELECT * FROM test";
        var perQueryConsistencyLevel = ConsistencyLevel.EachQuorum;
        _configuration.DefaultConsistencyLevel = ConsistencyLevel.One;

        // Act
        await _service!.ExecuteAsync(cql, perQueryConsistencyLevel, null);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.ConsistencyLevel == perQueryConsistencyLevel)), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Cql_UsesDefaultConsistencyLevel_WhenPerQueryIsNull()
    {
        // Arrange
        var cql = "SELECT * FROM test";
        var defaultConsistencyLevel = ConsistencyLevel.Quorum;
        _configuration.DefaultConsistencyLevel = defaultConsistencyLevel;

        // Act
        await _service!.ExecuteAsync(cql, null, null);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.ConsistencyLevel == defaultConsistencyLevel)), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Cql_UsesDriverDefaultConsistencyLevel_WhenNoLevelSet()
    {
        // Arrange
        var cql = "SELECT * FROM test";
        _configuration.DefaultConsistencyLevel = null;

        // Act
        await _service!.ExecuteAsync(cql, null, null);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.ConsistencyLevel == null)), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Cql_UsesPerQuerySerialConsistencyLevel()
    {
        // Arrange
        var cql = "SELECT * FROM test";
        var perQuerySerialConsistencyLevel = ConsistencyLevel.Serial;
        _configuration.DefaultSerialConsistencyLevel = ConsistencyLevel.LocalSerial;

        // Act
        await _service!.ExecuteAsync(cql, null, perQuerySerialConsistencyLevel);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.SerialConsistencyLevel == perQuerySerialConsistencyLevel)), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Cql_UsesDefaultSerialConsistencyLevel_WhenPerQueryIsNull()
    {
        // Arrange
        var cql = "SELECT * FROM test";
        var defaultSerialConsistencyLevel = ConsistencyLevel.Serial;
        _configuration.DefaultSerialConsistencyLevel = defaultSerialConsistencyLevel;

        // Act
        await _service!.ExecuteAsync(cql, null, null);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.SerialConsistencyLevel == defaultSerialConsistencyLevel)), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Cql_UsesDriverDefaultSerialConsistencyLevel_WhenNoLevelSet()
    {
        // Arrange
        var cql = "SELECT * FROM test";
        _configuration.DefaultSerialConsistencyLevel = null;

        // Act
        await _service!.ExecuteAsync(cql, null, null);

        // Assert
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.SerialConsistencyLevel == null)), Times.Once);
    }

    // Retry Policy Tests
    [Fact]
    public async Task ExecuteAsync_RetriesOnDriverException_AndSucceeds()
    {
        // Arrange
        _configuration.RetryPolicy.Enabled = true;
        _configuration.RetryPolicy.MaxRetries = 3;
        _configuration.RetryPolicy.DelayMilliseconds = 10; // Small delay for testing

        _service = new TestCassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object, _mockSession, _mockCluster);

        var statement = new SimpleStatement("SELECT * FROM test_retry_succeeds");
        var rowSet = new RowSet();

        _mockSession.SetupSequence(s => s.ExecuteAsync(statement))
            .ThrowsAsync(new OperationTimedOutException(new System.Net.IPEndPoint(0x0,0), "Simulated timeout"))
            .ReturnsAsync(rowSet);

        // Act
        var result = await _service.ExecuteAsync(statement);

        // Assert
        Assert.Same(rowSet, result);
        _mockSession.Verify(s => s.ExecuteAsync(statement), Times.Exactly(2));
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Retry 1 due to OperationTimedOutException")),
                It.IsAny<OperationTimedOutException>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_RetriesUpToMaxRetries_AndThrows()
    {
        // Arrange
        _configuration.RetryPolicy.Enabled = true;
        _configuration.RetryPolicy.MaxRetries = 2;
        _configuration.RetryPolicy.DelayMilliseconds = 10;

        _service = new TestCassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object, _mockSession, _mockCluster);

        var statement = new SimpleStatement("SELECT * FROM test_retry_fails");

        _mockSession.Setup(s => s.ExecuteAsync(statement))
            .ThrowsAsync(new UnavailableException(new System.Net.IPEndPoint(0x0,0), "Simulated unavailable", ConsistencyLevel.Any, 0, 0));

        // Act & Assert
        await Assert.ThrowsAsync<UnavailableException>(() => _service.ExecuteAsync(statement));
        _mockSession.Verify(s => s.ExecuteAsync(statement), Times.Exactly(_configuration.RetryPolicy.MaxRetries + 1));
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Retry")),
                It.IsAny<UnavailableException>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Exactly(_configuration.RetryPolicy.MaxRetries));
    }

    [Fact]
    public async Task ExecuteAsync_NoRetry_WhenPolicyDisabled()
    {
        // Arrange
        _configuration.RetryPolicy.Enabled = false;
        _service = new TestCassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object, _mockSession, _mockCluster);

        var statement = new SimpleStatement("SELECT * FROM test_no_retry_disabled");
        _mockSession.Setup(s => s.ExecuteAsync(statement))
            .ThrowsAsync(new OperationTimedOutException(new System.Net.IPEndPoint(0x0,0), "Simulated timeout"));

        // Act & Assert
        await Assert.ThrowsAsync<OperationTimedOutException>(() => _service.ExecuteAsync(statement));
        _mockSession.Verify(s => s.ExecuteAsync(statement), Times.Once); // Only one attempt
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Retry")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Never); // No logging of retries
    }

    [Fact]
    public async Task ExecuteAsync_NoRetry_OnNonRetryableException()
    {
        // Arrange
        _configuration.RetryPolicy.Enabled = true;
        _configuration.RetryPolicy.MaxRetries = 3;
        _service = new TestCassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object, _mockSession, _mockCluster);

        var statement = new SimpleStatement("SELECT * FROM test_non_retryable_exception");
        // AuthenticationException is not in the list of handled exceptions by the policy
        _mockSession.Setup(s => s.ExecuteAsync(statement))
            .ThrowsAsync(new AuthenticationException("Simulated auth error"));

        // Act & Assert
        await Assert.ThrowsAsync<AuthenticationException>(() => _service.ExecuteAsync(statement));
        _mockSession.Verify(s => s.ExecuteAsync(statement), Times.Once); // Should not retry
    }

    [Fact]
    public async Task ExecuteAsync_Cql_RetriesOnDriverException_AndSucceeds()
    {
        // Arrange
        _configuration.RetryPolicy.Enabled = true;
        _configuration.RetryPolicy.MaxRetries = 3;
        _configuration.RetryPolicy.DelayMilliseconds = 10;

        _service = new TestCassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object, _mockLoggerFactory.Object, _mockSession, _mockCluster);

        var cql = "SELECT * FROM test_cql_retry_succeeds";
        var rowSet = new RowSet();

        _mockSession.SetupSequence(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt is SimpleStatement && ((SimpleStatement)stmt).QueryString == cql)))
            .ThrowsAsync(new WriteTimeoutException(new System.Net.IPEndPoint(0x0,0), "Simulated timeout", ConsistencyLevel.Any, 0, 0, WriteType.Simple))
            .ReturnsAsync(rowSet);

        // Act
        var result = await _service.ExecuteAsync(cql);

        // Assert
        Assert.Same(rowSet, result);
        _mockSession.Verify(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt is SimpleStatement && ((SimpleStatement)stmt).QueryString == cql)), Times.Exactly(2));
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Retry 1 due to WriteTimeoutException")),
                It.IsAny<WriteTimeoutException>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }
}