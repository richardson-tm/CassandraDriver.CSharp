using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra; // For RowSet
using CassandraDriver.Initializers;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace CassandraDriver.Tests.Initializers
{
    public class CassandraStartupInitializerTests : IDisposable
    {
        private readonly Mock<CassandraService> _mockCassandraService;
        private readonly Mock<ILogger<CassandraStartupInitializer>> _mockLogger;
        private CassandraStartupInitializerOptions _options;

        public CassandraStartupInitializerTests()
        {
            // Mock<CassandraService> needs all its constructor args mocked if we call methods on it.
            // For the initializer, it mainly calls ExecuteAsync(string, ...).
            var mockServiceOptions = new Mock<IOptions<CassandraDriver.Configuration.CassandraConfiguration>>();
            mockServiceOptions.Setup(o => o.Value).Returns(new CassandraDriver.Configuration.CassandraConfiguration());
            var mockServiceLogger = new Mock<ILogger<CassandraService>>();
            var mockMappingResolver = new Mock<CassandraDriver.Mapping.TableMappingResolver>();
            var mockLoggerFactory = new Mock<ILoggerFactory>();
            mockLoggerFactory.Setup(f => f.CreateLogger(It.IsAny<string>())).Returns(new Mock<ILogger>().Object);

            _mockCassandraService = new Mock<CassandraService>(
                mockServiceOptions.Object,
                mockServiceLogger.Object,
                mockMappingResolver.Object,
                mockLoggerFactory.Object);

            _mockLogger = new Mock<ILogger<CassandraStartupInitializer>>();
            _options = new CassandraStartupInitializerOptions(); // mutable for tests
        }

        public void Dispose() { }

        private CassandraStartupInitializer CreateInitializer()
        {
            var optionsWrapper = new OptionsWrapper<CassandraStartupInitializerOptions>(_options);
            return new CassandraStartupInitializer(_mockCassandraService.Object, optionsWrapper, _mockLogger.Object);
        }

        [Fact]
        public async Task StartAsync_NotEnabled_DoesNothingAndLogs()
        {
            // Arrange
            _options.Enabled = false;
            var initializer = CreateInitializer();

            // Act
            await initializer.StartAsync(CancellationToken.None);

            // Assert
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("CassandraStartupInitializer is disabled.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
            _mockCassandraService.Verify(s => s.ExecuteAsync(It.IsAny<string>(), null, null, It.IsAny<object[]>()), Times.Never);
        }

        [Fact]
        public async Task StartAsync_Enabled_NoValidationQuery_SucceedsAndLogs()
        {
            // Arrange
            _options.ValidationQuery = string.Empty;
            var initializer = CreateInitializer();

            // Act
            await initializer.StartAsync(CancellationToken.None);

            // Assert
             _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("No validation query configured.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
            _mockCassandraService.Verify(s => s.ExecuteAsync(It.IsAny<string>(), null, null, It.IsAny<object[]>()), Times.Never);
        }

        [Fact]
        public async Task StartAsync_ValidationQuerySucceeds_LogsAndCompletes()
        {
            // Arrange
            _mockCassandraService.Setup(s => s.ExecuteAsync(_options.ValidationQuery, null, null, It.IsAny<CancellationToken>(), It.IsAny<object[]>()))
                .ReturnsAsync(new Mock<RowSet>().Object);
            var initializer = CreateInitializer();

            // Act
            await initializer.StartAsync(CancellationToken.None);

            // Assert
            _mockCassandraService.Verify(s => s.ExecuteAsync(_options.ValidationQuery, null, null, It.IsAny<CancellationToken>(), It.IsAny<object[]>()), Times.Once);
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Cassandra startup validation query executed successfully.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_ValidationQueryFails_ThrowOnFailureTrue_ThrowsException()
        {
            // Arrange
            _options.ThrowOnFailure = true;
            var expectedException = new Exception("Cassandra error");
            _mockCassandraService.Setup(s => s.ExecuteAsync(_options.ValidationQuery, null, null, It.IsAny<CancellationToken>(), It.IsAny<object[]>()))
                .ThrowsAsync(expectedException);
            var initializer = CreateInitializer();

            // Act & Assert
            var actualException = await Assert.ThrowsAsync<CassandraStartupValidationException>(() => initializer.StartAsync(CancellationToken.None));
            Assert.Contains("Cassandra startup validation query failed", actualException.Message);
            Assert.Same(expectedException, actualException.InnerException);
             _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Cassandra startup validation query failed")),
                    expectedException, // Verify exception is logged
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_ValidationQueryFails_ThrowOnFailureFalse_LogsErrorAndCompletes()
        {
            // Arrange
            _options.ThrowOnFailure = false;
            var expectedException = new Exception("Cassandra error");
            _mockCassandraService.Setup(s => s.ExecuteAsync(_options.ValidationQuery, null, null, It.IsAny<CancellationToken>(), It.IsAny<object[]>()))
                .ThrowsAsync(expectedException);
            var initializer = CreateInitializer();

            // Act
            await initializer.StartAsync(CancellationToken.None); // Should not throw

            // Assert
            _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Cassandra startup validation query failed")),
                    expectedException,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_TimeoutOccurs_ThrowOnFailureTrue_ThrowsOperationCanceledException()
        {
            // Arrange
            _options.Timeout = TimeSpan.FromMilliseconds(50); // Short timeout
            _options.ThrowOnFailure = true;
            _mockCassandraService.Setup(s => s.ExecuteAsync(_options.ValidationQuery, null, null, It.IsAny<CancellationToken>(), It.IsAny<object[]>()))
                .Returns(async (string cql, ConsistencyLevel? cl, ConsistencyLevel? scl, CancellationToken ct, object[]? v) => {
                    await Task.Delay(100, ct); // Delay longer than timeout, respecting the passed token
                    return new Mock<RowSet>().Object;
                });
            var initializer = CreateInitializer();

            // Act & Assert
            var actualException = await Assert.ThrowsAsync<CassandraStartupValidationException>(() => initializer.StartAsync(CancellationToken.None));
            Assert.Contains("timed out", actualException.Message);
            Assert.IsAssignableFrom<OperationCanceledException>(actualException.InnerException);
             _mockLogger.Verify(
                x => x.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("timed out")),
                    It.IsAny<OperationCanceledException>(), // Verify exception is logged
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_ExternalCancellation_DoesNotThrowValidationException_IfThrowOnFailureTrue()
        {
            // Arrange
            _options.ThrowOnFailure = true;
            var cts = new CancellationTokenSource();

            _mockCassandraService.Setup(s => s.ExecuteAsync(_options.ValidationQuery, null, null, It.IsAny<CancellationToken>(), It.IsAny<object[]>()))
                .Returns(async (string cql, ConsistencyLevel? cl, ConsistencyLevel? scl, CancellationToken ct, object[]? v) => {
                    await Task.Delay(1000, ct); // Delay to allow cancellation
                    ct.ThrowIfCancellationRequested(); // This will throw OperationCanceledException
                    return new Mock<RowSet>().Object;
                });

            var initializer = CreateInitializer();
            cts.CancelAfter(50); // Cancel externally before task completes

            // Act & Assert
            // Expect OperationCanceledException directly, not wrapped in CassandraStartupValidationException
            await Assert.ThrowsAsync<OperationCanceledException>(() => initializer.StartAsync(cts.Token));
        }
    }
}
