using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Exceptions; // For specific exceptions
using CassandraDriver.Configuration;
using CassandraDriver.Mapping;
using CassandraDriver.Services;
using CassandraDriver.Telemetry; // For DriverMetrics
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace CassandraDriver.Tests.Telemetry
{
    public class MetricsTests : IDisposable
    {
        private readonly Mock<IOptions<CassandraConfiguration>> _mockOptions;
        private readonly Mock<ILogger<CassandraService>> _mockLogger;
        private readonly CassandraConfiguration _configuration;
        private readonly Mock<TableMappingResolver> _mockMappingResolver;
        private readonly Mock<ILoggerFactory> _mockLoggerFactory;

        private CassandraService _service;
        private Mock<ISession> _mockSession;

        private MeterListener _meterListener;
        private List<MeasurementResult> _measurements;

        private record MeasurementResult(string InstrumentName, double? Value, Dictionary<string, object?> Tags);

        public MetricsTests()
        {
            _mockOptions = new Mock<IOptions<CassandraConfiguration>>();
            _mockLogger = new Mock<ILogger<CassandraService>>();
            _configuration = new CassandraConfiguration(); // Default config
            _mockOptions.Setup(x => x.Value).Returns(_configuration);
            _mockMappingResolver = new Mock<TableMappingResolver>();
            _mockLoggerFactory = new Mock<ILoggerFactory>();
            _mockLoggerFactory.Setup(x => x.CreateLogger(It.IsAny<string>())).Returns(new Mock<ILogger>().Object);

            _mockSession = new Mock<ISession>();

            // Create service with real logic but mocked ISession
            _service = new CassandraServiceWithMockedSession(
                _mockOptions.Object,
                _mockLogger.Object,
                _mockMappingResolver.Object,
                _mockLoggerFactory.Object,
                _mockSession.Object);

            // Setup MeterListener
            _measurements = new List<MeasurementResult>();
            _meterListener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter.Name == DriverMetrics.MeterName)
                    {
                        listener.EnableMeasurementEvents(instrument);
                    }
                }
            };
            _meterListener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
            {
                _measurements.Add(new MeasurementResult(instrument.Name, measurement, tags.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)));
            });
            _meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
            {
                 _measurements.Add(new MeasurementResult(instrument.Name, measurement, tags.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)));
            });
            _meterListener.Start();
        }

        // Custom CassandraService that allows injecting a pre-mocked ISession.
        private class CassandraServiceWithMockedSession : CassandraService
        {
            private readonly ISession _mockedSessionInstance;
            public CassandraServiceWithMockedSession(
                IOptions<CassandraConfiguration> configuration,
                ILogger<CassandraService> logger,
                TableMappingResolver mappingResolver,
                ILoggerFactory loggerFactory,
                ISession mockedSession)
                : base(configuration, logger, mappingResolver, loggerFactory)
            {
                _mockedSessionInstance = mockedSession;
            }

            // Override Connect to prevent real connection attempts and ensure our mock session is used.
            protected override void Connect()
            {
                // base.Connect() would create a real cluster and session.
                // We need to assign our mocked session to the internal _session field.
                // This is tricky as _session is private. For testing, one might make it protected
                // or provide a test hook.
                // For now, we assume that the base.Session property will be overridden or handled.
                // However, the base class's Session getter throws if _session is null.
                // We need to ensure the internal _session field is set to our mock.
                // This is a common challenge when testing classes that manage their own internal state heavily.
                // A simple way for testing: make _session protected in CassandraService.
                // For now, let's assume it gets set or we can test via ExecuteAsync which uses the Session property.
                // The test below will set up the Session property directly if needed.
            }

            // Ensure our mock session is returned
            public override ISession Session => _mockedSessionInstance;
        }


        public void Dispose()
        {
            _meterListener.Dispose();
        }

        [Fact]
        public async Task ExecuteAsync_IStatement_RecordsMetrics_OnSuccess()
        {
            // Arrange
            var statement = new SimpleStatement("SELECT * FROM system.local");
            _mockSession.Setup(s => s.ExecuteAsync(statement)).ReturnsAsync(new RowSet());

            // Act
            await _service.ExecuteAsync(statement);

            // Assert
            AssertMeasurement("cassandra.driver.queries.started", 1);
            AssertMeasurement("cassandra.driver.queries.succeeded", 1, DriverMetrics.TagKeys.CqlOperation, "SELECT");
            AssertMeasurement("cassandra.driver.query.duration", valuePredicate: v => v >= 0, DriverMetrics.TagKeys.CqlOperation, "SELECT");
            Assert.DoesNotContain(_measurements, m => m.InstrumentName == "cassandra.driver.queries.failed");
        }

        [Fact]
        public async Task ExecuteAsync_StringCql_RecordsMetrics_OnSuccess()
        {
            // Arrange
            var cql = "INSERT INTO users (id, name) VALUES (1, 'test')";
            var mockPs = new Mock<PreparedStatement>();
            var mockBs = new Mock<BoundStatement>();
            _mockSession.Setup(s => s.PrepareAsync(cql)).ReturnsAsync(mockPs.Object);
            mockPs.Setup(ps => ps.Bind(It.IsAny<object[]>())).Returns(mockBs.Object);
            _mockSession.Setup(s => s.ExecuteAsync(mockBs.Object)).ReturnsAsync(new RowSet());

            // Act
            await _service.ExecuteAsync(cql, 1, "test");

            // Assert
            AssertMeasurement("cassandra.driver.queries.started", 1);
            AssertMeasurement("cassandra.driver.queries.succeeded", 1, DriverMetrics.TagKeys.CqlOperation, "INSERT");
            AssertMeasurement("cassandra.driver.query.duration", valuePredicate: v => v >= 0, DriverMetrics.TagKeys.CqlOperation, "INSERT");
        }

        [Fact]
        public async Task ExecuteAsync_IStatement_RecordsMetrics_OnFailure()
        {
            // Arrange
            var statement = new SimpleStatement("SELECT * FROM bad_table");
            var exception = new InvalidQueryException("Simulated query error");
            _mockSession.Setup(s => s.ExecuteAsync(statement)).ThrowsAsync(exception);

            // Act
            await Assert.ThrowsAsync<InvalidQueryException>(() => _service.ExecuteAsync(statement));

            // Assert
            AssertMeasurement("cassandra.driver.queries.started", 1);
            AssertMeasurement("cassandra.driver.queries.failed", 1,
                new KeyValuePair<string, object?>(DriverMetrics.TagKeys.CqlOperation, "SELECT"),
                new KeyValuePair<string, object?>(DriverMetrics.TagKeys.ExceptionType, nameof(InvalidQueryException))
            );
            AssertMeasurement("cassandra.driver.query.duration", valuePredicate: v => v >= 0, DriverMetrics.TagKeys.CqlOperation, "SELECT");
            Assert.DoesNotContain(_measurements, m => m.InstrumentName == "cassandra.driver.queries.succeeded");
        }

        [Fact]
        public async Task ExecuteAsync_StringCql_RecordsMetrics_OnFailure_PrepareAsyncFails()
        {
            // Arrange
            var cql = "SELECT * FROM very_bad_table";
            var exception = new SyntaxError("Simulated prepare error");
            _mockSession.Setup(s => s.PrepareAsync(cql)).ThrowsAsync(exception);

            // Act
            await Assert.ThrowsAsync<SyntaxError>(() => _service.ExecuteAsync(cql));

            // Assert
            // PrepareAsync is outside the try/catch for succeeded/failed counters in ExecuteAsync(cql,...)
            // So, QueriesStarted, Succeeded, Failed won't be hit for prepare failures.
            // Only duration will be recorded for the attempt.
            // This highlights a nuance: should PrepareAsync also be instrumented or part of the query's metrics?
            // Based on current ExecuteAsync(cql,...) structure, it's like this:
            AssertMeasurement("cassandra.driver.queries.started", 1); // Started is before prepare
            AssertMeasurement("cassandra.driver.queries.failed", 1,
                new KeyValuePair<string, object?>(DriverMetrics.TagKeys.CqlOperation, "SELECT"),
                new KeyValuePair<string, object?>(DriverMetrics.TagKeys.ExceptionType, nameof(SyntaxError))
            );
             AssertMeasurement("cassandra.driver.query.duration", valuePredicate: v => v >= 0, DriverMetrics.TagKeys.CqlOperation, "SELECT");
            Assert.DoesNotContain(_measurements, m => m.InstrumentName == "cassandra.driver.queries.succeeded");
        }


        private void AssertMeasurement(string name, double? expectedValue = null, string? tagName = null, object? tagValue = null, Func<double?, bool>? valuePredicate = null)
        {
            var measurement = _measurements.FirstOrDefault(m => m.InstrumentName == name);
            Assert.NotNull(measurement);

            if (expectedValue.HasValue) Assert.Equal(expectedValue.Value, Convert.ToDouble(measurement.Value));
            if (valuePredicate != null) Assert.True(valuePredicate(Convert.ToDouble(measurement.Value)));

            if (tagName != null)
            {
                Assert.True(measurement.Tags.ContainsKey(tagName));
                Assert.Equal(tagValue?.ToString(), measurement.Tags[tagName]?.ToString());
            }
        }

        private void AssertMeasurement(string name, double? expectedValue = null, params KeyValuePair<string, object?>[] tags)
        {
            var measurement = _measurements.FirstOrDefault(m => m.InstrumentName == name);
            Assert.NotNull(measurement);

            if (expectedValue.HasValue) Assert.Equal(expectedValue.Value, Convert.ToDouble(measurement.Value));

            foreach(var tag in tags)
            {
                Assert.True(measurement.Tags.ContainsKey(tag.Key));
                Assert.Equal(tag.Value?.ToString(), measurement.Tags[tag.Key]?.ToString());
            }
        }
    }
}
