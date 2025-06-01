using System;
using System.Threading.Tasks;
using Cassandra;
using CassandraDriver.Configuration;
using CassandraDriver.Mapping;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace CassandraDriver.Tests.Services
{
    public class CassandraServiceStatementCachingTests : IDisposable
    {
        private readonly Mock<IOptions<CassandraConfiguration>> _mockOptions;
        private readonly Mock<ILogger<CassandraService>> _mockLogger;
        private readonly CassandraConfiguration _configuration;
        private readonly Mock<TableMappingResolver> _mockMappingResolver;
        private CassandraService _service;
        private Mock<ISession> _mockSession;
        private Mock<ICluster> _mockCluster;

        // Testable CassandraService that allows us to inject a mocked ISession
        // This version doesn't need to override builder methods, just needs access to a real CassandraService
        // using a mocked ISession.
        private class CassandraServiceWithMockSession : CassandraService
        {
            private readonly ISession _sessionOverride;
            public CassandraServiceWithMockSession(
                IOptions<CassandraConfiguration> configuration,
                ILogger<CassandraService> logger,
                TableMappingResolver mappingResolver,
                ISession sessionOverride)
                : base(configuration, logger, mappingResolver)
            {
                _sessionOverride = sessionOverride;
            }

            // Override the Session property to return the mock session
            // This requires the Session property in the base CassandraService to be virtual.
            // And indeed it is: public virtual ISession Session { get; }
             public override ISession Session => _sessionOverride ?? base.Session;

            // Allow calling Connect for setup, but it won't fully connect to a real DB
            public void CallConnectForTest() => base.Connect();
        }


        public CassandraServiceStatementCachingTests()
        {
            _mockOptions = new Mock<IOptions<CassandraConfiguration>>();
            _mockLogger = new Mock<ILogger<CassandraService>>();
            _configuration = new CassandraConfiguration();
            _mockOptions.Setup(x => x.Value).Returns(_configuration);
            _mockMappingResolver = new Mock<TableMappingResolver>();

            _mockSession = new Mock<ISession>();
            _mockCluster = new Mock<ICluster>();

            // Setup mock cluster to return mock session
            // _mockCluster.Setup(c => c.Connect(It.IsAny<string>())).Returns(_mockSession.Object);
            // _mockCluster.Setup(c => c.Connect()).Returns(_mockSession.Object);


            // Instantiate the service with the mocked session.
            // The CassandraService constructor will need the IOptions, ILogger, and TableMappingResolver.
            // The key is that when CassandraService.Connect() eventually tries to use _cluster.Connect(),
            // it should get our _mockSession.
            // For these tests, we don't want StartAsync to run the real Connect().
            // We will use a CassandraService instance where the _session field is directly our _mockSession.
            // The CassandraServiceWithMockSession helper class above is intended for this.

            _service = new CassandraServiceWithMockSession(
                _mockOptions.Object,
                _mockLogger.Object,
                _mockMappingResolver.Object,
                _mockSession.Object // Directly inject the mock session
            );

            // The StartAsync calls Connect which sets up the real _session.
            // For these tests, we want _service to use _mockSession directly.
            // The CassandraServiceWithMockSession will ensure that its Session property returns _mockSession.
        }

        public void Dispose() { }

        [Fact]
        public async Task ExecuteAsyncCql_UsesCachedPreparedStatements()
        {
            // Arrange
            var cql = "SELECT * FROM test_table WHERE id = ?";
            var mockPreparedStatement = new Mock<PreparedStatement>();
            var mockBoundStatement = new Mock<BoundStatement>();

            _mockSession.Setup(s => s.PrepareAsync(cql))
                        .ReturnsAsync(mockPreparedStatement.Object)
                        .Verifiable();
            mockPreparedStatement.Setup(ps => ps.Bind(It.IsAny<object[]>()))
                                 .Returns(mockBoundStatement.Object);
            _mockSession.Setup(s => s.ExecuteAsync(mockBoundStatement.Object))
                        .ReturnsAsync(new RowSet());

            // Act
            await _service.ExecuteAsync(cql, 1);
            await _service.ExecuteAsync(cql, 2);
            await _service.ExecuteAsync(cql, 3);

            // Assert
            _mockSession.Verify(s => s.PrepareAsync(cql), Times.Once()); // PrepareAsync should only be called once
            mockPreparedStatement.Verify(ps => ps.Bind(new object[]{1}), Times.Once());
            mockPreparedStatement.Verify(ps => ps.Bind(new object[]{2}), Times.Once());
            mockPreparedStatement.Verify(ps => ps.Bind(new object[]{3}), Times.Once());
            _mockSession.Verify(s => s.ExecuteAsync(mockBoundStatement.Object), Times.Exactly(3));
        }

        [Fact]
        public async Task ExecuteAsyncCql_PreparesDifferentStatementsForDifferentCql()
        {
            // Arrange
            var cql1 = "SELECT * FROM test_table WHERE key = ?";
            var cql2 = "INSERT INTO test_table (key, value) VALUES (?, ?)";

            var mockPs1 = new Mock<PreparedStatement>();
            var mockBs1 = new Mock<BoundStatement>();
            var mockPs2 = new Mock<PreparedStatement>();
            var mockBs2 = new Mock<BoundStatement>();

            _mockSession.Setup(s => s.PrepareAsync(cql1)).ReturnsAsync(mockPs1.Object).Verifiable();
            mockPs1.Setup(ps => ps.Bind(It.IsAny<object[]>())).Returns(mockBs1.Object);
            _mockSession.Setup(s => s.ExecuteAsync(mockBs1.Object)).ReturnsAsync(new RowSet());

            _mockSession.Setup(s => s.PrepareAsync(cql2)).ReturnsAsync(mockPs2.Object).Verifiable();
            mockPs2.Setup(ps => ps.Bind(It.IsAny<object[]>())).Returns(mockBs2.Object);
            _mockSession.Setup(s => s.ExecuteAsync(mockBs2.Object)).ReturnsAsync(new RowSet());

            // Act
            await _service.ExecuteAsync(cql1, "key1");
            await _service.ExecuteAsync(cql2, "key1", "value1");
            await _service.ExecuteAsync(cql1, "key2"); // Should use cached PS1

            // Assert
            _mockSession.Verify(s => s.PrepareAsync(cql1), Times.Once());
            _mockSession.Verify(s => s.PrepareAsync(cql2), Times.Once());
            _mockSession.Verify(s => s.ExecuteAsync(mockBs1.Object), Times.Exactly(2)); // cql1 called twice
            _mockSession.Verify(s => s.ExecuteAsync(mockBs2.Object), Times.Once());  // cql2 called once
        }
    }
}
