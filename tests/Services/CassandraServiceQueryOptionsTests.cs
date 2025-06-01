using System;
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
    public class CassandraServiceQueryOptionsTests : IDisposable
    {
        private readonly Mock<IOptions<CassandraConfiguration>> _mockOptions;
        private readonly Mock<ILogger<CassandraService>> _mockLogger;
        private readonly CassandraConfiguration _configuration;
        private readonly Mock<TableMappingResolver> _mockMappingResolver;

        public CassandraServiceQueryOptionsTests()
        {
            _mockOptions = new Mock<IOptions<CassandraConfiguration>>();
            _mockLogger = new Mock<ILogger<CassandraService>>();
            _configuration = new CassandraConfiguration();
            _mockOptions.Setup(x => x.Value).Returns(_configuration);
            _mockMappingResolver = new Mock<TableMappingResolver>();
        }

        public void Dispose() { }

        private class TestableCassandraServiceQueryOptions : CassandraService
        {
            public readonly Mock<Builder> MockBuilderInstance = new Mock<Builder>();
            public readonly Mock<QueryOptions> MockQueryOptionsInstance = new Mock<QueryOptions>();
            public bool ConfigureQueryOptionsCalled { get; private set; } = false;

            public TestableCassandraServiceQueryOptions(
                IOptions<CassandraConfiguration> configuration,
                ILogger<CassandraService> logger,
                TableMappingResolver mappingResolver)
                : base(configuration, logger, mappingResolver)
            {
            }

            protected override Builder CreateClusterBuilder()
            {
                MockBuilderInstance.Setup(b => b.WithQueryOptions(It.IsAny<QueryOptions>()))
                                   .Returns(MockBuilderInstance.Object); // Return builder for chaining
                return MockBuilderInstance.Object;
            }

            // Override to use the mocked QueryOptions and track invocation
            protected override Builder ConfigureQueryOptions(Builder builder, QueryOptionsConfiguration queryConfig)
            {
                ConfigureQueryOptionsCalled = true;
                // Apply config to the *mocked* QueryOptions instance
                if (queryConfig.DefaultPageSize.HasValue)
                    MockQueryOptionsInstance.Object.SetPageSize(queryConfig.DefaultPageSize.Value);
                if (queryConfig.PrepareStatementsOnAllHosts.HasValue)
                    MockQueryOptionsInstance.Object.SetPrepareOnAllHosts(queryConfig.PrepareStatementsOnAllHosts.Value);
                if (queryConfig.ReprepareStatementsOnUp.HasValue)
                    MockQueryOptionsInstance.Object.SetReprepareOnUp(queryConfig.ReprepareStatementsOnUp.Value);
                if (queryConfig.DefaultMetadataSyncEnabled.HasValue)
                    MockQueryOptionsInstance.Object.SetMetadataSyncEnabled(queryConfig.DefaultMetadataSyncEnabled.Value);

                return builder.WithQueryOptions(MockQueryOptionsInstance.Object);
            }
        }

        [Fact]
        public void Connect_AppliesQueryOptions_WhenConfigured()
        {
            // Arrange
            _configuration.QueryOptions = new QueryOptionsConfiguration
            {
                DefaultPageSize = 500,
                PrepareStatementsOnAllHosts = true,
                ReprepareStatementsOnUp = false,
                DefaultMetadataSyncEnabled = true
            };

            var mockCluster = new Mock<ICluster>();
            var mockSession = new Mock<ISession>();
            mockCluster.Setup(c => c.Connect(It.IsAny<string>())).Returns(mockSession.Object);
            mockCluster.Setup(c => c.Connect()).Returns(mockSession.Object);

            var service = new TestableCassandraServiceQueryOptions(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object);
            service.MockBuilderInstance.Setup(b => b.Build()).Returns(mockCluster.Object);

            // Act
            service.StartAsync(System.Threading.CancellationToken.None).Wait();

            // Assert
            Assert.True(service.ConfigureQueryOptionsCalled);
            service.MockBuilderInstance.Verify(b => b.WithQueryOptions(service.MockQueryOptionsInstance.Object), Times.Once);

            service.MockQueryOptionsInstance.Verify(qo => qo.SetPageSize(500), Times.Once);
            service.MockQueryOptionsInstance.Verify(qo => qo.SetPrepareOnAllHosts(true), Times.Once);
            service.MockQueryOptionsInstance.Verify(qo => qo.SetReprepareOnUp(false), Times.Once);
            service.MockQueryOptionsInstance.Verify(qo => qo.SetMetadataSyncEnabled(true), Times.Once);
        }

        [Fact]
        public void Connect_DoesNotApplyQueryOptions_WhenNotConfigured()
        {
            // Arrange
            _configuration.QueryOptions = null; // No query options configuration

            var mockCluster = new Mock<ICluster>();
            var mockSession = new Mock<ISession>();
            mockCluster.Setup(c => c.Connect(It.IsAny<string>())).Returns(mockSession.Object);
            mockCluster.Setup(c => c.Connect()).Returns(mockSession.Object);

            var service = new TestableCassandraServiceQueryOptions(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object);
            service.MockBuilderInstance.Setup(b => b.Build()).Returns(mockCluster.Object);

            // Act
            service.StartAsync(System.Threading.CancellationToken.None).Wait();

            // Assert
            Assert.False(service.ConfigureQueryOptionsCalled); // Method should not be called
            service.MockBuilderInstance.Verify(b => b.WithQueryOptions(It.IsAny<QueryOptions>()), Times.Never);
        }
    }
}
