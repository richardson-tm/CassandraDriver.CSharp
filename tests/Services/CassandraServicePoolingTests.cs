using System;
using Cassandra;
using CassandraDriver.Configuration;
using CassandraDriver.Mapping; // Assuming TableMappingResolver is still needed by CassandraService constructor
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace CassandraDriver.Tests.Services
{
    public class CassandraServicePoolingTests : IDisposable
    {
        private readonly Mock<IOptions<CassandraConfiguration>> _mockOptions;
        private readonly Mock<ILogger<CassandraService>> _mockLogger;
        private readonly CassandraConfiguration _configuration;
        private readonly Mock<TableMappingResolver> _mockMappingResolver; // CassandraService expects this

        public CassandraServicePoolingTests()
        {
            _mockOptions = new Mock<IOptions<CassandraConfiguration>>();
            _mockLogger = new Mock<ILogger<CassandraService>>();
            _configuration = new CassandraConfiguration();
            _mockOptions.Setup(x => x.Value).Returns(_configuration);
            _mockMappingResolver = new Mock<TableMappingResolver>();
        }

        public void Dispose() { }

        // Test class to expose the Builder for verification
        private class TestableCassandraService : CassandraService
        {
            public readonly Mock<Builder> MockBuilderInstance = new Mock<Builder>();

            // We need to mock PoolingOptions as well to verify calls to its methods
            public readonly Mock<PoolingOptions> MockPoolingOptionsInstance = new Mock<PoolingOptions>();

            public TestableCassandraService(
                IOptions<CassandraConfiguration> configuration,
                ILogger<CassandraService> logger,
                TableMappingResolver mappingResolver)
                : base(configuration, logger, mappingResolver)
            {
            }

            protected override Builder CreateClusterBuilder()
            {
                // Return the mocked Builder instance that CassandraService will use.
                // We also need to ensure that this mocked Builder, when WithPoolingOptions is called,
                // uses a PoolingOptions object that we can also mock/verify.
                // This is getting complex due to the internal newing up of PoolingOptions.

                // One way: MockBuilderInstance setup to return itself on fluent calls
                // and setup WithPoolingOptions to capture the passed PoolingOptions.
                // However, we can't easily verify calls on the *captured* PoolingOptions.

                // Let's try to override the method that applies pooling options.
                return MockBuilderInstance.Object;
            }

            // This method is new and intended to be overridden for testing.
            // In real CassandraService, this logic is inside Connect().
            // This refactoring makes testing pooling options easier.
            protected override Builder ConfigurePoolingOptions(Builder builder, PoolingOptionsConfiguration poolingConfig)
            {
                var poolingOptions = MockPoolingOptionsInstance.Object; // Use the mocked PoolingOptions

                // Call the actual configuration logic from base class or duplicate here for verification
                // This is not ideal. The original method should be structured to allow this.
                // For now, let's assume we can verify calls on MockPoolingOptionsInstance.

                if (poolingConfig.CoreConnectionsPerHostLocal.HasValue)
                    poolingOptions.SetCoreConnectionsPerHost(HostDistance.Local, poolingConfig.CoreConnectionsPerHostLocal.Value);
                if (poolingConfig.MaxConnectionsPerHostLocal.HasValue)
                    poolingOptions.SetMaxConnectionsPerHost(HostDistance.Local, poolingConfig.MaxConnectionsPerHostLocal.Value);
                // ... other properties

                return builder.WithPoolingOptions(poolingOptions);
            }
        }

        [Fact]
        public void Connect_AppliesPoolingOptions_WhenConfigured()
        {
            // Arrange
            _configuration.Pooling = new PoolingOptionsConfiguration
            {
                CoreConnectionsPerHostLocal = 10,
                MaxConnectionsPerHostLocal = 20,
                HeartbeatIntervalMillis = 30000
            };

            // Mocking ICluster and ISession to allow Connect to complete
            var mockCluster = new Mock<ICluster>();
            var mockSession = new Mock<ISession>();
            mockCluster.Setup(c => c.Connect(It.IsAny<string>())).Returns(mockSession.Object);
            mockCluster.Setup(c => c.Connect()).Returns(mockSession.Object);

            var service = new TestableCassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object);

            // Setup the mocked builder to return the mocked cluster
            service.MockBuilderInstance.Setup(b => b.Build()).Returns(mockCluster.Object);

            // Setup WithPoolingOptions to verify the PoolingOptions instance passed to it.
            // This is where it gets tricky because PoolingOptions is normally newed up.
            // The TestableCassandraService tries to use a mocked PoolingOptions.

            service.MockBuilderInstance.Setup(b => b.WithPoolingOptions(It.IsAny<PoolingOptions>()))
                .Callback<PoolingOptions>(actualPoolingOptions => {
                    // This callback gives us the actual PoolingOptions instance.
                    // We can't easily verify calls on it *after the fact* if it's not a mock.
                    // However, if TestableCassandraService.ConfigurePooling correctly uses MockPoolingOptionsInstance,
                    // we can verify calls on MockPoolingOptionsInstance.
                })
                .Returns(service.MockBuilderInstance.Object); // Return builder for fluent chaining


            // Act
            // Connecting involves calling internal methods. We need to ensure StartAsync calls Connect,
            // and Connect uses the overridden CreateClusterBuilder and ConfigurePooling.
            // For this test, we might need to call a method that encapsulates the builder configuration part.
            // Or, we assume the internal Connect method will use our overridden CreateClusterBuilder.

            // To test Connect directly, we would need CassandraService.Connect to be protected virtual.
            // Let's assume StartAsync will trigger the Connect logic which uses CreateClusterBuilder.
            service.StartAsync(System.Threading.CancellationToken.None).Wait(); // Fire and forget for test setup

            // Assert
            // Verify that WithPoolingOptions was called on the builder.
            service.MockBuilderInstance.Verify(b => b.WithPoolingOptions(service.MockPoolingOptionsInstance.Object), Times.Once);

            // Verify that Set methods were called on our MockPoolingOptionsInstance
            service.MockPoolingOptionsInstance.Verify(po => po.SetCoreConnectionsPerHost(HostDistance.Local, 10), Times.Once);
            service.MockPoolingOptionsInstance.Verify(po => po.SetMaxConnectionsPerHost(HostDistance.Local, 20), Times.Once);
            service.MockPoolingOptionsInstance.Verify(po => po.SetHeartbeatInterval(30000), Times.Once);

            // Verify other Set methods were NOT called if their corresponding config was null
            service.MockPoolingOptionsInstance.Verify(po => po.SetCoreConnectionsPerHost(HostDistance.Remote, It.IsAny<int>()), Times.Never);
        }

        [Fact]
        public void Connect_DoesNotApplyPoolingOptions_WhenNotConfigured()
        {
            // Arrange
            _configuration.Pooling = null; // No pooling configuration

            var mockCluster = new Mock<ICluster>();
            var mockSession = new Mock<ISession>();
            mockCluster.Setup(c => c.Connect(It.IsAny<string>())).Returns(mockSession.Object);
            mockCluster.Setup(c => c.Connect()).Returns(mockSession.Object);

            var service = new TestableCassandraService(_mockOptions.Object, _mockLogger.Object, _mockMappingResolver.Object);
            service.MockBuilderInstance.Setup(b => b.Build()).Returns(mockCluster.Object);

            // Act
            service.StartAsync(System.Threading.CancellationToken.None).Wait();

            // Assert
            // Verify that WithPoolingOptions was NOT called on the builder.
            service.MockBuilderInstance.Verify(b => b.WithPoolingOptions(It.IsAny<PoolingOptions>()), Times.Never);
        }
    }
}
