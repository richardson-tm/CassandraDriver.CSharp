using Cassandra;
using CassandraDriver.HealthChecks;
using CassandraDriver.Services;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using System.Net;

namespace CassandraDriver.Tests.HealthChecks;

public class CassandraHealthCheckTests
{
    [Fact]
    public async Task CheckHealthAsync_ReturnsUnhealthy_WhenNoHostsConnected()
    {
        // Arrange
        var mockLogger = new Mock<ILogger<CassandraHealthCheck>>();
        var mockCassandraService = new Mock<CassandraService>(
            Options.Create(new CassandraDriver.Configuration.CassandraConfiguration { Seeds = new List<string> { "localhost" } }),
            new Mock<ILogger<CassandraService>>().Object);
        
        var mockCluster = new Mock<ICluster>();
        mockCluster.Setup(c => c.AllHosts()).Returns(new List<Host>());
        
        mockCassandraService.Setup(s => s.Cluster).Returns(mockCluster.Object);
        
        var healthCheck = new CassandraHealthCheck(mockCassandraService.Object, mockLogger.Object);
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("cassandra", healthCheck, null, null)
        };

        // Act
        var result = await healthCheck.CheckHealthAsync(context);

        // Assert
        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Equal("No connected hosts found", result.Description);
    }

    [Fact]
    public async Task CheckHealthAsync_HandlesExceptions()
    {
        // Arrange
        var mockLogger = new Mock<ILogger<CassandraHealthCheck>>();
        var mockCassandraService = new Mock<CassandraService>(
            Options.Create(new CassandraDriver.Configuration.CassandraConfiguration { Seeds = new List<string> { "localhost" } }),
            new Mock<ILogger<CassandraService>>().Object);
        
        // Setup Cluster to throw an exception when accessed
        var exception = new InvalidOperationException("Cluster not initialized");
        mockCassandraService.Setup(s => s.Cluster).Throws(exception);
        
        var healthCheck = new CassandraHealthCheck(mockCassandraService.Object, mockLogger.Object);
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("cassandra", healthCheck, null, null)
        };

        // Act
        var result = await healthCheck.CheckHealthAsync(context);

        // Assert
        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Equal("Cassandra connection is unhealthy", result.Description);
        Assert.Equal(exception, result.Exception);
        Assert.Contains("error", result.Data);
        Assert.Contains("exception_type", result.Data);
    }

    [Fact]
    public void Constructor_InitializesWithDependencies()
    {
        // Arrange
        var mockLogger = new Mock<ILogger<CassandraHealthCheck>>();
        var mockCassandraService = new Mock<CassandraService>(
            Options.Create(new CassandraDriver.Configuration.CassandraConfiguration { Seeds = new List<string> { "localhost" } }),
            new Mock<ILogger<CassandraService>>().Object);

        // Act
        var healthCheck = new CassandraHealthCheck(mockCassandraService.Object, mockLogger.Object);

        // Assert
        Assert.NotNull(healthCheck);
    }
}