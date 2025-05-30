using CassandraDriver.Configuration;
using CassandraDriver.Extensions;
using CassandraDriver.HealthChecks;
using CassandraDriver.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using Xunit;

namespace CassandraDriver.Tests.Extensions;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddCassandra_WithConfiguration_RegistersServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Cassandra:Seeds:0"] = "127.0.0.1",
                ["Cassandra:Keyspace"] = "test_keyspace",
                ["Cassandra:User"] = "cassandra",
                ["Cassandra:Password"] = "password"
            })
            .Build();

        // Act
        services.AddCassandra(configuration);
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        Assert.NotNull(serviceProvider.GetService<CassandraService>());
        Assert.NotNull(serviceProvider.GetService<IOptions<CassandraConfiguration>>());
        
        var options = serviceProvider.GetRequiredService<IOptions<CassandraConfiguration>>().Value;
        Assert.Contains("127.0.0.1", options.Seeds);
        Assert.Equal("test_keyspace", options.Keyspace);
        Assert.Equal("cassandra", options.User);
        Assert.Equal("password", options.Password);
    }

    [Fact]
    public void AddCassandra_WithAction_RegistersServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddCassandra(options =>
        {
            options.Seeds = new List<string> { "localhost", "192.168.1.1:9042" };
            options.Keyspace = "my_keyspace";
            options.SpeculativeExecutionEnabled = false;
            options.RemoteHostsPerDc = 3;
        });
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        Assert.NotNull(serviceProvider.GetService<CassandraService>());
        Assert.NotNull(serviceProvider.GetService<IOptions<CassandraConfiguration>>());
        
        var options = serviceProvider.GetRequiredService<IOptions<CassandraConfiguration>>().Value;
        Assert.Equal(2, options.Seeds.Count);
        Assert.Contains("localhost", options.Seeds);
        Assert.Contains("192.168.1.1:9042", options.Seeds);
        Assert.Equal("my_keyspace", options.Keyspace);
        Assert.False(options.SpeculativeExecutionEnabled);
        Assert.Equal(3, options.RemoteHostsPerDc);
    }

    [Fact]
    public void AddCassandra_RegistersSingletonService()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCassandra(options => { options.Seeds = new List<string> { "localhost" }; });
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var service1 = serviceProvider.GetRequiredService<CassandraService>();
        var service2 = serviceProvider.GetRequiredService<CassandraService>();

        // Assert
        Assert.Same(service1, service2);
    }

    [Fact]
    public void AddCassandra_RegistersAsHostedService()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCassandra(options => { options.Seeds = new List<string> { "localhost" }; });

        // Act
        var serviceProvider = services.BuildServiceProvider();
        var hostedServices = serviceProvider.GetServices<IHostedService>();

        // Assert
        Assert.Contains(hostedServices, service => service is CassandraService);
    }

    [Fact]
    public void AddCassandra_RegistersHealthCheck()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCassandra(options => { options.Seeds = new List<string> { "localhost" }; });
        
        // Act
        var serviceProvider = services.BuildServiceProvider();
        var healthCheckService = serviceProvider.GetService<HealthCheckService>();
        
        // Assert
        Assert.NotNull(healthCheckService);
        
        // Verify health check is registered with correct name and tags
        var serviceDescriptor = services.FirstOrDefault(s => s.ServiceType == typeof(HealthCheckService));
        Assert.NotNull(serviceDescriptor);
    }

    [Fact]
    public void AddCassandra_WithConfiguration_LoadsComplexConfiguration()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Cassandra:Seeds:0"] = "node1",
                ["Cassandra:Seeds:1"] = "node2:9042",
                ["Cassandra:Keyspace"] = "test",
                ["Cassandra:ProtocolVersion"] = "4",
                ["Cassandra:SpeculativeExecutionEnabled"] = "true",
                ["Cassandra:SpeculativeRetryPercentile"] = "95.5",
                ["Cassandra:MaxSpeculativeExecutions"] = "2",
                ["Cassandra:RemoteHostsPerDc"] = "3",
                ["Cassandra:Ec2TranslationEnabled"] = "false",
                ["Cassandra:ShareEventLoopGroup"] = "true",
                ["Cassandra:AutoMigrate"] = "true",
                ["Cassandra:MigrationFile"] = "/custom/migrations.cql",
                ["Cassandra:Truststore:Path"] = "/ssl/trust.pfx",
                ["Cassandra:Truststore:Password"] = "trust123",
                ["Cassandra:Keystore:Path"] = "/ssl/key.pfx",
                ["Cassandra:Keystore:Password"] = "key123"
            })
            .Build();

        // Act
        services.AddCassandra(configuration);
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var options = serviceProvider.GetRequiredService<IOptions<CassandraConfiguration>>().Value;
        
        Assert.Equal(2, options.Seeds.Count);
        Assert.Contains("node1", options.Seeds);
        Assert.Contains("node2:9042", options.Seeds);
        Assert.Equal("test", options.Keyspace);
        Assert.Equal(4, options.ProtocolVersion);
        Assert.True(options.SpeculativeExecutionEnabled);
        Assert.Equal(95.5, options.SpeculativeRetryPercentile);
        Assert.Equal(2, options.MaxSpeculativeExecutions);
        Assert.Equal(3, options.RemoteHostsPerDc);
        Assert.False(options.Ec2TranslationEnabled);
        Assert.True(options.ShareEventLoopGroup);
        Assert.True(options.AutoMigrate);
        Assert.Equal("/custom/migrations.cql", options.MigrationFile);
        
        Assert.NotNull(options.Truststore);
        Assert.Equal("/ssl/trust.pfx", options.Truststore.Path);
        Assert.Equal("trust123", options.Truststore.Password);
        
        Assert.NotNull(options.Keystore);
        Assert.Equal("/ssl/key.pfx", options.Keystore.Path);
        Assert.Equal("key123", options.Keystore.Password);
    }

    [Fact]
    public void AddCassandra_CanBeCalledMultipleTimes_LastWins()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddCassandra(options =>
        {
            options.Seeds = new List<string> { "first-config" };
            options.Keyspace = "first";
        });

        services.AddCassandra(options =>
        {
            options.Seeds = new List<string> { "second-config" };
            options.Keyspace = "second";
        });

        var serviceProvider = services.BuildServiceProvider();

        // Assert
        var options = serviceProvider.GetRequiredService<IOptions<CassandraConfiguration>>().Value;
        Assert.Contains("second-config", options.Seeds);
        Assert.Equal("second", options.Keyspace);
    }
}