using CassandraDriver.Configuration;
using Xunit;

namespace CassandraDriver.Tests.Configuration;

public class CassandraConfigurationTests
{
    [Fact]
    public void Constructor_SetsDefaultValues()
    {
        // Arrange & Act
        var config = new CassandraConfiguration();

        // Assert
        Assert.NotNull(config.Seeds);
        Assert.Empty(config.Seeds);
        Assert.False(config.ShareEventLoopGroup);
        Assert.True(config.Ec2TranslationEnabled);
        Assert.Equal("/migrations/cql.changelog", config.MigrationFile);
        Assert.False(config.AutoMigrate);
        Assert.True(config.SpeculativeExecutionEnabled);
        Assert.Equal(99, config.SpeculativeRetryPercentile);
        Assert.Equal(3, config.MaxSpeculativeExecutions);
        Assert.Equal(1, config.RemoteHostsPerDc);
        Assert.Null(config.ProtocolVersion);
        Assert.Null(config.User);
        Assert.Null(config.Password);
        Assert.Null(config.Keyspace);
        Assert.Null(config.Truststore);
        Assert.Null(config.Keystore);
    }

    [Fact]
    public void Properties_CanBeSetAndRetrieved()
    {
        // Arrange
        var config = new CassandraConfiguration();
        var seeds = new List<string> { "127.0.0.1", "localhost:9042" };
        var truststore = new SslConfiguration { Path = "/path/to/truststore", Password = "trust123" };
        var keystore = new SslConfiguration { Path = "/path/to/keystore", Password = "key123" };

        // Act
        config.Seeds = seeds;
        config.User = "cassandra";
        config.Password = "password123";
        config.Keyspace = "test_keyspace";
        config.ShareEventLoopGroup = true;
        config.Ec2TranslationEnabled = false;
        config.MigrationFile = "/custom/migrations.cql";
        config.AutoMigrate = true;
        config.SpeculativeExecutionEnabled = false;
        config.SpeculativeRetryPercentile = 95.5;
        config.MaxSpeculativeExecutions = 5;
        config.RemoteHostsPerDc = 3;
        config.ProtocolVersion = 4;
        config.Truststore = truststore;
        config.Keystore = keystore;

        // Assert
        Assert.Equal(seeds, config.Seeds);
        Assert.Equal("cassandra", config.User);
        Assert.Equal("password123", config.Password);
        Assert.Equal("test_keyspace", config.Keyspace);
        Assert.True(config.ShareEventLoopGroup);
        Assert.False(config.Ec2TranslationEnabled);
        Assert.Equal("/custom/migrations.cql", config.MigrationFile);
        Assert.True(config.AutoMigrate);
        Assert.False(config.SpeculativeExecutionEnabled);
        Assert.Equal(95.5, config.SpeculativeRetryPercentile);
        Assert.Equal(5, config.MaxSpeculativeExecutions);
        Assert.Equal(3, config.RemoteHostsPerDc);
        Assert.Equal(4, config.ProtocolVersion);
        Assert.Equal(truststore, config.Truststore);
        Assert.Equal(keystore, config.Keystore);
    }

    [Fact]
    public void SslConfiguration_PropertiesWork()
    {
        // Arrange
        var sslConfig = new SslConfiguration();

        // Act
        sslConfig.Path = "/path/to/cert.pfx";
        sslConfig.Password = "secure123";

        // Assert
        Assert.Equal("/path/to/cert.pfx", sslConfig.Path);
        Assert.Equal("secure123", sslConfig.Password);
    }

    [Fact]
    public void Seeds_CanHandleMultipleFormats()
    {
        // Arrange
        var config = new CassandraConfiguration();

        // Act
        config.Seeds = new List<string> 
        { 
            "192.168.1.1", 
            "cassandra-node1:9042",
            "10.0.0.1:9142",
            "localhost"
        };

        // Assert
        Assert.Equal(4, config.Seeds.Count);
        Assert.Contains("192.168.1.1", config.Seeds);
        Assert.Contains("cassandra-node1:9042", config.Seeds);
        Assert.Contains("10.0.0.1:9142", config.Seeds);
        Assert.Contains("localhost", config.Seeds);
    }
}