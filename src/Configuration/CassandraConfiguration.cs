using Cassandra;

namespace CassandraDriver.Configuration;

public class CassandraConfiguration
{
    public CassandraConfiguration()
    {
        Seeds = new List<string>();
        ShareEventLoopGroup = false;
        Ec2TranslationEnabled = true;
        MigrationFile = "/migrations/cql.changelog";
        AutoMigrate = false;
        SpeculativeExecutionEnabled = true;
        SpeculativeRetryPercentile = 99;
        MaxSpeculativeExecutions = 3;
        RemoteHostsPerDc = 1;
        RetryPolicy = new RetryPolicyConfiguration();
    }

    public SslConfiguration? Truststore { get; set; }
    public SslConfiguration? Keystore { get; set; }
    
    public string? User { get; set; }
    public string? Password { get; set; }
    
    public string? Keyspace { get; set; }
    
    public bool ShareEventLoopGroup { get; set; }
    
    public bool Ec2TranslationEnabled { get; set; }
    
    public string MigrationFile { get; set; }
    public bool AutoMigrate { get; set; }
    
    public bool SpeculativeExecutionEnabled { get; set; }
    
    public double SpeculativeRetryPercentile { get; set; }
    
    public int MaxSpeculativeExecutions { get; set; }
    
    public List<string> Seeds { get; set; }
    
    public int RemoteHostsPerDc { get; set; }
    
    public int? ProtocolVersion { get; set; }

    public ConsistencyLevel? DefaultConsistencyLevel { get; set; }

    public ConsistencyLevel? DefaultSerialConsistencyLevel { get; set; }

    public RetryPolicyConfiguration RetryPolicy { get; set; }
}

public class RetryPolicyConfiguration
{
    public bool Enabled { get; set; } = false;
    public int MaxRetries { get; set; } = 3;
    public int DelayMilliseconds { get; set; } = 1000;
    public int MaxDelayMilliseconds { get; set; } = 30000;
}

public class SslConfiguration
{
    public string? Path { get; set; }
    public string? Password { get; set; }
}