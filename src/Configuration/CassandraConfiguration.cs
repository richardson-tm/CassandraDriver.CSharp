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
        Migrations = new MigrationConfiguration(); // Initialize Migrations configuration
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
    public CircuitBreakerPolicyConfiguration? CircuitBreakerPolicy { get; set; }
    public PoolingOptionsConfiguration? Pooling { get; set; }
    public QueryOptionsConfiguration? QueryOptions { get; set; }
    public MigrationConfiguration Migrations { get; set; }
}

public class CircuitBreakerPolicyConfiguration
{
    public int ExceptionsAllowedBeforeBreaking { get; set; } = 5;
    public int DurationOfBreakSeconds { get; set; } = 30;
}

public class MigrationConfiguration
{
    public bool Enabled { get; set; } = false;
    public string ScriptsLocation { get; set; } = "cassandramigrations";
    public string TableName { get; set; } = "schema_migrations";
}

public class QueryOptionsConfiguration
{
    public int? DefaultPageSize { get; set; }
    public bool? PrepareStatementsOnAllHosts { get; set; } // Renamed for clarity, maps to SetPrepareOnAllHosts
    public bool? ReprepareStatementsOnUp { get; set; } // Renamed for clarity, maps to SetReprepareOnUp
    public bool? DefaultMetadataSyncEnabled { get; set; } // Renamed for clarity, maps to SetMetadataSyncEnabled
    // Add other relevant QueryOptions properties if needed, e.g., DefaultConsistencyLevel, DefaultSerialConsistencyLevel
    // However, these are already on CassandraConfiguration. QueryOptions can override them per query.
    // For global query options, these are the main ones related to prepared statements and paging.
}

public class PoolingOptionsConfiguration
{
    public int? CoreConnectionsPerHostLocal { get; set; }
    public int? MaxConnectionsPerHostLocal { get; set; }
    public int? CoreConnectionsPerHostRemote { get; set; }
    public int? MaxConnectionsPerHostRemote { get; set; }

    public int? MinRequestsPerConnectionThresholdLocal { get; set; } // New: Maps to SetMinRequestsPerConnectionThreshold(HostDistance.Local, value)
    public int? MinRequestsPerConnectionThresholdRemote { get; set; } // New

    public int? MaxRequestsPerConnection { get; set; }
    public int? MaxQueueSize { get; set; } // New: Maps to SetMaxQueueSize(value)

    public int? HeartbeatIntervalMillis { get; set; }
    public int? PoolTimeoutMillis { get; set; }

    // The IdleTimeoutSeconds is not directly on PoolingOptions but on SocketOptions.
    // However, some drivers/versions might have it on PoolingOptions.
    // For now, let's assume it's not here unless specifically requested for PoolingOptions.
    // public int? IdleTimeoutSeconds { get; set; }
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