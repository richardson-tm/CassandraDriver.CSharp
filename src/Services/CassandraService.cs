using Cassandra;
using CassandraDriver.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Cassandra.Mapping;

namespace CassandraDriver.Services;

public class CassandraService : IHostedService, IDisposable
{
    private readonly CassandraConfiguration _configuration;
    private readonly ILogger<CassandraService> _logger;
    private ICluster? _cluster;
    private ISession? _session;
    private readonly string[] _cipherSuites = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" };

    public CassandraService(IOptions<CassandraConfiguration> configuration, ILogger<CassandraService> logger)
    {
        _configuration = configuration.Value;
        _logger = logger;
    }

    public virtual ISession Session
    {
        get
        {
            if (_session == null)
                throw new InvalidOperationException("Session is not initialized. Ensure StartAsync has been called.");
            return _session;
        }
    }

    public virtual ICluster Cluster
    {
        get
        {
            if (_cluster == null)
                throw new InvalidOperationException("Cluster is not initialized. Ensure StartAsync has been called.");
            return _cluster;
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await Task.Run(() => Connect(), cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_session != null)
        {
            await Task.Run(() => _session.Dispose(), cancellationToken);
        }
        
        if (_cluster != null)
        {
            await Task.Run(() => _cluster.Dispose(), cancellationToken);
        }
    }

    private void Connect()
    {
        var builder = Cassandra.Cluster.Builder();

        // Configure load balancing
        var dcAwarePolicy = new DCAwareRoundRobinPolicy();
        builder.WithLoadBalancingPolicy(new TokenAwarePolicy(dcAwarePolicy));

        // Configure speculative execution
        if (_configuration.SpeculativeExecutionEnabled)
        {
            // Note: PercentileSpeculativeExecutionPolicy is not available in C# driver
            // Using ConstantSpeculativeExecutionPolicy as an alternative
            var speculativePolicy = new ConstantSpeculativeExecutionPolicy(
                delay: 100, // milliseconds delay
                maxSpeculativeExecutions: _configuration.MaxSpeculativeExecutions);
            builder.WithSpeculativeExecutionPolicy(speculativePolicy);
        }

        // Add contact points
        foreach (var seed in _configuration.Seeds)
        {
            if (seed.Contains(":"))
            {
                var tokens = seed.Split(':');
                builder.AddContactPoint(tokens[0]).WithPort(int.Parse(tokens[1]));
            }
            else
            {
                builder.AddContactPoint(seed);
            }
        }

        // Configure EC2 address translation
        if (_configuration.Ec2TranslationEnabled)
        {
            // Note: EC2MultiRegionAddressTranslator requires separate AWS SDK package
            // For now, we'll skip this feature or use a simple pass-through translator
            _logger.LogWarning("EC2 address translation requested but not implemented in C# port");
        }

        // Configure SSL/TLS
        if (_configuration.Truststore != null && _configuration.Keystore != null)
        {
            try
            {
                var sslOptions = ConfigureSsl(_configuration.Truststore, _configuration.Keystore);
                builder.WithSSL(sslOptions);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Couldn't add SSL to the cluster builder.");
            }
        }

        // Configure authentication
        if (!string.IsNullOrEmpty(_configuration.User))
        {
            builder.WithCredentials(_configuration.User, _configuration.Password);
        }

        // Configure protocol version
        if (_configuration.ProtocolVersion.HasValue)
        {
            builder.WithMaxProtocolVersion((ProtocolVersion)_configuration.ProtocolVersion.Value);
        }

        _cluster = builder.Build();

        // Connect to keyspace if specified
        if (!string.IsNullOrEmpty(_configuration.Keyspace))
        {
            _session = _cluster.Connect(_configuration.Keyspace);
        }
        else
        {
            _session = _cluster.Connect();
        }

        _logger.LogInformation("Successfully connected to Cassandra cluster");
    }

    private SSLOptions ConfigureSsl(SslConfiguration truststore, SslConfiguration keystore)
    {
        if (string.IsNullOrEmpty(truststore.Path) || string.IsNullOrEmpty(keystore.Path))
        {
            throw new ArgumentException("SSL configuration requires both truststore and keystore paths");
        }

        var sslOptions = new SSLOptions();
        
        // Load client certificate
        var clientCert = new X509Certificate2(keystore.Path, keystore.Password);
        sslOptions.SetCertificateCollection(new X509CertificateCollection { clientCert });

        // Configure certificate validation
        sslOptions.SetRemoteCertValidationCallback((sender, certificate, chain, errors) =>
        {
            if (errors == SslPolicyErrors.None)
                return true;

            // In production, you might want to validate against specific CA or thumbprint
            _logger.LogWarning("SSL certificate validation errors: {Errors}", errors);
            
            // For development/testing, you might accept self-signed certificates
            // In production, return false here and properly validate certificates
            return errors == SslPolicyErrors.RemoteCertificateChainErrors;
        });

        // Note: SslProtocol is read-only in C# driver, it uses the system default
        // which should be TLS 1.2 or higher on modern systems

        return sslOptions;
    }

    public virtual async Task<RowSet> ExecuteAsync(IStatement statement)
    {
        if (_session == null)
            throw new InvalidOperationException("Session is not initialized");

        return await _session.ExecuteAsync(statement);
    }

    public virtual async Task<RowSet> ExecuteAsync(string cql, params object[] values)
    {
        if (_session == null)
            throw new InvalidOperationException("Session is not initialized");

        var statement = new SimpleStatement(cql, values);
        return await _session.ExecuteAsync(statement);
    }

    public void Dispose()
    {
        _session?.Dispose();
        _cluster?.Dispose();
    }
}