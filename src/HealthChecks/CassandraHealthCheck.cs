using CassandraDriver.Services;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace CassandraDriver.HealthChecks;

public class CassandraHealthCheck : IHealthCheck
{
    private readonly CassandraService _cassandraService;
    private readonly ILogger<CassandraHealthCheck> _logger;
    private string? _validationQuery;

    public CassandraHealthCheck(CassandraService cassandraService, ILogger<CassandraHealthCheck> logger)
    {
        _cassandraService = cassandraService;
        _logger = logger;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            // Get connected hosts
            var hosts = _cassandraService.Cluster.AllHosts();
            if (!hosts.Any())
            {
                return HealthCheckResult.Unhealthy("No connected hosts found");
            }

            // Determine validation query based on Cassandra version
            var host = hosts.First();
            var version = host.CassandraVersion;
            
            if (version.Major >= 3)
            {
                _validationQuery = "SELECT * FROM system_schema.keyspaces LIMIT 1";
            }
            else if (version.Major == 2)
            {
                _validationQuery = "SELECT * FROM system.schema_keyspaces LIMIT 1";
            }
            else
            {
                _validationQuery = "SELECT * FROM system.schema_keyspaces LIMIT 1"; // Default for older versions
            }

            // Execute validation query
            await _cassandraService.ExecuteAsync(_validationQuery);
            
            var connectedHostsCount = hosts.Count(h => h.IsUp);
            var data = new Dictionary<string, object>
            {
                ["connected_hosts"] = connectedHostsCount,
                ["total_hosts"] = hosts.Count(),
                ["cassandra_version"] = version.ToString(),
                ["datacenter"] = host.Datacenter ?? "unknown"
            };

            return HealthCheckResult.Healthy($"Cassandra is healthy. Connected to {connectedHostsCount} hosts.", data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Cassandra connection is unhealthy");
            
            var data = new Dictionary<string, object>
            {
                ["error"] = ex.Message,
                ["exception_type"] = ex.GetType().Name
            };

            return HealthCheckResult.Unhealthy("Cassandra connection is unhealthy", ex, data);
        }
    }
}