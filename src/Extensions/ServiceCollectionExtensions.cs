using CassandraDriver.Configuration;
using CassandraDriver.HealthChecks;
using CassandraDriver.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace CassandraDriver.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddCassandra(this IServiceCollection services, IConfiguration configuration)
    {
        // Configure CassandraConfiguration from appsettings
        services.Configure<CassandraConfiguration>(configuration.GetSection("Cassandra"));
        
        // Register CassandraService as singleton and hosted service
        services.AddSingleton<CassandraService>();
        services.AddHostedService(provider => provider.GetRequiredService<CassandraService>());
        
        // Register health check
        services.AddHealthChecks()
            .Add(new HealthCheckRegistration(
                "cassandra",
                provider => new CassandraHealthCheck(
                    provider.GetRequiredService<CassandraService>(),
                    provider.GetRequiredService<ILogger<CassandraHealthCheck>>()),
                HealthStatus.Unhealthy,
                new[] { "cassandra", "database" },
                TimeSpan.FromSeconds(5)));
        
        return services;
    }
    
    public static IServiceCollection AddCassandra(this IServiceCollection services, Action<CassandraConfiguration> configureOptions)
    {
        // Configure CassandraConfiguration with provided action
        services.Configure(configureOptions);
        
        // Register CassandraService as singleton and hosted service
        services.AddSingleton<CassandraService>();
        services.AddHostedService(provider => provider.GetRequiredService<CassandraService>());
        
        // Register health check
        services.AddHealthChecks()
            .Add(new HealthCheckRegistration(
                "cassandra",
                provider => new CassandraHealthCheck(
                    provider.GetRequiredService<CassandraService>(),
                    provider.GetRequiredService<ILogger<CassandraHealthCheck>>()),
                HealthStatus.Unhealthy,
                new[] { "cassandra", "database" },
                TimeSpan.FromSeconds(5)));
        
        return services;
    }
}