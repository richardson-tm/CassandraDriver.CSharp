using CassandraDriver.Configuration;
using CassandraDriver.HealthChecks;
using CassandraDriver.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using CassandraDriver.Initializers; // For CassandraStartupInitializer and Options
using Microsoft.Extensions.Options; // For IOptions (used by Initializer)

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

    public static IServiceCollection AddCassandraStartupInitializer(
        this IServiceCollection services,
        Action<CassandraStartupInitializerOptions>? configureOptions = null)
    {
        // Ensure CassandraService and its dependencies are registered.
        // This is typically done by AddCassandra() before this.
        // Consider adding a check or relying on documentation.

        // Configure options
        if (configureOptions != null)
        {
            services.Configure(configureOptions);
        }
        else
        {
            // Ensure options are available even if not explicitly configured by user,
            // so default values are used.
            services.AddOptions<CassandraStartupInitializerOptions>();
        }

        // Register the initializer as a hosted service
        services.AddHostedService<CassandraStartupInitializer>();

        return services;
    }
}