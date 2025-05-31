using System;
using System.Threading;
using System.Threading.Tasks;
using CassandraDriver.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CassandraDriver.Initializers
{
    public class CassandraStartupValidationException : Exception
    {
        public CassandraStartupValidationException(string message) : base(message) { }
        public CassandraStartupValidationException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class CassandraStartupInitializer : IHostedService
    {
        private readonly CassandraService _cassandraService;
        private readonly CassandraStartupInitializerOptions _options;
        private readonly ILogger<CassandraStartupInitializer> _logger;

        public CassandraStartupInitializer(
            CassandraService cassandraService,
            IOptions<CassandraStartupInitializerOptions> options,
            ILogger<CassandraStartupInitializer> logger)
        {
            _cassandraService = cassandraService ?? throw new ArgumentNullException(nameof(cassandraService));
            _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (!_options.Enabled)
            {
                _logger.LogInformation("CassandraStartupInitializer is disabled.");
                return;
            }

            _logger.LogInformation("CassandraStartupInitializer is starting.");

            // CassandraService.StartAsync (called by host) should ensure connection is attempted.
            // We wait for the CassandraService to be operational or for its connection attempt to resolve.
            // A simple check might be to see if the session is available, but this could race if called too early.
            // For now, we assume CassandraService's StartAsync has made it "ready enough" or will throw if failed.
            // The validation query itself will test the connection.

            if (string.IsNullOrWhiteSpace(_options.ValidationQuery))
            {
                _logger.LogInformation("No validation query configured. CassandraStartupInitializer completed.");
                return;
            }

            var overallTimeoutCts = new CancellationTokenSource(_options.Timeout);
            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, overallTimeoutCts.Token);

            try
            {
                _logger.LogInformation("Executing Cassandra startup validation query: {ValidationQuery}", _options.ValidationQuery);

                // We use the ExecuteAsync that takes a CQL string.
                // The CassandraService.ExecuteAsync itself has retry logic and metrics.
                await _cassandraService.ExecuteAsync(_options.ValidationQuery, null, null, linkedCts.Token) // Pass the linked token
                                       .ConfigureAwait(false);

                _logger.LogInformation("Cassandra startup validation query executed successfully.");
                _logger.LogInformation("CassandraStartupInitializer completed successfully.");
            }
            catch (OperationCanceledException ex) when (overallTimeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                _logger.LogError(ex, "Cassandra startup validation query timed out after {TimeoutSeconds}s.", _options.Timeout.TotalSeconds);
                if (_options.ThrowOnFailure)
                {
                    throw new CassandraStartupValidationException($"Cassandra startup validation query timed out after {_options.Timeout.TotalSeconds}s.", ex);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Cassandra startup validation query failed: {ValidationQuery}", _options.ValidationQuery);
                if (_options.ThrowOnFailure)
                {
                    throw new CassandraStartupValidationException($"Cassandra startup validation query failed: {_options.ValidationQuery}", ex);
                }
            }
            finally
            {
                overallTimeoutCts.Dispose();
                linkedCts.Dispose();
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("CassandraStartupInitializer is stopping.");
            return Task.CompletedTask;
        }
    }
}
