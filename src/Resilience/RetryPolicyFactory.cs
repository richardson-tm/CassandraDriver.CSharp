// src/Resilience/RetryPolicyFactory.cs
using System;
using Polly;
using Polly.Retry;

using Cassandra; // For RowSet

namespace CassandraDriver.Resilience
{
    public static class RetryPolicyFactory
    {
        public static Polly.Retry.AsyncRetryPolicy<Cassandra.RowSet> CreateExponentialBackoffPolicy(
            int retryCount = 3,
            TimeSpan initialDelay = default,
            double factor = 2.0,
            Action<DelegateResult<Cassandra.RowSet>, TimeSpan, int, Context>? onRetry = null)
        {
            if (initialDelay == default)
            {
                initialDelay = TimeSpan.FromSeconds(1);
            }

            return Policy
                .Handle<Exception>()
                .OrResult<Cassandra.RowSet>(r => false)
                .WaitAndRetryAsync<Cassandra.RowSet>( // Explicitly generic
                    retryCount,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(factor, retryAttempt - 1) * initialDelay.TotalSeconds),
                    onRetry ?? ((delegateResult, timespan, attempt, context) =>
                    {
                        // Default onRetry behavior
                    })
                );
        }

        public static Polly.Retry.AsyncRetryPolicy<Cassandra.RowSet> CreateFixedDelayPolicy(
            int retryCount = 3,
            TimeSpan delay = default,
            Action<DelegateResult<Cassandra.RowSet>, TimeSpan, int, Context>? onRetry = null)
        {
            if (delay == default)
            {
                delay = TimeSpan.FromSeconds(1);
            }

            return Policy
                .Handle<Exception>()
                .OrResult<Cassandra.RowSet>(r => false)
                .WaitAndRetryAsync<Cassandra.RowSet>( // Explicitly generic
                    retryCount,
                    retryAttempt => delay,
                    onRetry ?? ((delegateResult, timespan, attempt, context) =>
                    {
                        // Default onRetry behavior
                    })
                );
        }

        public static Polly.Retry.AsyncRetryPolicy<Cassandra.RowSet> CreateCassandraTimeoutExponentialBackoffPolicy(
            int retryCount = 3,
            TimeSpan initialDelay = default,
            double factor = 2.0,
            Action<DelegateResult<Cassandra.RowSet>, TimeSpan, int, Context>? onRetry = null)
        {
            if (initialDelay == default)
            {
                initialDelay = TimeSpan.FromSeconds(1);
            }

            Func<Exception, bool> isCassandraTransientException = ex =>
                ex is TimeoutException ||
                (ex.GetType().FullName?.Contains("Cassandra.RequestTimeoutException") == true) ||
                (ex.GetType().FullName?.Contains("Cassandra.NoHostAvailableException") == true);

            return Policy
                .Handle(isCassandraTransientException)
                .OrResult<Cassandra.RowSet>(r => false)
                .WaitAndRetryAsync<Cassandra.RowSet>( // Explicitly generic
                    retryCount,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(factor, retryAttempt - 1) * initialDelay.TotalSeconds),
                    onRetry ?? ((delegateResult, timespan, attempt, context) =>
                    {
                        // Default onRetry behavior
                    })
                );
        }
    }
}
