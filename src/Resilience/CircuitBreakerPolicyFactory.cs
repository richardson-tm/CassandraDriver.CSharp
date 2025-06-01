// src/Resilience/CircuitBreakerPolicyFactory.cs
using System;
using Polly;
using Polly.CircuitBreaker;

using Cassandra; // For RowSet

namespace CassandraDriver.Resilience
{
    public static class CircuitBreakerPolicyFactory
    {
        public static Polly.CircuitBreaker.AsyncCircuitBreakerPolicy<Cassandra.RowSet> CreateDefaultCircuitBreakerPolicy(
            int exceptionsAllowedBeforeBreaking = 5,
            TimeSpan durationOfBreak = default,
            Action<DelegateResult<Cassandra.RowSet>, TimeSpan, Context>? onBreak = null,
            Action<Context>? onReset = null,
            Action? onHalfOpen = null)
        {
            if (durationOfBreak == default)
            {
                durationOfBreak = TimeSpan.FromSeconds(30);
            }

            return Policy
                .Handle<Exception>()
                .OrResult<Cassandra.RowSet>(r => false)
                .CircuitBreakerAsync<Cassandra.RowSet>( // Explicitly generic
                    exceptionsAllowedBeforeBreaking,
                    durationOfBreak,
                    onBreak ?? ((delegateResult, timespan, context) =>
                    {
                        // Default onBreak behavior
                    }),
                    onReset ?? (context =>
                    {
                        // Default onReset behavior
                    }),
                    onHalfOpen ?? (() =>
                    {
                        // Default onHalfOpen behavior
                    })
                );
        }

        public static Polly.CircuitBreaker.AsyncCircuitBreakerPolicy<Cassandra.RowSet> CreateCassandraCircuitBreakerPolicy(
            int exceptionsAllowedBeforeBreaking = 3,
            TimeSpan durationOfBreak = default,
            Action<DelegateResult<Cassandra.RowSet>, TimeSpan, Context>? onBreak = null,
            Action<Context>? onReset = null,
            Action? onHalfOpen = null)
        {
            if (durationOfBreak == default)
            {
                durationOfBreak = TimeSpan.FromSeconds(60);
            }

            Func<Exception, bool> isCassandraCriticalException = ex =>
                (ex.GetType().FullName?.Contains("Cassandra.NoHostAvailableException") == true) ||
                (ex.GetType().FullName?.Contains("Cassandra.AuthenticationException") == true) ||
                (ex.GetType().FullName?.Contains("Cassandra.InvalidQueryException") == true && !IsQuerySyntaxError(ex));

            return Policy
                .Handle(isCassandraCriticalException)
                .OrResult<Cassandra.RowSet>(r => false)
                .CircuitBreakerAsync<Cassandra.RowSet>( // Explicitly generic
                    exceptionsAllowedBeforeBreaking,
                    durationOfBreak,
                    onBreak ?? ((delegateResult, timespan, context) =>
                    {
                        // Default onBreak behavior
                    }),
                    onReset ?? (context =>
                    {
                        // Default onReset behavior
                    }),
                    onHalfOpen ?? (() =>
                    {
                        // Default onHalfOpen behavior
                    })
                );
        }

        // Helper to refine exception handling logic
        private static bool IsQuerySyntaxError(Exception ex)
        {
            // Crude check, real implementation would inspect error codes or specific properties
            return ex.Message.ToLowerInvariant().Contains("syntax error");
        }
    }
}
