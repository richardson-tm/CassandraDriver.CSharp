// src/Resilience/ResiliencePolicyProfile.cs
namespace CassandraDriver.Resilience
{
    public enum ResiliencePolicyProfile
    {
        /// <summary>
        /// No resilience policy will be applied.
        /// </summary>
        None,
        /// <summary>
        /// Default retry policy (e.g., exponential backoff for transient errors).
        /// </summary>
        DefaultRetry,
        /// <summary>
        /// Default circuit breaker policy.
        /// </summary>
        DefaultCircuitBreaker,
        /// <summary>
        /// Combines default retry and circuit breaker policies.
        /// </summary>
        DefaultRetryAndCircuitBreaker,
        /// <summary>
        /// A more aggressive retry policy, potentially for specific idempotent operations.
        /// </summary>
        IdempotentRetry, // Example for specific idempotent retries
    }

    public class ResilienceOptions
    {
        public ResiliencePolicyProfile Profile { get; set; } = ResiliencePolicyProfile.DefaultRetryAndCircuitBreaker;
        public bool IsIdempotent { get; set; } = false; // Caller should specify if the operation is idempotent

        // Optional: Allow passing specific context to Polly policies
        // public Context PollyContext { get; set; }
    }
}
