using System;
using System.Threading.Tasks;
using CassandraDriver.Resilience;
using Polly;
using Xunit;
using Polly.Retry;

namespace CassandraDriver.Tests.Resilience
{
    public class RetryPolicyFactoryTests
    {
        [Fact]
        public void CreateExponentialBackoffPolicy_DefaultParameters_CreatesPolicy()
        {
            // Act
            var policy = RetryPolicyFactory.CreateExponentialBackoffPolicy();

            // Assert
            Assert.NotNull(policy);
            // Further assertions could involve trying to execute and check retry behavior,
            // but that's more complex for a factory unit test.
            // We can check some configurable aspects if they were exposed, but Polly policies hide internals.
        }

        [Fact]
        public async Task CreateExponentialBackoffPolicy_RetriesOnException_AndCallsOnRetry()
        {
            // Arrange
            var retryCount = 2;
            var onRetryCalled = 0;
            var contextKey = "TestOperation";
            var pollyContext = new Context(contextKey);

            var policy = RetryPolicyFactory.CreateExponentialBackoffPolicy(
                retryCount: retryCount,
                initialDelay: TimeSpan.FromMilliseconds(1), // Minimal delay for test speed
                factor: 1.5,
                onRetry: (ex, ts, attempt, ctx) => {
                    onRetryCalled++;
                    Assert.Equal(contextKey, ctx.OperationKey);
                }
            );

            var executionCount = 0;
            Func<Task> action = () => {
                executionCount++;
                throw new Exception("Test exception");
            };

            // Act & Assert
            await Assert.ThrowsAsync<Exception>(() => policy.ExecuteAsync((ctx) => action(), pollyContext));
            Assert.Equal(1 + retryCount, executionCount); // Initial attempt + retries
            Assert.Equal(retryCount, onRetryCalled);
        }

        [Fact]
        public void CreateFixedDelayPolicy_DefaultParameters_CreatesPolicy()
        {
            // Act
            var policy = RetryPolicyFactory.CreateFixedDelayPolicy();

            // Assert
            Assert.NotNull(policy);
        }

        [Fact]
        public async Task CreateFixedDelayPolicy_RetriesOnException_WithFixedDelay()
        {
            // Arrange
            var retryCount = 1;
            var fixedDelay = TimeSpan.FromMilliseconds(5);
            var onRetryCalled = 0;
            var policy = RetryPolicyFactory.CreateFixedDelayPolicy(
                retryCount: retryCount,
                delay: fixedDelay,
                onRetry: (ex, ts, attempt, ctx) => {
                    onRetryCalled++;
                    Assert.Equal(fixedDelay, ts); // Check if the delay is fixed
                }
            );

            var executionCount = 0;
            Func<Task> action = () => {
                executionCount++;
                throw new Exception("Test exception for fixed delay");
            };

            // Act & Assert
            await Assert.ThrowsAsync<Exception>(() => policy.ExecuteAsync(action));
            Assert.Equal(1 + retryCount, executionCount);
            Assert.Equal(retryCount, onRetryCalled);
        }

        [Fact]
        public async Task CreateCassandraTimeoutExponentialBackoffPolicy_RetriesOnSpecificExceptions()
        {
            // Arrange
            var policy = RetryPolicyFactory.CreateCassandraTimeoutExponentialBackoffPolicy(
                retryCount: 1,
                initialDelay: TimeSpan.FromMilliseconds(1)
            );
            var executionCount_Timeout = 0;
            var executionCount_OtherEx = 0;

            Func<Task> timeoutAction = () => {
                executionCount_Timeout++;
                throw new TimeoutException("Simulated timeout");
            };

            // Simulate a non-driver specific exception that the policy might not handle by default
            // For this test, CassandraTimeoutExponentialBackoffPolicy handles generic Exception if not specific Cassandra ones.
            // Let's refine this: it *only* handles specific Cassandra exceptions or TimeoutException.
            // So a generic Exception should NOT be retried by *this specific* policy if it were more restrictive.
            // However, the current CassandraTimeoutExponentialBackoffPolicy handles *any* exception derived from System.Exception
            // if it matches the `isCassandraTransientException` func.
            // For this test, we'll use TimeoutException (handled) and ArgumentException (not handled by a more specific filter).
            // The provided `isCassandraTransientException` also includes specific full name checks.
            // For this test, just TimeoutException is enough.

            // Act & Assert
            // Should retry TimeoutException
            await Assert.ThrowsAsync<TimeoutException>(() => policy.ExecuteAsync(timeoutAction));
            Assert.Equal(2, executionCount_Timeout); // 1 initial + 1 retry

            // If we had an exception it *doesn't* handle:
             Func<Task> otherAction = () => {
                executionCount_OtherEx++;
                throw new ArgumentException("Other exception");
            };
            // This would not retry if ArgumentException is not in `isCassandraTransientException`
            // The current `isCassandraTransientException` handles TimeoutException. It does NOT handle ArgumentException.
            // So, it should not retry.
             var policyForNonRetry = RetryPolicyFactory.CreateCassandraTimeoutExponentialBackoffPolicy(retryCount:1, initialDelay: TimeSpan.FromMilliseconds(1));
             await Assert.ThrowsAsync<ArgumentException>(() => policyForNonRetry.ExecuteAsync(otherAction));
             Assert.Equal(1, executionCount_OtherEx); // Only 1 attempt, no retry
        }
    }
}
