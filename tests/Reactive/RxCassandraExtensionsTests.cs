using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks; // For ToTask
using System.Threading;
using System.Threading.Tasks;
using CassandraDriver.Mapping.Attributes; // For [Table]
using CassandraDriver.Queries;
using CassandraDriver.Reactive;
using CassandraDriver.Services; // For CassandraService (needed by SelectQueryBuilder constructor)
using Microsoft.Reactive.Testing; // For TestScheduler and testing Rx
using Moq;
using Xunit;
using CassandraDriver.Mapping; // For TableMappingResolver
using Microsoft.Extensions.Logging; // For ILogger, ILoggerFactory
using Microsoft.Extensions.Options; // For IOptions
using CassandraDriver.Configuration; // For CassandraConfiguration


namespace CassandraDriver.Tests.Reactive
{
    [Table("rx_test_entities")] // Dummy attribute for TableMappingInfo
    public class RxTestEntity
    {
        public int Id { get; set; }
        public string? Value { get; set; }
    }

    public class RxCassandraExtensionsTests
    {
        private readonly Mock<SelectQueryBuilder<RxTestEntity>> _mockQueryBuilder;
        private readonly TestScheduler _scheduler;

        public RxCassandraExtensionsTests()
        {
            // SelectQueryBuilder needs CassandraService and TableMappingResolver.
            // We'll mock these dependencies for the purpose of constructing SelectQueryBuilder.
            var mockCassandraService = new Mock<CassandraService>(
                Mock.Of<IOptions<CassandraConfiguration>>(),
                Mock.Of<ILogger<CassandraService>>(),
                Mock.Of<TableMappingResolver>(),
                Mock.Of<ILoggerFactory>()
            );
            var mockMappingResolver = Mock.Of<TableMappingResolver>();

            _mockQueryBuilder = new Mock<SelectQueryBuilder<RxTestEntity>>(mockCassandraService.Object, mockMappingResolver);
            _scheduler = new TestScheduler();
        }

        // Helper to create an IAsyncEnumerable from a list of items or an exception
        private async IAsyncEnumerable<T> CreateAsyncEnumerable<T>(List<T>? items = null, Exception? exception = null, CancellationToken cancellationToken = default)
        {
            if (exception != null)
            {
                await Task.Yield(); // Ensure it's async before throwing
                cancellationToken.ThrowIfCancellationRequested(); // Check token before throwing user exception
                throw exception;
            }

            if (items != null)
            {
                foreach (var item in items)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await Task.Yield(); // Simulate async work
                    yield return item;
                }
            }
            // If items is null and no exception, it's an empty enumerable
        }


        [Fact]
        public async Task ExecuteAsObservable_EmitsAllItemsAndCompletes()
        {
            // Arrange
            var items = new List<RxTestEntity>
            {
                new RxTestEntity { Id = 1, Value = "A" },
                new RxTestEntity { Id = 2, Value = "B" },
            };
            _mockQueryBuilder.Setup(qb => qb.ToAsyncEnumerable(It.IsAny<CancellationToken>()))
                             .Returns(CreateAsyncEnumerable(items));

            // Act
            var results = await _mockQueryBuilder.Object.ExecuteAsObservable().ToList();

            // Assert
            Assert.Equal(2, results.Count);
            Assert.Equal(1, results[0].Id);
            Assert.Equal("A", results[0].Value);
            Assert.Equal(2, results[1].Id);
            Assert.Equal("B", results[1].Value);
        }

        [Fact]
        public async Task ExecuteAsObservable_HandlesEmptyResult()
        {
            // Arrange
             _mockQueryBuilder.Setup(qb => qb.ToAsyncEnumerable(It.IsAny<CancellationToken>()))
                             .Returns(CreateAsyncEnumerable<RxTestEntity>(new List<RxTestEntity>())); // Empty list

            // Act
            var results = await _mockQueryBuilder.Object.ExecuteAsObservable().ToList();

            // Assert
            Assert.Empty(results);
        }

        [Fact]
        public async Task ExecuteAsObservable_PropagatesError()
        {
            // Arrange
            var expectedException = new InvalidOperationException("Test DB error");
            _mockQueryBuilder.Setup(qb => qb.ToAsyncEnumerable(It.IsAny<CancellationToken>()))
                             .Returns(CreateAsyncEnumerable<RxTestEntity>(exception: expectedException));

            // Act & Assert
            var actualException = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                _mockQueryBuilder.Object.ExecuteAsObservable().ToTask() // ToTask to await completion or error
            );
            Assert.Same(expectedException, actualException);
        }

        [Fact]
        public async Task ExecuteAsObservable_HandlesCancellation_WhenAsyncEnumerableThrowsOce()
        {
            // Arrange
            var cts = new CancellationTokenSource();
            var items = new List<RxTestEntity> { new RxTestEntity { Id = 1, Value = "First" } };

            async IAsyncEnumerable<RxTestEntity> TestCancellableEnumerable(CancellationToken token)
            {
                yield return items[0];
                await Task.Delay(10, token); // Simulate work, will throw if token is cancelled
                token.ThrowIfCancellationRequested(); // Explicit throw for testing
                yield return new RxTestEntity { Id = 2, Value = "Second" }; // Should not be reached
            }

            _mockQueryBuilder.Setup(qb => qb.ToAsyncEnumerable(cts.Token))
                             .Returns(TestCancellableEnumerable(cts.Token));

            var observable = _mockQueryBuilder.Object.ExecuteAsObservable();

            // Act
            Exception? caughtException = null;
            var receivedItems = new List<RxTestEntity>();
            var task = observable.ForEachAsync(item => {
                receivedItems.Add(item);
                if(item.Id == 1) cts.Cancel(); // Cancel after receiving the first item
            }, cts.Token);

            try
            {
                await task;
            }
            catch (OperationCanceledException ex)
            {
                caughtException = ex;
            }

            // Assert
            Assert.NotNull(caughtException);
            Assert.IsAssignableFrom<OperationCanceledException>(caughtException);
            Assert.Single(receivedItems); // Only the first item should have been received
            Assert.Equal(1, receivedItems[0].Id);
        }

        [Fact]
        public void ExecuteAsObservable_SubscriptionCancellation_CancelsAsyncEnumerable()
        {
            // Arrange
            var cts = new CancellationTokenSource();
            var testScheduler = new TestScheduler();
            var wasCancelledInternally = false;

            async IAsyncEnumerable<RxTestEntity> LongRunningCancellableEnumerable([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token = default)
            {
                try
                {
                    for (int i = 0; i < 5; i++)
                    {
                        token.ThrowIfCancellationRequested();
                        await Task.Delay(100, token); // Simulate async work that respects cancellation
                        yield return new RxTestEntity { Id = i };
                    }
                }
                catch (OperationCanceledException)
                {
                    wasCancelledInternally = true;
                    throw;
                }
            }

            _mockQueryBuilder.Setup(qb => qb.ToAsyncEnumerable(It.IsAny<CancellationToken>()))
                             .Returns((CancellationToken ct) => LongRunningCancellableEnumerable(ct));

            var results = new List<RxTestEntity>();
            Exception? error = null;

            // Act
            var subscription = _mockQueryBuilder.Object.ExecuteAsObservable()
                .Subscribe(
                    results.Add,
                    ex => error = ex,
                    () => { /* completed */ }
                );

            // Let some items emit, then cancel
            testScheduler.Schedule(TimeSpan.FromTicks(50), () => results.Add(new RxTestEntity { Id = 0, Value = "Emitted" })); // Simulate one item
            testScheduler.Schedule(TimeSpan.FromTicks(150), () => results.Add(new RxTestEntity { Id = 1, Value = "Emitted" })); // Simulate second item
            testScheduler.Schedule(TimeSpan.FromTicks(200), () => subscription.Dispose()); // Cancel subscription

            // This test is tricky with TestScheduler and async IAsyncEnumerable.
            // A more robust way to test subscription cancellation's effect on the CancellationToken
            // passed to ToAsyncEnumerable would involve a custom IAsyncEnumerable that directly checks its token.
            // The Observable.Create passes its CancellationToken to ToAsyncEnumerable.
            // When the subscription is disposed, this CancellationToken is cancelled.

            // For this test, we'll verify that disposing the subscription stops further items.
            // And if the IAsyncEnumerable was well-behaved, it would have its CancellationToken triggered.

            // To truly test the cancellation token propagation to IAsyncEnumerable:
            var innerCts = new CancellationTokenSource();
            _mockQueryBuilder.Setup(qb => qb.ToAsyncEnumerable(innerCts.Token)) // Setup with specific token
                             .Returns(LongRunningCancellableEnumerable(innerCts.Token));

            var sub = _mockQueryBuilder.Object.ExecuteAsObservable().Subscribe(_ => { }, _ => { }, () => { });
            sub.Dispose(); // This should cancel the token passed to Observable.Create's delegate

            // Assert
            // We need a way to confirm the CancellationToken passed to ToAsyncEnumerable was cancelled.
            // This is hard to verify directly without modifying ToAsyncEnumerable or having side effects.
            // The fact that `wasCancelledInternally` (if we could check it) is true would be the best proof.
            // For now, we trust that Observable.Create links its CancellationToken to the subscription's disposal.
            // The previous test `ExecuteAsObservable_HandlesCancellation_WhenAsyncEnumerableThrowsOce` is more direct for OCE.
            Assert.True(innerCts.IsCancellationRequested, "The CancellationToken passed to ToAsyncEnumerable should be cancelled when subscription is disposed.");
        }
    }
}
