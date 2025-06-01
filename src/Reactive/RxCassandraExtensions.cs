using System;
using System.Reactive.Linq; // For Observable.Create
using System.Threading; // For CancellationToken
using CassandraDriver.Queries; // For SelectQueryBuilder<T>

namespace CassandraDriver.Reactive
{
    public static class RxCassandraExtensions
    {
        public static IObservable<T> ExecuteAsObservable<T>(this SelectQueryBuilder<T> queryBuilder) where T : class, new()
        {
            if (queryBuilder == null)
            {
                throw new ArgumentNullException(nameof(queryBuilder));
            }

            return Observable.Create<T>(async (observer, cancellationToken) =>
            {
                try
                {
                    // The ToAsyncEnumerable method on SelectQueryBuilder already handles cancellationToken.
                    await foreach (var item in queryBuilder.ToAsyncEnumerable(cancellationToken).ConfigureAwait(false))
                    {
                        // Check for cancellation before OnNext, as the operation inside ToAsyncEnumerable might have completed
                        // but the overall observable subscription might be cancelled.
                        cancellationToken.ThrowIfCancellationRequested();
                        observer.OnNext(item);
                    }

                    // Check for cancellation one last time before OnCompleted.
                    cancellationToken.ThrowIfCancellationRequested();
                    observer.OnCompleted();
                }
                catch (OperationCanceledException ex)
                {
                    // Check if the OperationCanceledException is due to our own cancellationToken.
                    // If the token passed to Observable.Create is cancelled, it's an expected cancellation.
                    // Otherwise, it might be an unexpected OperationCanceledException from deeper layers.
                    if (ex.CancellationToken == cancellationToken || cancellationToken.IsCancellationRequested)
                    {
                        // This means the observable subscription itself was cancelled.
                        // Rx typically handles this by just stopping, no OnError for this.
                        // However, if ToAsyncEnumerable throws OperationCanceledException due to *its* token
                        // (which is linked to this one), it's an error from the perspective of the sequence.
                        // For simplicity and common Rx behavior, let OnError handle it if the token is from the enumeration.
                        // If the observer's token is cancelled, the await foreach should break and this might not be hit,
                        // or it will be hit with the observer's token.
                        observer.OnError(ex); // Propagate OCE as OnError, as is common.
                    }
                    else
                    {
                        observer.OnError(ex); // Different OCE, treat as error.
                    }
                }
                catch (Exception ex)
                {
                    observer.OnError(ex);
                }
            });
        }
    }
}
