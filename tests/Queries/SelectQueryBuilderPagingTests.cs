using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using CassandraDriver.Configuration;
using CassandraDriver.Mapping;
using CassandraDriver.Mapping.Attributes;
using CassandraDriver.Queries;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace CassandraDriver.Tests.Queries
{
    [Table("paging_test_entities")]
    public class PagingTestEntity
    {
        [PartitionKey] public int Id { get; set; }
        [Column("data")] public string? Data { get; set; }
    }

    public class SelectQueryBuilderPagingTests : IDisposable
    {
        private readonly Mock<CassandraService> _mockCassandraService;
        private readonly TableMappingResolver _mappingResolver;
        private readonly Mock<ISession> _mockSession; // Used by CassandraService mock
        private readonly Mock<IOptions<CassandraConfiguration>> _mockOptions;
        private readonly Mock<ILogger<CassandraService>> _mockCassandraServiceLogger;
        private readonly Mock<ILoggerFactory> _mockLoggerFactory;


        public SelectQueryBuilderPagingTests()
        {
            _mockOptions = new Mock<IOptions<CassandraConfiguration>>();
            _mockOptions.Setup(o => o.Value).Returns(new CassandraConfiguration());
            _mockCassandraServiceLogger = new Mock<ILogger<CassandraService>>();
            _mockMappingResolver = new Mock<TableMappingResolver>(); // Injected into CassandraService
             _mappingResolver = new TableMappingResolver(); // Used directly by builder if not mocking CassandraService methods

            _mockSession = new Mock<ISession>();
            _mockLoggerFactory = new Mock<ILoggerFactory>();
            _mockLoggerFactory.Setup(x => x.CreateLogger(It.IsAny<string>())).Returns(new Mock<ILogger>().Object);


            // Mock CassandraService itself. We need its MapRowToEntity and ExecuteAsync(IStatement) for the builder.
            // The builder calls _cassandraService.ExecuteAsync(statement) for the first RowSet,
            // then relies on rowSet.AutoPageAsync().
            // MapRowToEntity is also on CassandraService.
            _mockCassandraService = new Mock<CassandraService>(
                _mockOptions.Object,
                _mockCassandraServiceLogger.Object,
                _mappingResolver, // Use real resolver for mapping info
                _mockLoggerFactory.Object
            );

            // Delegate calls to MapRowToEntity to the real implementation if possible,
            // or provide a simple mock implementation for testing.
            // Using the real one requires _mappingResolver to be correctly set up for PagingTestEntity.
             _mockCassandraService.Setup(cs => cs.MapRowToEntity<PagingTestEntity>(It.IsAny<Row>(), It.IsAny<TableMappingInfo>()))
                .Returns((Row r, TableMappingInfo tmi) => new PagingTestEntity {
                    Id = r.GetValue<int>("id"),
                    Data = r.GetValue<string>("data")
                });
        }

        public void Dispose() { }

        private SelectQueryBuilder<PagingTestEntity> CreateBuilder()
        {
            return new SelectQueryBuilder<PagingTestEntity>(_mockCassandraService.Object, _mappingResolver);
        }

        private Mock<Row> CreateMockRow(int id, string data)
        {
            var mockRow = new Mock<Row>();
            mockRow.Setup(r => r.GetValue<int>("id")).Returns(id);
            mockRow.Setup(r => r.GetValue<string>("data")).Returns(data);
            mockRow.Setup(r => r.ContainsColumn("id")).Returns(true);
            mockRow.Setup(r => r.ContainsColumn("data")).Returns(true);
            return mockRow;
        }

        [Fact]
        public async Task ToAsyncEnumerable_RetrievesAllRowsAcrossMultiplePages()
        {
            // Arrange
            var builder = CreateBuilder().PageSize(2); // Small page size for testing

            var rowsPage1 = new List<Row> { CreateMockRow(1, "Page1_Row1").Object, CreateMockRow(2, "Page1_Row2").Object };
            var rowsPage2 = new List<Row> { CreateMockRow(3, "Page2_Row1").Object, CreateMockRow(4, "Page2_Row2").Object };
            var rowsPage3 = new List<Row> { CreateMockRow(5, "Page3_Row1").Object };

            var mockRowSetPage1 = new Mock<RowSet>();
            var mockRowSetPage2 = new Mock<RowSet>();
            var mockRowSetPage3 = new Mock<RowSet>();

            // Setup AutoPageAsync to simulate multiple pages
            // This is tricky. AutoPageAsync is an extension method on RowSet.
            // We need ExecuteAsync to return a RowSet that, when AutoPageAsync is called, yields our multi-page data.

            // Let's mock what AutoPageAsync would produce by creating a custom IAsyncEnumerable<Row>
            async IAsyncEnumerable<Row> MockedAutoPaging(List<Row> p1, List<Row> p2, List<Row> p3, CancellationToken token)
            {
                foreach(var r in p1) { token.ThrowIfCancellationRequested(); yield return r; await Task.Yield(); }
                foreach(var r in p2) { token.ThrowIfCancellationRequested(); yield return r; await Task.Yield(); }
                foreach(var r in p3) { token.ThrowIfCancellationRequested(); yield return r; await Task.Yield(); }
            }

            var combinedRows = new List<Row>();
            combinedRows.AddRange(rowsPage1);
            combinedRows.AddRange(rowsPage2);
            combinedRows.AddRange(rowsPage3);

            // The initial ExecuteAsync returns the first RowSet.
            // The RowSet.AutoPageAsync() method then takes over.
            // We need to ensure the RowSet returned by ExecuteAsync, when AutoPageAsync is called on it,
            // produces our desired sequence. The actual PagingState mechanism is internal to AutoPageAsync.

            mockRowSetPage1.Setup(rs => rs.GetEnumerator()).Returns(rowsPage1.GetEnumerator()); // For the first batch of rows if not using AutoPageAsync directly from start
            // This is the key: mock AutoPageAsync on the RowSet that ExecuteAsync returns.
            mockRowSetPage1
                .Setup(rs => rs.AutoPageAsync(It.IsAny<CancellationToken>()))
                .Returns(MockedAutoPaging(rowsPage1, rowsPage2, rowsPage3, CancellationToken.None));


            _mockCassandraService.Setup(cs => cs.ExecuteAsync(It.IsAny<IStatement>()))
                                 .ReturnsAsync(mockRowSetPage1.Object);

            // Act
            var results = new List<PagingTestEntity>();
            await foreach (var entity in builder.ToAsyncEnumerable())
            {
                results.Add(entity);
            }

            // Assert
            Assert.Equal(5, results.Count);
            Assert.Equal(1, results[0].Id);
            Assert.Equal("Page1_Row1", results[0].Data);
            Assert.Equal(3, results[2].Id);
            Assert.Equal("Page2_Row1", results[2].Data);
            Assert.Equal(5, results[4].Id);
            Assert.Equal("Page3_Row1", results[4].Data);

            _mockCassandraService.Verify(cs => cs.MapRowToEntity<PagingTestEntity>(It.IsAny<Row>(), It.IsAny<TableMappingInfo>()), Times.Exactly(5));
            _mockCassandraService.Verify(cs => cs.ExecuteAsync(It.Is<IStatement>(s => s.PageSize == 2)), Times.Once); // Initial query
        }

        [Fact]
        public async Task ToAsyncEnumerable_HandlesEmptyResult()
        {
            // Arrange
            var builder = CreateBuilder();
            var emptyRows = new List<Row>();
            var mockRowSet = new Mock<RowSet>();

            async IAsyncEnumerable<Row> EmptyAutoPaging(CancellationToken token)
            {
                await Task.CompletedTask; // Ensure it's async
                if (false) yield return null; // Standard way to make an empty IAsyncEnumerable
            }

            mockRowSet.Setup(rs => rs.GetEnumerator()).Returns(emptyRows.GetEnumerator());
            mockRowSet.Setup(rs => rs.AutoPageAsync(It.IsAny<CancellationToken>())).Returns(EmptyAutoPaging(CancellationToken.None));

            _mockCassandraService.Setup(cs => cs.ExecuteAsync(It.IsAny<IStatement>()))
                                 .ReturnsAsync(mockRowSet.Object);

            // Act
            var results = new List<PagingTestEntity>();
            await foreach (var entity in builder.ToAsyncEnumerable())
            {
                results.Add(entity);
            }

            // Assert
            Assert.Empty(results);
            _mockCassandraService.Verify(cs => cs.MapRowToEntity<PagingTestEntity>(It.IsAny<Row>(), It.IsAny<TableMappingInfo>()), Times.Never);
        }

        [Fact]
        public async Task ToAsyncEnumerable_RespectsCancellationToken()
        {
            // Arrange
            var builder = CreateBuilder().PageSize(1);
            var cts = new CancellationTokenSource();

            var rows = new List<Row> { CreateMockRow(1, "Row1").Object, CreateMockRow(2, "Row2").Object, CreateMockRow(3, "Row3").Object };

            async IAsyncEnumerable<Row> MockedAutoPagingWithCancel(List<Row> pageRows, CancellationTokenSource tokenSource, CancellationToken token)
            {
                foreach(var r in pageRows)
                {
                    token.ThrowIfCancellationRequested();
                    yield return r;
                    await Task.Yield();
                    if (r.GetValue<int>("id") == 2) tokenSource.Cancel(); // Cancel after the second item
                }
            }

            var mockRowSet = new Mock<RowSet>();
            mockRowSet.Setup(rs => rs.GetEnumerator()).Returns(rows.Take(1).GetEnumerator()); // Initial fetch
            mockRowSet.Setup(rs => rs.AutoPageAsync(It.IsAny<CancellationToken>()))
                      .Returns((CancellationToken ct) => MockedAutoPagingWithCancel(rows, cts, ct));


            _mockCassandraService.Setup(cs => cs.ExecuteAsync(It.IsAny<IStatement>()))
                                 .ReturnsAsync(mockRowSet.Object);

            var results = new List<PagingTestEntity>();

            // Act & Assert
            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await foreach (var entity in builder.ToAsyncEnumerable(cts.Token))
                {
                    results.Add(entity);
                }
            });

            // Verify that items were processed until cancellation
            Assert.Equal(2, results.Count); // Should have processed two items before cancellation
            Assert.Equal(1, results[0].Id);
            Assert.Equal(2, results[1].Id);
        }

        [Fact]
        public async Task ToAsyncEnumerable_AppliesUserDefinedPageSize()
        {
            // Arrange
            var builder = CreateBuilder();
            var userPageSize = 50;
            builder.PageSize(userPageSize);

            var mockRowSet = new Mock<RowSet>();
             async IAsyncEnumerable<Row> EmptyAutoPaging(CancellationToken token) { await Task.CompletedTask; if (false) yield return null; }
            mockRowSet.Setup(rs => rs.AutoPageAsync(It.IsAny<CancellationToken>())).Returns(EmptyAutoPaging(CancellationToken.None));

            _mockCassandraService.Setup(cs => cs.ExecuteAsync(It.Is<IStatement>(stmt => stmt.PageSize == userPageSize)))
                                 .ReturnsAsync(mockRowSet.Object)
                                 .Verifiable();

            // Act
            await foreach (var _ in builder.ToAsyncEnumerable()) { }

            // Assert
            _mockCassandraService.Verify();
        }

        [Fact]
        public async Task ToAsyncEnumerable_AppliesDefaultPageSize_WhenNotSetByUser()
        {
            // Arrange
            var builder = CreateBuilder(); // No PageSize() call

            var mockRowSet = new Mock<RowSet>();
            async IAsyncEnumerable<Row> EmptyAutoPaging(CancellationToken token) { await Task.CompletedTask; if (false) yield return null; }
            mockRowSet.Setup(rs => rs.AutoPageAsync(It.IsAny<CancellationToken>())).Returns(EmptyAutoPaging(CancellationToken.None));

            // Default page size set in SelectQueryBuilder is 100 if not otherwise configured
            _mockCassandraService.Setup(cs => cs.ExecuteAsync(It.Is<IStatement>(stmt => stmt.PageSize == 100)))
                                 .ReturnsAsync(mockRowSet.Object)
                                 .Verifiable();

            // Act
            await foreach (var _ in builder.ToAsyncEnumerable()) { }

            // Assert
            _mockCassandraService.Verify();
        }
    }
}
