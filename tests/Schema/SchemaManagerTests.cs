using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CassandraDriver.Mapping;
using CassandraDriver.Schema;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging; // For ILogger<SchemaManager>
using Moq;
using Xunit;
using CassandraDriver.Configuration; // For CassandraConfiguration
using Microsoft.Extensions.Options; // For IOptions

namespace CassandraDriver.Tests.Schema
{
    // Re-use SimpleEntity from SchemaGeneratorTests or define a local one
    // For simplicity, assuming SimpleEntity is accessible here (e.g. if in same namespace or imported)
    // If not, it would need to be redefined or imported.
    // For this test, let's assume SimpleEntity is available.

    public class SchemaManagerTests : IDisposable
    {
        private readonly Mock<CassandraService> _mockCassandraService;
        private readonly TableMappingResolver _mappingResolver;
        private readonly Mock<ILogger<SchemaManager>> _mockLogger;
        private readonly SchemaManager _schemaManager;

        public SchemaManagerTests()
        {
            // Basic setup for CassandraService mock
            var mockOptions = new Mock<IOptions<CassandraConfiguration>>();
            mockOptions.Setup(o => o.Value).Returns(new CassandraConfiguration());
            var mockCassandraLogger = new Mock<ILogger<CassandraService>>();
            var mockCassandraMappingResolver = new Mock<TableMappingResolver>(); // CassandraService needs one
            var mockLoggerFactory = new Mock<ILoggerFactory>(); // CassandraService needs one
             mockLoggerFactory.Setup(x => x.CreateLogger(It.IsAny<string>())).Returns(new Mock<ILogger>().Object);


            _mockCassandraService = new Mock<CassandraService>(
                mockOptions.Object,
                mockCassandraLogger.Object,
                mockCassandraMappingResolver.Object, // Pass the one for CassandraService
                mockLoggerFactory.Object);

            _mappingResolver = new TableMappingResolver(); // SchemaManager uses its own, or could share
            _mockLogger = new Mock<ILogger<SchemaManager>>();

            _schemaManager = new SchemaManager(_mockCassandraService.Object, _mappingResolver, _mockLogger.Object);
        }

        public void Dispose() { }

        [Fact]
        public async Task CreateTableAsync_CallsExecuteAsyncWithCorrectCql()
        {
            // Arrange
            var expectedCql = SchemaGenerator.GetCreateTableCql<SimpleEntity>(_mappingResolver, true);
            _mockCassandraService.Setup(s => s.ExecuteAsync(expectedCql, null, null, It.IsAny<object[]>()))
                .Returns(Task.FromResult(new Cassandra.RowSet())) // Return completed task with empty RowSet
                .Verifiable();

            // Act
            await _schemaManager.CreateTableAsync<SimpleEntity>();

            // Assert
            _mockCassandraService.Verify();
        }

        [Fact]
        public async Task CreateIndexAsync_CallsExecuteAsyncWithCorrectCql()
        {
            // Arrange
            Expression<Func<SimpleEntity, object?>> propertyExp = e => e.Name;
            var expectedCql = SchemaGenerator.GetCreateIndexCql<SimpleEntity>(_mappingResolver, propertyExp, null, true);
             _mockCassandraService.Setup(s => s.ExecuteAsync(expectedCql, null, null, It.IsAny<object[]>()))
                .Returns(Task.FromResult(new Cassandra.RowSet()))
                .Verifiable();

            // Act
            await _schemaManager.CreateIndexAsync(propertyExp);

            // Assert
            _mockCassandraService.Verify();
        }

        [Fact]
        public async Task DropTableAsync_CallsExecuteAsyncWithCorrectCql()
        {
            // Arrange
            var expectedCql = SchemaGenerator.GetDropTableCql<SimpleEntity>(_mappingResolver, true);
            _mockCassandraService.Setup(s => s.ExecuteAsync(expectedCql, null, null, It.IsAny<object[]>()))
                .Returns(Task.FromResult(new Cassandra.RowSet()))
                .Verifiable();

            // Act
            await _schemaManager.DropTableAsync<SimpleEntity>();

            // Assert
            _mockCassandraService.Verify();
        }

        [Fact]
        public async Task DropIndexAsync_CallsExecuteAsyncWithCorrectCql()
        {
            // Arrange
            var indexName = "my_test_index";
            var expectedCql = SchemaGenerator.GetDropIndexCql(indexName, true);
            _mockCassandraService.Setup(s => s.ExecuteAsync(expectedCql, null, null, It.IsAny<object[]>()))
                .Returns(Task.FromResult(new Cassandra.RowSet()))
                .Verifiable();

            // Act
            await _schemaManager.DropIndexAsync(indexName);

            // Assert
            _mockCassandraService.Verify();
        }
    }
}
