using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Cassandra;
using CassandraDriver.Configuration;
using CassandraDriver.Mapping;
using CassandraDriver.Mapping.Attributes;
using CassandraDriver.Queries;
using CassandraDriver.Queries.Expressions;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace CassandraDriver.Tests.Queries
{
    [Table("query_test_users")]
    public class QueryTestUser
    {
        [PartitionKey]
        [Column("user_id")]
        public Guid UserId { get; set; }

        [Column("name")]
        public string? Name { get; set; }

        [Column("age")]
        public int Age { get; set; }

        [ClusteringKey(0)]
        [Column("created_at")]
        public DateTimeOffset CreatedAt { get; set; }

        [Computed("writetime(name)")]
        [Column("name_writetime")]
        public long NameWriteTime { get; private set; }

        [Ignore]
        public string? TempData { get; set; }
    }

    public class SelectQueryBuilderTests : IDisposable
    {
        private readonly Mock<IOptions<CassandraConfiguration>> _mockOptions;
        private readonly Mock<ILogger<CassandraService>> _mockLogger;
        private readonly CassandraConfiguration _configuration;
        private readonly TableMappingResolver _mappingResolver;
        private readonly Mock<CassandraService> _mockCassandraService; // Mocking CassandraService itself
        private readonly Mock<ISession> _mockSession; // For direct session verification if needed by CassandraService mock

        public SelectQueryBuilderTests()
        {
            _mockOptions = new Mock<IOptions<CassandraConfiguration>>();
            _mockLogger = new Mock<ILogger<CassandraService>>();
            _configuration = new CassandraConfiguration();
            _mockOptions.Setup(x => x.Value).Returns(_configuration);
            _mappingResolver = new TableMappingResolver();

            _mockSession = new Mock<ISession>();
            _mockSession.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                        .ReturnsAsync(new RowSet()); // Default mock for ExecuteAsync

            // Mock CassandraService. We need to mock ExecuteAsync and MapRowToEntity.
            // CassandraService constructor requires IOptions, ILogger, TableMappingResolver
            _mockCassandraService = new Mock<CassandraService>(_mockOptions.Object, _mockLogger.Object, _mappingResolver);

            // Setup ExecuteAsync on the mocked CassandraService to return a default RowSet
            _mockCassandraService.Setup(cs => cs.ExecuteAsync(It.IsAny<IStatement>(), null, null))
                                 .ReturnsAsync(new RowSet());
            _mockCassandraService.Setup(cs => cs.ExecuteAsync(It.IsAny<string>(), null, null, It.IsAny<object[]>()))
                                 .ReturnsAsync(new RowSet());
        }

        public void Dispose() { }

        private SelectQueryBuilder<QueryTestUser> CreateBuilder()
        {
            // The builder now takes the actual CassandraService instance, not a mock of it,
            // because it calls ExecuteAsync and MapRowToEntity on it.
            // So, we need a real CassandraService that uses a mocked ISession.

            // Re-think: CassandraService needs a real ISession to work.
            // The SelectQueryBuilder calls methods on CassandraService.
            // We should mock the CassandraService's ExecuteAsync and MapRowToEntity methods.
            return new SelectQueryBuilder<QueryTestUser>(_mockCassandraService.Object, _mappingResolver);
        }

        [Fact]
        public void BuildStatement_SelectAll_Default()
        {
            // Arrange
            var builder = CreateBuilder();

            // Act
            var statement = builder.BuildStatement();

            // Assert
            Assert.Equal("SELECT \"user_id\", \"name\", \"age\", \"created_at\", writetime(name) AS \"name_writetime\" FROM \"query_test_users\"", statement.Cql);
        }

        [Fact]
        public void BuildStatement_Select_StringColumns()
        {
            // Arrange
            var builder = CreateBuilder();
            builder.Select("user_id", "name");

            // Act
            var statement = builder.BuildStatement();

            // Assert
            Assert.Equal("SELECT \"user_id\", \"name\" FROM \"query_test_users\"", statement.Cql);
        }

        [Fact]
        public void BuildStatement_Select_ExpressionColumns()
        {
            // Arrange
            var builder = CreateBuilder();
            builder.Select(u => u.UserId).Select(u => u.Name);

            // Act
            var statement = builder.BuildStatement();

            // Assert
            Assert.Equal("SELECT \"user_id\", \"name\" FROM \"query_test_users\"", statement.Cql);
        }

        [Fact]
        public void BuildStatement_Select_ComputedColumnExpression()
        {
            var builder = CreateBuilder();
            builder.Select(u => u.NameWriteTime);
            var statement = builder.BuildStatement();
            Assert.Equal("SELECT writetime(name) AS \"name_writetime\" FROM \"query_test_users\"", statement.Cql);
        }


        [Fact]
        public void BuildStatement_Where_RawCql()
        {
            // Arrange
            var builder = CreateBuilder();
            var userId = Guid.NewGuid();
            builder.Where("user_id = ? AND age > ?", userId, 30);

            // Act
            var statement = builder.BuildStatement();

            // Assert
            Assert.Contains("WHERE user_id = ? AND age > ?", statement.Cql);
            Assert.Equal(2, statement.Parameters.Length);
            Assert.Equal(userId, statement.Parameters[0]);
            Assert.Equal(30, statement.Parameters[1]);
        }

        [Fact]
        public void BuildStatement_Where_Expression()
        {
            // Arrange
            var builder = CreateBuilder();
            var userId = Guid.NewGuid();
            builder.Where(u => u.UserId, QueryOperator.Equal, userId);
            builder.Where(u => u.Age, QueryOperator.GreaterThan, 30);

            // Act
            var statement = builder.BuildStatement();

            // Assert
            Assert.Contains("WHERE \"user_id\" = ? AND \"age\" > ?", statement.Cql);
            Assert.Equal(2, statement.Parameters.Length);
            Assert.Equal(userId, statement.Parameters[0]);
            Assert.Equal(30, statement.Parameters[1]);
        }

        [Fact]
        public void BuildStatement_Where_InOperator_Enumerable()
        {
            var builder = CreateBuilder();
            var ages = new List<int> { 30, 40, 50 };
            builder.Where(u => u.Age, QueryOperator.In, ages);
            var statement = builder.BuildStatement();
            Assert.Contains("WHERE \"age\" IN ?", statement.Cql);
            Assert.Single(statement.Parameters);
            Assert.Equal(ages, statement.Parameters[0]);
        }


        [Fact]
        public void BuildStatement_OrderBy_Expression()
        {
            // Arrange
            var builder = CreateBuilder();
            builder.OrderBy(u => u.CreatedAt, ascending: false);

            // Act
            var statement = builder.BuildStatement();

            // Assert
            Assert.Contains("ORDER BY \"created_at\" DESC", statement.Cql);
        }

        [Fact]
        public void BuildStatement_Limit()
        {
            // Arrange
            var builder = CreateBuilder();
            builder.Limit(10);

            // Act
            var statement = builder.BuildStatement();

            // Assert
            Assert.Contains("LIMIT ?", statement.Cql); // Changed to check for placeholder
            Assert.Single(statement.Parameters);
            Assert.Equal(10, statement.Parameters[0]);
        }

        [Fact]
        public async Task ToListAsync_ExecutesQueryAndMapsResults()
        {
            // Arrange
            var builder = CreateBuilder();
            var userId = Guid.NewGuid();
            var mockRow = new Mock<Row>();
            var columnDefinitions = new ColumnDefinitions(new Column[] { new Column { Name = "user_id", TypeCode = ColumnTypeCode.Uuid, Index = 0 } });
            mockRow.Setup(r => r.GetValue<Guid>("user_id")).Returns(userId);
            mockRow.Setup(r => r.ContainsColumn("user_id")).Returns(true);
             // Mock other properties as needed for a full TestUser mapping
            mockRow.Setup(r => r.ContainsColumn("name")).Returns(false);
            mockRow.Setup(r => r.ContainsColumn("age")).Returns(false);
            mockRow.Setup(r => r.ContainsColumn("created_at")).Returns(false);
            mockRow.Setup(r => r.ContainsColumn("name_writetime")).Returns(false);


            var rowSet = new RowSet(new ExecutionInfo(), new Row[] { mockRow.Object }, columnDefinitions, _mockSession.Object);

            _mockCassandraService.Setup(cs => cs.ExecuteAsync(It.IsAny<IStatement>(), null, null))
                                 .ReturnsAsync(rowSet);
            // Setup MapRowToEntity on the mock
            _mockCassandraService.Setup(cs => cs.MapRowToEntity<QueryTestUser>(It.IsAny<Row>(), It.IsAny<TableMappingInfo>()))
                                 .Returns((Row r, TableMappingInfo tmi) => new QueryTestUser { UserId = r.GetValue<Guid>("user_id") });


            // Act
            var results = await builder.Select(u => u.UserId).ToListAsync();

            // Assert
            Assert.Single(results);
            Assert.Equal(userId, results[0].UserId);
            _mockCassandraService.Verify(cs => cs.ExecuteAsync(It.IsAny<IStatement>(), null, null), Times.Once);
            _mockCassandraService.Verify(cs => cs.MapRowToEntity<QueryTestUser>(mockRow.Object, It.IsAny<TableMappingInfo>()), Times.Once);
        }

        [Fact]
        public async Task FirstOrDefaultAsync_ExecutesQueryWithLimit1AndMapsResult()
        {
            // Arrange
            var builder = CreateBuilder();
            var userId = Guid.NewGuid();
            var mockRow = new Mock<Row>();
            var columnDefinitions = new ColumnDefinitions(new Column[] { new Column { Name = "user_id", TypeCode = ColumnTypeCode.Uuid, Index = 0 } });
            mockRow.Setup(r => r.GetValue<Guid>("user_id")).Returns(userId);
            mockRow.Setup(r => r.ContainsColumn("user_id")).Returns(true);
            // Mock other properties as needed
            mockRow.Setup(r => r.ContainsColumn("name")).Returns(false);
            mockRow.Setup(r => r.ContainsColumn("age")).Returns(false);
            mockRow.Setup(r => r.ContainsColumn("created_at")).Returns(false);
            mockRow.Setup(r => r.ContainsColumn("name_writetime")).Returns(false);

            var rowSet = new RowSet(new ExecutionInfo(), new Row[] { mockRow.Object }, columnDefinitions, _mockSession.Object);

             _mockCassandraService.Setup(cs => cs.ExecuteAsync(It.Is<IStatement>(stmt => stmt.Cql.Contains("LIMIT ?")), null, null))
                                 .ReturnsAsync(rowSet);
            _mockCassandraService.Setup(cs => cs.MapRowToEntity<QueryTestUser>(It.IsAny<Row>(), It.IsAny<TableMappingInfo>()))
                                 .Returns((Row r, TableMappingInfo tmi) => new QueryTestUser { UserId = r.GetValue<Guid>("user_id") });

            // Act
            var result = await builder.Select(u => u.UserId).FirstOrDefaultAsync();

            // Assert
            Assert.NotNull(result);
            Assert.Equal(userId, result!.UserId);
            _mockCassandraService.Verify(cs => cs.ExecuteAsync(It.Is<IStatement>(stmt => stmt.Cql.Contains("LIMIT ?") && (int)stmt.Parameters.Last() == 1 ), null, null), Times.Once);
            _mockCassandraService.Verify(cs => cs.MapRowToEntity<QueryTestUser>(mockRow.Object, It.IsAny<TableMappingInfo>()), Times.Once);
        }
    }
}
