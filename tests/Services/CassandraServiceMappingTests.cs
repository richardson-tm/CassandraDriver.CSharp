using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using CassandraDriver.Configuration;
using CassandraDriver.Mapping;
using CassandraDriver.Mapping.Attributes;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace CassandraDriver.Tests.Services
{
    // Sample Entity for testing
    [Table("test_entities")]
    public class TestEntity
    {
        [PartitionKey]
        [Column("id")]
        public Guid Id { get; set; }

        [ClusteringKey(0)]
        [Column("cluster_key")]
        public int ClusterKey { get; set; }

        [Column("name")]
        public string? Name { get; set; }

        [Column("value")]
        public double Value { get; set; }

        [Ignore]
        public string? IgnoredProperty { get; set; }

        [Computed("writetime(value)")]
        [Column("value_writetime")] // Alias for the computed value
        public long ValueWriteTime { get; private set; } // Typically read-only

        public DateTimeOffset Timestamp { get; set; } // Implicit column name "timestamp"
    }

    public enum TestEnum { Value1, Value2, Value3 }

    [Table("enum_entities")]
    public class EnumEntity
    {
        [PartitionKey] public int Id { get; set; }
        [Column("enum_val_string")] public TestEnum EnumValString { get; set; }
        [Column("enum_val_int")] public TestEnum EnumValInt { get; set; }
    }


    public class CassandraServiceMappingTests : IDisposable
    {
        private readonly Mock<IOptions<CassandraConfiguration>> _mockOptions;
        private readonly Mock<ILogger<CassandraService>> _mockLogger;
        private readonly CassandraConfiguration _configuration;
        private readonly TableMappingResolver _mappingResolver;
        private TestCassandraService _service; // Using TestCassandraService from CassandraServiceTests
        private Mock<ISession> _mockSession;
        private Mock<ICluster> _mockCluster;

        // Helper class from CassandraServiceTests - ensure it's accessible or duplicated if needed
        // For now, assuming it can be used or defined similarly.
        private class TestCassandraService : CassandraService
        {
            public Mock<ISession> MockSession { get; }
            public Mock<ICluster> MockCluster { get; }

            public TestCassandraService(
                IOptions<CassandraConfiguration> configuration,
                ILogger<CassandraService> logger,
                TableMappingResolver mappingResolver,
                Mock<ISession> mockSession,
                Mock<ICluster> mockCluster)
                : base(configuration, logger, mappingResolver)
            {
                MockSession = mockSession;
                MockCluster = mockCluster;
            }

            public override ISession Session => MockSession.Object;
            public override ICluster Cluster => MockCluster.Object;
        }


        public CassandraServiceMappingTests()
        {
            _mockOptions = new Mock<IOptions<CassandraConfiguration>>();
            _mockLogger = new Mock<ILogger<CassandraService>>();
            _configuration = new CassandraConfiguration();
            _mockOptions.Setup(x => x.Value).Returns(_configuration);

            _mappingResolver = new TableMappingResolver(); // Instantiate our resolver

            _mockSession = new Mock<ISession>();
            _mockCluster = new Mock<ICluster>();

            _mockSession.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                .ReturnsAsync(new RowSet()); // Default behavior

            _service = new TestCassandraService(_mockOptions.Object, _mockLogger.Object, _mappingResolver, _mockSession, _mockCluster);
        }

        public void Dispose()
        {
            // Cleanup if needed
        }

        // --- GetAsync Tests ---
        [Fact]
        public async Task GetAsync_ConstructsCorrectCql_AndMapsRow()
        {
            // Arrange
            var entityId = Guid.NewGuid();
            var clusterKey = 123;
            var entityName = "Test Name";
            var entityValue = 45.67;
            var entityTimestamp = DateTimeOffset.UtcNow;
            var entityValueWriteTime = DateTime.UtcNow.Ticks; // long

            var mockRow = new Mock<Row>();
            var columnDefinitions = new ColumnDefinitions(new Column[] {
                new Column { Name = "id", TypeCode = ColumnTypeCode.Uuid, Index = 0, Keyspace = "ks", Table = "tbl" },
                new Column { Name = "cluster_key", TypeCode = ColumnTypeCode.Int, Index = 1, Keyspace = "ks", Table = "tbl" },
                new Column { Name = "name", TypeCode = ColumnTypeCode.Text, Index = 2, Keyspace = "ks", Table = "tbl" },
                new Column { Name = "value", TypeCode = ColumnTypeCode.Double, Index = 3, Keyspace = "ks", Table = "tbl" },
                new Column { Name = "value_writetime", TypeCode = ColumnTypeCode.Bigint, Index = 4, Keyspace = "ks", Table = "tbl" },
                new Column { Name = "timestamp", TypeCode = ColumnTypeCode.Timestamp, Index = 5, Keyspace = "ks", Table = "tbl" },
            });

            mockRow.Setup(r => r.GetValue<Guid>("id")).Returns(entityId);
            mockRow.Setup(r => r.GetValue<int>("cluster_key")).Returns(clusterKey);
            mockRow.Setup(r => r.GetValue<string>("name")).Returns(entityName);
            mockRow.Setup(r => r.GetValue<double>("value")).Returns(entityValue);
            mockRow.Setup(r => r.GetValue<long>("value_writetime")).Returns(entityValueWriteTime);
            mockRow.Setup(r => r.GetValue<DateTimeOffset>("timestamp")).Returns(entityTimestamp);

            // Setup ContainsColumn for all expected columns
            mockRow.Setup(r => r.ContainsColumn("id")).Returns(true);
            mockRow.Setup(r => r.ContainsColumn("cluster_key")).Returns(true);
            mockRow.Setup(r => r.ContainsColumn("name")).Returns(true);
            mockRow.Setup(r => r.ContainsColumn("value")).Returns(true);
            mockRow.Setup(r => r.ContainsColumn("value_writetime")).Returns(true);
            mockRow.Setup(r => r.ContainsColumn("timestamp")).Returns(true);


            var rowSet = new RowSet(new ExecutionInfo(), new Row[] { mockRow.Object }, columnDefinitions, _mockSession.Object);

            _mockSession.Setup(s => s.ExecuteAsync(It.Is<IStatement>(stmt =>
                    stmt.Cql.StartsWith("SELECT id, cluster_key, name, value, writetime(value) AS value_writetime, timestamp FROM test_entities WHERE \"id\" = ? AND \"cluster_key\" = ?"))))
                .ReturnsAsync(rowSet)
                .Verifiable();

            // Act
            var result = await _service.GetAsync<TestEntity>(entityId, clusterKey);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(entityId, result.Id);
            Assert.Equal(clusterKey, result.ClusterKey);
            Assert.Equal(entityName, result.Name);
            Assert.Equal(entityValue, result.Value);
            Assert.Equal(entityTimestamp, result.Timestamp);
            Assert.Equal(entityValueWriteTime, result.ValueWriteTime); // Check computed
            Assert.Null(result.IgnoredProperty); // Ignored

            _mockSession.Verify(); // Verifies the ExecuteAsync setup
        }

        // TODO: Add more tests for GetAsync (not found, wrong PK components)

        // --- InsertAsync Tests ---
        [Fact]
        public async Task InsertAsync_ConstructsCorrectCql_WithAllParameters()
        {
            // Arrange
            var entity = new TestEntity
            {
                Id = Guid.NewGuid(),
                ClusterKey = 1,
                Name = "Insert Me",
                Value = 123.45,
                Timestamp = DateTimeOffset.UtcNow,
                IgnoredProperty = "Should Be Ignored"
            };

            _mockSession.Setup(s => s.ExecuteAsync(It.Is<IStatement>(stmt =>
                    stmt.Cql == "INSERT INTO test_entities (\"id\", \"cluster_key\", \"name\", \"value\", \"timestamp\") VALUES (?, ?, ?, ?, ?)" &&
                    ((SimpleStatement)stmt).Parameters.Length == 5 &&
                    (Guid)((SimpleStatement)stmt).Parameters[0] == entity.Id &&
                    (int)((SimpleStatement)stmt).Parameters[1] == entity.ClusterKey &&
                    (string)((SimpleStatement)stmt).Parameters[2] == entity.Name &&
                    (double)((SimpleStatement)stmt).Parameters[3] == entity.Value &&
                    (DateTimeOffset)((SimpleStatement)stmt).Parameters[4] == entity.Timestamp
                )))
                .ReturnsAsync(new RowSet())
                .Verifiable();

            // Act
            await _service.InsertAsync(entity);

            // Assert
            _mockSession.Verify();
        }

        [Fact]
        public async Task InsertAsync_HandlesIfNotExists_AndTtl()
        {
            // Arrange
            var entity = new TestEntity { Id = Guid.NewGuid(), ClusterKey = 2, Name = "Insert If Not Exists", Value = 1.0, Timestamp = DateTimeOffset.UtcNow };
            var ttl = 3600;

            _mockSession.Setup(s => s.ExecuteAsync(It.Is<IStatement>(stmt =>
                    stmt.Cql == "INSERT INTO test_entities (\"id\", \"cluster_key\", \"name\", \"value\", \"timestamp\") VALUES (?, ?, ?, ?, ?) IF NOT EXISTS USING TTL ?" &&
                     ((SimpleStatement)stmt).Parameters.Length == 6 &&
                     (int)((SimpleStatement)stmt).Parameters[5] == ttl // TTL is the last parameter
                )))
                .ReturnsAsync(new RowSet())
                .Verifiable();

            // Act
            await _service.InsertAsync(entity, ifNotExists: true, ttl: ttl);

            // Assert
            _mockSession.Verify();
        }

        // TODO: Add tests for InsertAsync with null values for nullable properties

        // --- UpdateAsync Tests ---
        [Fact]
        public async Task UpdateAsync_ConstructsCorrectCql()
        {
            // Arrange
            var entity = new TestEntity
            {
                Id = Guid.NewGuid(),
                ClusterKey = 3,
                Name = "Update Me",
                Value = 543.21,
                Timestamp = DateTimeOffset.UtcNow.AddHours(-1)
            };

            // Parameters for SET, then parameters for WHERE
            // SET "name" = ?, "value" = ?, "timestamp" = ? WHERE "id" = ? AND "cluster_key" = ?
             _mockSession.Setup(s => s.ExecuteAsync(It.Is<IStatement>(stmt =>
                    stmt.Cql == "UPDATE test_entities SET \"name\" = ?, \"value\" = ?, \"timestamp\" = ? WHERE \"id\" = ? AND \"cluster_key\" = ?" &&
                    ((SimpleStatement)stmt).Parameters.Length == 5 &&
                    (string)((SimpleStatement)stmt).Parameters[0] == entity.Name &&
                    (double)((SimpleStatement)stmt).Parameters[1] == entity.Value &&
                    (DateTimeOffset)((SimpleStatement)stmt).Parameters[2] == entity.Timestamp &&
                    (Guid)((SimpleStatement)stmt).Parameters[3] == entity.Id &&
                    (int)((SimpleStatement)stmt).Parameters[4] == entity.ClusterKey
                )))
                .ReturnsAsync(new RowSet())
                .Verifiable();

            // Act
            await _service.UpdateAsync(entity);

            // Assert
            _mockSession.Verify();
        }

        // TODO: Add tests for UpdateAsync with TTL

        // --- DeleteAsync Tests ---
        [Fact]
        public async Task DeleteAsync_ByPrimaryKeyComponents_ConstructsCorrectCql()
        {
            // Arrange
            var entityId = Guid.NewGuid();
            var clusterKey = 10;

            _mockSession.Setup(s => s.ExecuteAsync(It.Is<IStatement>(stmt =>
                    stmt.Cql == "DELETE FROM test_entities WHERE \"id\" = ? AND \"cluster_key\" = ?" &&
                    ((SimpleStatement)stmt).Parameters.Length == 2 &&
                    (Guid)((SimpleStatement)stmt).Parameters[0] == entityId &&
                    (int)((SimpleStatement)stmt).Parameters[1] == clusterKey
                )))
                .ReturnsAsync(new RowSet())
                .Verifiable();

            // Act
            await _service.DeleteAsync<TestEntity>(entityId, clusterKey);

            // Assert
            _mockSession.Verify();
        }

        [Fact]
        public async Task DeleteAsync_ByEntity_ConstructsCorrectCql()
        {
            // Arrange
            var entity = new TestEntity { Id = Guid.NewGuid(), ClusterKey = 11, Name = "Delete Me" };

            _mockSession.Setup(s => s.ExecuteAsync(It.Is<IStatement>(stmt =>
                    stmt.Cql == "DELETE FROM test_entities WHERE \"id\" = ? AND \"cluster_key\" = ?" &&
                     ((SimpleStatement)stmt).Parameters.Length == 2 &&
                    (Guid)((SimpleStatement)stmt).Parameters[0] == entity.Id &&
                    (int)((SimpleStatement)stmt).Parameters[1] == entity.ClusterKey
                )))
                .ReturnsAsync(new RowSet())
                .Verifiable();

            // Act
            await _service.DeleteAsync(entity);

            // Assert
            _mockSession.Verify();
        }

        // --- Enum Mapping Tests ---
        [Fact]
        public async Task GetAsync_MapsEnumsCorrectly()
        {
            // Arrange
            var entityId = 1;
            var enumString = TestEnum.Value2;
            var enumInt = TestEnum.Value3;

            var mockRow = new Mock<Row>();
            var columnDefinitions = new ColumnDefinitions(new Column[] {
                new Column { Name = "id", TypeCode = ColumnTypeCode.Int, Index = 0, Keyspace = "ks", Table = "tbl" },
                new Column { Name = "enum_val_string", TypeCode = ColumnTypeCode.Text, Index = 1, Keyspace = "ks", Table = "tbl" },
                new Column { Name = "enum_val_int", TypeCode = ColumnTypeCode.Int, Index = 2, Keyspace = "ks", Table = "tbl" },
            });

            mockRow.Setup(r => r.GetValue<int>("id")).Returns(entityId);
            mockRow.Setup(r => r.GetValue<string>("enum_val_string")).Returns(enumString.ToString());
            mockRow.Setup(r => r.GetValue<int>("enum_val_int")).Returns((int)enumInt);

            mockRow.Setup(r => r.ContainsColumn("id")).Returns(true);
            mockRow.Setup(r => r.ContainsColumn("enum_val_string")).Returns(true);
            mockRow.Setup(r => r.ContainsColumn("enum_val_int")).Returns(true);

            var rowSet = new RowSet(new ExecutionInfo(), new Row[] { mockRow.Object }, columnDefinitions, _mockSession.Object);

            _mockSession.Setup(s => s.ExecuteAsync(It.Is<IStatement>(stmt => stmt.Cql.StartsWith("SELECT id, enum_val_string, enum_val_int FROM enum_entities"))))
                .ReturnsAsync(rowSet);

            // Act
            var result = await _service.GetAsync<EnumEntity>(entityId);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(entityId, result.Id);
            Assert.Equal(enumString, result.EnumValString);
            Assert.Equal(enumInt, result.EnumValInt);
        }

        [Fact]
        public async Task InsertAsync_MapsEnumsCorrectly()
        {
            // Arrange
            var entity = new EnumEntity { Id = 1, EnumValString = TestEnum.Value1, EnumValInt = TestEnum.Value2 };

            _mockSession.Setup(s => s.ExecuteAsync(It.Is<IStatement>(stmt =>
                    stmt.Cql.Contains("INSERT INTO enum_entities") &&
                    ((SimpleStatement)stmt).Parameters[1] is string && (string)((SimpleStatement)stmt).Parameters[1] == TestEnum.Value1.ToString() && // Driver typically converts enums to string by default
                    ((SimpleStatement)stmt).Parameters[2] is int && (int)((SimpleStatement)stmt).Parameters[2] == (int)TestEnum.Value2 // Or handle as int if specified
                )))
                .Callback<IStatement>(stmt => {
                    // Cassandra C# driver's default enum handling might convert enums to strings or integers.
                    // The test here assumes that if the property type is enum, the value sent to the driver could be its string representation or underlying int.
                    // For robust testing, one might need to configure the driver's mapping options or ensure the custom mapper prepares values as expected by the database schema.
                    // For this example, let's assume string for EnumValString and int for EnumValInt based on typical schema design.
                    // This part of the test is more about how the custom mapper prepares data for the SimpleStatement.
                    // The custom mapping logic in CassandraService's InsertAsync should ensure p.PropertyInfo.GetValue(entity) results in appropriate types.
                    // The driver itself might do further conversion.
                    // For now, the assertion is simplified.
                     Assert.Equal(entity.EnumValString.ToString(), ((SimpleStatement)stmt).Parameters[1]);
                     Assert.Equal((int)entity.EnumValInt, ((SimpleStatement)stmt).Parameters[2]);

                })
                .ReturnsAsync(new RowSet())
                .Verifiable();

            // Act
            await _service.InsertAsync(entity);

            // Assert
            _mockSession.Verify();
        }
    }
}
