using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using CassandraDriver.Configuration;
using CassandraDriver.Migrations;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace CassandraDriver.Tests.Migrations
{
    public class CassandraMigrationRunnerTests : IDisposable
    {
        private readonly Mock<CassandraService> _mockCassandraService;
        private readonly Mock<ILogger<CassandraMigrationRunner>> _mockLogger;
        private readonly MigrationConfiguration _migrationConfig;
        private readonly string _tempMigrationScriptsLocation;

        public CassandraMigrationRunnerTests()
        {
            // Mock CassandraService - only need ExecuteAsync for schema_migrations and script execution
            var mockOptions = new Mock<IOptions<CassandraConfiguration>>();
            mockOptions.Setup(o => o.Value).Returns(new CassandraConfiguration()); // Default config
            var mockCassandraLogger = new Mock<ILogger<CassandraService>>();
            var mockMappingResolver = new Mock<Mapping.TableMappingResolver>();
            var mockLoggerFactory = new Mock<ILoggerFactory>();
            mockLoggerFactory.Setup(x => x.CreateLogger(It.IsAny<string>())).Returns(new Mock<ILogger>().Object);


            _mockCassandraService = new Mock<CassandraService>(
                mockOptions.Object,
                mockCassandraLogger.Object,
                mockMappingResolver.Object,
                mockLoggerFactory.Object);

            _mockLogger = new Mock<ILogger<CassandraMigrationRunner>>();

            _migrationConfig = new MigrationConfiguration(); // Use default table name "schema_migrations"

            // Create a temporary directory for migration scripts
            _tempMigrationScriptsLocation = Path.Combine(Path.GetTempPath(), "cassandra_migrations_tests", Path.GetRandomFileName());
            Directory.CreateDirectory(_tempMigrationScriptsLocation);
            _migrationConfig.ScriptsLocation = _tempMigrationScriptsLocation;
        }

        public void Dispose()
        {
            // Clean up temporary migration scripts directory
            if (Directory.Exists(_tempMigrationScriptsLocation))
            {
                Directory.Delete(_tempMigrationScriptsLocation, true);
            }
        }

        private void CreateScriptFile(string fileName, string content)
        {
            File.WriteAllText(Path.Combine(_tempMigrationScriptsLocation, fileName), content);
        }

        [Fact]
        public async Task EnsureSchemaMigrationsTableAsync_CreatesTable()
        {
            // Arrange
            var runner = new CassandraMigrationRunner(_mockCassandraService.Object, _migrationConfig, _mockLogger.Object);
            var expectedCql = $@"
CREATE TABLE IF NOT EXISTS {_migrationConfig.TableName} (
    version TEXT PRIMARY KEY,
    applied_on TIMESTAMP,
    description TEXT
);".Trim();

            _mockCassandraService.Setup(s => s.ExecuteAsync(It.Is<string>(cql => cql.Contains($"CREATE TABLE IF NOT EXISTS {_migrationConfig.TableName}")), null, null, It.IsAny<object[]>()))
                .ReturnsAsync(new RowSet())
                .Verifiable();

            // Act
            await runner.EnsureSchemaMigrationsTableAsync();

            // Assert
            _mockCassandraService.Verify();
        }

        [Fact]
        public async Task DiscoverMigrationsAsync_FindsAndSortsScripts()
        {
            // Arrange
            CreateScriptFile("002_add_data.cql", "INSERT INTO test (id) VALUES (2);");
            CreateScriptFile("001_create_table.cql", "CREATE TABLE test (id int PRIMARY KEY);");
            CreateScriptFile("003_alter_table.cql", "ALTER TABLE test ADD name text;");
            CreateScriptFile("invalid_script.txt", "SELECT * FROM other;"); // Should be ignored
            CreateScriptFile("000_should_be_first.cql", "SELECT * FROM start;");
            CreateScriptFile("not_a_migration.cql", "SELECT 1;"); // Invalid name format

            var runner = new CassandraMigrationRunner(_mockCassandraService.Object, _migrationConfig, _mockLogger.Object);

            // Act
            var migrations = await runner.DiscoverMigrationsAsync(_tempMigrationScriptsLocation);

            // Assert
            Assert.Equal(4, migrations.Count);
            Assert.Equal(0, migrations[0].Version);
            Assert.Equal("should be first", migrations[0].Description);
            Assert.Equal(1, migrations[1].Version);
            Assert.Equal("create table", migrations[1].Description);
            Assert.Equal(2, migrations[2].Version);
            Assert.Equal("add data", migrations[2].Description);
            Assert.Equal(3, migrations[3].Version);
            Assert.Equal("alter table", migrations[3].Description);
        }

        [Fact]
        public async Task ApplyMigrationsAsync_AppliesPendingMigrations_AndRecordsThem()
        {
            // Arrange
            CreateScriptFile("001_initial.cql", "CREATE TABLE tbl1 (id int PRIMARY KEY);");
            CreateScriptFile("002_add_column.cql", "ALTER TABLE tbl1 ADD data text;");

            var runner = new CassandraMigrationRunner(_mockCassandraService.Object, _migrationConfig, _mockLogger.Object);

            // Mock GetAppliedMigrationsAsync to return none initially
            var mockEmptyRowSet = new Mock<RowSet>();
            mockEmptyRowSet.As<IEnumerable<Row>>().Setup(rs => rs.GetEnumerator()).Returns(new List<Row>().GetEnumerator());
            _mockCassandraService.Setup(s => s.ExecuteAsync($"SELECT version, applied_on, description FROM {_migrationConfig.TableName}", null, null, It.IsAny<object[]>()))
                .ReturnsAsync(mockEmptyRowSet.Object);

            // Capture executed CQL statements
            var executedStatements = new List<string>();
            _mockCassandraService.Setup(s => s.ExecuteAsync(It.IsAny<string>(), null, null, It.IsAny<object[]>()))
                .Callback<string, ConsistencyLevel?, ConsistencyLevel?, object[]>((cql, cl, scl, p) => executedStatements.Add(cql))
                .ReturnsAsync(new RowSet());

            // Act
            await runner.ApplyMigrationsAsync();

            // Assert
            // 1. EnsureSchemaTable, 2. GetApplied (empty), 3. Script 001, 4. Insert 001, 5. Script 002, 6. Insert 002
            Assert.Contains(executedStatements, cql => cql.Contains("CREATE TABLE tbl1"));
            Assert.Contains(executedStatements, cql => cql.Contains("ALTER TABLE tbl1 ADD data text"));

            var expectedInsertCql = $"INSERT INTO {_migrationConfig.TableName} (version, applied_on, description) VALUES (?, ?, ?)";
            Assert.Contains(executedStatements, cql => cql == expectedInsertCql); // Called twice

            // Verify that ExecuteAsync was called for each script and for each insert into schema_migrations
            // The specific number of calls:
            // 1 for EnsureSchemaMigrationsTableAsync
            // 1 for GetAppliedMigrationsAsync
            // 1 for script 001
            // 1 for inserting 001 record
            // 1 for script 002
            // 1 for inserting 002 record
            _mockCassandraService.Verify(s => s.ExecuteAsync(It.IsAny<string>(), null, null, It.IsAny<object[]>()), Times.Exactly(6));
        }

        [Fact]
        public async Task ApplyMigrationsAsync_SkipsAppliedMigrations()
        {
            // Arrange
            CreateScriptFile("001_initial.cql", "CREATE TABLE tbl1 (id int PRIMARY KEY);"); // Already applied
            CreateScriptFile("002_new_script.cql", "ALTER TABLE tbl1 ADD data text;");     // New

            var runner = new CassandraMigrationRunner(_mockCassandraService.Object, _migrationConfig, _mockLogger.Object);

            // Mock GetAppliedMigrationsAsync to return "001" as applied
            var mockRow = new Mock<Row>();
            mockRow.Setup(r => r.GetValue<string>("version")).Returns("001");
            mockRow.Setup(r => r.GetValue<DateTimeOffset>("applied_on")).Returns(DateTimeOffset.UtcNow);
            mockRow.Setup(r => r.GetValue<string>("description")).Returns("initial");

            var colDefs = new ColumnDefinitions(new Column[] {
                new Column { Name = "version", TypeCode = ColumnTypeCode.Text, Index = 0},
                new Column { Name = "applied_on", TypeCode = ColumnTypeCode.Timestamp, Index = 1},
                new Column { Name = "description", TypeCode = ColumnTypeCode.Text, Index = 2}
            });
            var mockAppliedRowSet = new RowSet(new ExecutionInfo(), new Row[] { mockRow.Object }, colDefs, null);

            _mockCassandraService.Setup(s => s.ExecuteAsync($"SELECT version, applied_on, description FROM {_migrationConfig.TableName}", null, null, It.IsAny<object[]>()))
                .ReturnsAsync(mockAppliedRowSet);

            var executedStatements = new List<string>();
            _mockCassandraService.Setup(s => s.ExecuteAsync(It.IsAny<string>(), null, null, It.IsAny<object[]>()))
                .Callback<string, ConsistencyLevel?, ConsistencyLevel?, object[]>((cql, cl, scl, p) => {
                    // Don't add the SELECT from schema_migrations to this list for easier assertion
                    if (!cql.StartsWith("SELECT version, applied_on, description")) executedStatements.Add(cql);
                 })
                .ReturnsAsync(new RowSet());


            // Act
            await runner.ApplyMigrationsAsync();

            // Assert
            Assert.DoesNotContain(executedStatements, cql => cql.Contains("CREATE TABLE tbl1")); // Script 001 should be skipped
            Assert.Contains(executedStatements, cql => cql.Contains("ALTER TABLE tbl1 ADD data text"));   // Script 002 should be applied

            var expectedInsertCql = $"INSERT INTO {_migrationConfig.TableName} (version, applied_on, description) VALUES (?, ?, ?)";
            // Verify that insert for 002 was called, but not for 001
            _mockCassandraService.Verify(s => s.ExecuteAsync(expectedInsertCql, null, null, It.Is<object[]>(p => (string)p[0] == "002")), Times.Once);
            _mockCassandraService.Verify(s => s.ExecuteAsync(expectedInsertCql, null, null, It.Is<object[]>(p => (string)p[0] == "001")), Times.Never);
        }

        [Fact]
        public async Task ApplyMigrationsAsync_HandlesMultiStatementScripts()
        {
            // Arrange
            CreateScriptFile("001_multi.cql", "CREATE TABLE multi1 (id int PRIMARY KEY);\nCREATE TABLE multi2 (id int PRIMARY KEY); \n -- a comment \n ALTER TABLE multi1 ADD value text;");
            var runner = new CassandraMigrationRunner(_mockCassandraService.Object, _migrationConfig, _mockLogger.Object);

            var mockEmptyRowSet = new Mock<RowSet>();
            mockEmptyRowSet.As<IEnumerable<Row>>().Setup(rs => rs.GetEnumerator()).Returns(new List<Row>().GetEnumerator());
            _mockCassandraService.Setup(s => s.ExecuteAsync($"SELECT version, applied_on, description FROM {_migrationConfig.TableName}", null, null, It.IsAny<object[]>()))
                .ReturnsAsync(mockEmptyRowSet.Object);

            var executedStatements = new List<string>();
             _mockCassandraService.Setup(s => s.ExecuteAsync(It.IsAny<string>(), null, null, It.IsAny<object[]>()))
                .Callback<string, ConsistencyLevel?, ConsistencyLevel?, object[]>((cql, cl, scl, p) => executedStatements.Add(cql))
                .ReturnsAsync(new RowSet());

            // Act
            await runner.ApplyMigrationsAsync();

            // Assert
            Assert.Contains(executedStatements, cql => cql.Trim() == "CREATE TABLE multi1 (id int PRIMARY KEY);");
            Assert.Contains(executedStatements, cql => cql.Trim() == "CREATE TABLE multi2 (id int PRIMARY KEY);");
            Assert.Contains(executedStatements, cql => cql.Trim() == "ALTER TABLE multi1 ADD value text;");
            Assert.Equal(3 + 2, executedStatements.Count); // 3 script stmts + EnsureSchema + Insert record
        }
    }
}
