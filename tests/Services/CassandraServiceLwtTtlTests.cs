using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using CassandraDriver.Configuration;
using CassandraDriver.Mapping;
using CassandraDriver.Mapping.Attributes;
using CassandraDriver.Results;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace CassandraDriver.Tests.Services
{
    [Table("lwt_test_entities")]
    public class LwtTestEntity
    {
        [PartitionKey] public int Id { get; set; }
        [Column("value")] public string? Value { get; set; }
        [Column("counter_val")] public long CounterVal { get; set; } // For counter updates if needed
    }

    public class CassandraServiceLwtTtlTests : IDisposable
    {
        private readonly Mock<IOptions<CassandraConfiguration>> _mockOptions;
        private readonly Mock<ILogger<CassandraService>> _mockLogger;
        private readonly Mock<ILoggerFactory> _mockLoggerFactory;
        private readonly CassandraConfiguration _configuration;
        private readonly TableMappingResolver _mappingResolver;
        private readonly Mock<ISession> _mockSession;
        private CassandraService _service;

        public CassandraServiceLwtTtlTests()
        {
            _mockOptions = new Mock<IOptions<CassandraConfiguration>>();
            _mockLogger = new Mock<ILogger<CassandraService>>();
            _mockLoggerFactory = new Mock<ILoggerFactory>();
            _configuration = new CassandraConfiguration();
            _mockOptions.Setup(x => x.Value).Returns(_configuration);
            _mappingResolver = new TableMappingResolver(); // Using real resolver

            _mockLoggerFactory.Setup(x => x.CreateLogger(It.IsAny<string>()))
                              .Returns(new Mock<ILogger>().Object);

            _mockSession = new Mock<ISession>();

            // Setup CassandraService with a way to inject the mocked ISession
            // The constructor of CassandraService takes ILoggerFactory now
            _service = new TestableCassandraService(_mockOptions.Object, _mockLogger.Object, _mappingResolver, _mockLoggerFactory.Object, _mockSession.Object);
        }

        // Testable service to inject mock session
        private class TestableCassandraService : CassandraService
        {
            private readonly ISession _sessionOverride;
            public TestableCassandraService(
                IOptions<CassandraConfiguration> configuration,
                ILogger<CassandraService> logger,
                TableMappingResolver mappingResolver,
                ILoggerFactory loggerFactory, // Added
                ISession sessionOverride)
                : base(configuration, logger, mappingResolver, loggerFactory) // Pass loggerFactory
            {
                _sessionOverride = sessionOverride;
            }
            public override ISession Session => _sessionOverride;
        }


        public void Dispose() { }

        private Mock<Row> CreateLwtAppliedRow(bool applied)
        {
            var mockRow = new Mock<Row>();
            mockRow.Setup(r => r.ContainsColumn("[applied]")).Returns(true);
            mockRow.Setup(r => r.GetValue<bool>("[applied]")).Returns(applied);
            return mockRow;
        }

        private Mock<Row> CreateLwtNotAppliedDataRow(int id, string value)
        {
            var mockRow = CreateLwtAppliedRow(false); // [applied] = false
            mockRow.Setup(r => r.ContainsColumn("id")).Returns(true);
            mockRow.Setup(r => r.GetValue<int>("id")).Returns(id);
            mockRow.Setup(r => r.ContainsColumn("value")).Returns(true);
            mockRow.Setup(r => r.GetValue<string>("value")).Returns(value);
            return mockRow;
        }


        [Fact]
        public async Task InsertAsync_IfNotExists_Applied()
        {
            // Arrange
            var entity = new LwtTestEntity { Id = 1, Value = "Test" };
            var mockRowSet = new Mock<RowSet>();
            var appliedRow = CreateLwtAppliedRow(true).Object;
            var colDefs = new ColumnDefinitions(new Column[] { new Column { Name = "[applied]", Index = 0, TypeCode = ColumnTypeCode.Boolean } });
            mockRowSet.As<IEnumerable<Row>>().Setup(rs => rs.GetEnumerator()).Returns(new List<Row> { appliedRow }.GetEnumerator());
            mockRowSet.Setup(rs => rs.FirstOrDefault()).Returns(appliedRow);
            mockRowSet.SetupGet(rs => rs.Info).Returns(new ExecutionInfo());
            mockRowSet.SetupGet(rs => rs.Columns).Returns(colDefs);


            _mockSession.Setup(s => s.PrepareAsync(It.Is<string>(cql => cql.Contains("IF NOT EXISTS"))))
                        .ReturnsAsync(new Mock<PreparedStatement>().Object);
            _mockSession.Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                        .ReturnsAsync(mockRowSet.Object);

            var mockPs = new Mock<PreparedStatement>();
            mockPs.Setup(ps => ps.Bind(It.IsAny<object[]>())).Returns(new Mock<BoundStatement>().Object);
            _mockSession.Setup(s => s.PrepareAsync(It.Is<string>(cql => cql.StartsWith("INSERT INTO \"lwt_test_entities\"")))).ReturnsAsync(mockPs.Object);


            // Act
            var result = await _service.InsertAsync(entity, ifNotExists: true);

            // Assert
            Assert.True(result.Applied);
            Assert.Same(entity, result.Entity); // For applied inserts, we return the original entity
            _mockSession.Verify(s => s.PrepareAsync(It.Is<string>(cql => cql.Contains("IF NOT EXISTS"))), Times.Once);
        }

        [Fact]
        public async Task InsertAsync_IfNotExists_NotApplied_ReturnsOriginalEntity()
        {
            // Arrange
            var entity = new LwtTestEntity { Id = 1, Value = "Test" };
            var existingEntityData = new LwtTestEntity { Id = 1, Value = "Existing" }; // This might not be returned by INSERT LWT

            var mockRowSet = new Mock<RowSet>();
            var notAppliedRow = CreateLwtAppliedRow(false).Object; // [applied] = false
            // For INSERT ... IF NOT EXISTS, if it's not applied, Cassandra might not return the existing row's data,
            // just [applied]=false. Some drivers/versions might return the conflicting row.
            // Let's assume it just returns [applied]=false and no other columns for INSERT.
            var colDefs = new ColumnDefinitions(new Column[] { new Column { Name = "[applied]", Index = 0, TypeCode = ColumnTypeCode.Boolean } });
            mockRowSet.As<IEnumerable<Row>>().Setup(rs => rs.GetEnumerator()).Returns(new List<Row> { notAppliedRow }.GetEnumerator());
            mockRowSet.Setup(rs => rs.FirstOrDefault()).Returns(notAppliedRow);
            mockRowSet.SetupGet(rs => rs.Info).Returns(new ExecutionInfo());
            mockRowSet.SetupGet(rs => rs.Columns).Returns(colDefs);


            var mockPs = new Mock<PreparedStatement>();
            mockPs.Setup(ps => ps.Bind(It.IsAny<object[]>())).Returns(new Mock<BoundStatement>().Object);
            _mockSession.Setup(s => s.PrepareAsync(It.Is<string>(cql => cql.StartsWith("INSERT INTO \"lwt_test_entities\"") && cql.Contains("IF NOT EXISTS"))))
                        .ReturnsAsync(mockPs.Object);
            _mockSession.Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                        .ReturnsAsync(mockRowSet.Object);

            // Act
            var result = await _service.InsertAsync(entity, ifNotExists: true);

            // Assert
            Assert.False(result.Applied);
            // For INSERT IF NOT EXISTS, if not applied, the Entity in LwtResult should be the original entity passed in.
            // Cassandra doesn't typically return the "existing" entity data on a failed INSERT LWT in the same way it does for UPDATE.
            Assert.Same(entity, result.Entity);
        }


        [Fact]
        public async Task UpdateAsync_IfCondition_Applied()
        {
            // Arrange
            var entity = new LwtTestEntity { Id = 1, Value = "UpdatedValue" };
            var condition = "\"value\" = 'OldValue'";

            var mockRowSet = new Mock<RowSet>();
            var appliedRow = CreateLwtAppliedRow(true).Object;
            var colDefs = new ColumnDefinitions(new Column[] { new Column { Name = "[applied]", Index = 0, TypeCode = ColumnTypeCode.Boolean } });
            mockRowSet.As<IEnumerable<Row>>().Setup(rs => rs.GetEnumerator()).Returns(new List<Row> { appliedRow }.GetEnumerator());
            mockRowSet.Setup(rs => rs.FirstOrDefault()).Returns(appliedRow);
            mockRowSet.SetupGet(rs => rs.Info).Returns(new ExecutionInfo());
            mockRowSet.SetupGet(rs => rs.Columns).Returns(colDefs);

            var mockPs = new Mock<PreparedStatement>();
            mockPs.Setup(ps => ps.Bind(It.IsAny<object[]>())).Returns(new Mock<BoundStatement>().Object);
             _mockSession.Setup(s => s.PrepareAsync(It.Is<string>(cql => cql.StartsWith("UPDATE \"lwt_test_entities\"") && cql.Contains($"IF {condition}"))))
                        .ReturnsAsync(mockPs.Object);
            _mockSession.Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                        .ReturnsAsync(mockRowSet.Object);

            // Act
            var result = await _service.UpdateAsync(entity, ifCondition: condition);

            // Assert
            Assert.True(result.Applied);
            Assert.Same(entity, result.Entity); // If applied, return original entity
            _mockSession.Verify(s => s.PrepareAsync(It.Is<string>(cql => cql.Contains($"IF {condition}"))), Times.Once);
        }

        [Fact]
        public async Task UpdateAsync_IfCondition_NotApplied_ReturnsCurrentValues()
        {
            // Arrange
            var entityToUpdate = new LwtTestEntity { Id = 1, Value = "NewAttemptedValue" };
            var actualCurrentValueInDb = "ExistingValueInDB";
            var condition = $"\"value\" = '{actualCurrentValueInDb}'"; // This condition would fail if entityToUpdate.Value is different

            var mockRowSet = new Mock<RowSet>();
            var notAppliedDataRow = CreateLwtNotAppliedDataRow(entityToUpdate.Id, actualCurrentValueInDb).Object;
            var colDefs = new ColumnDefinitions(new Column[] {
                new Column { Name = "[applied]", Index = 0, TypeCode = ColumnTypeCode.Boolean },
                new Column { Name = "id", Index = 1, TypeCode = ColumnTypeCode.Int },
                new Column { Name = "value", Index = 2, TypeCode = ColumnTypeCode.Text }
            });
            mockRowSet.As<IEnumerable<Row>>().Setup(rs => rs.GetEnumerator()).Returns(new List<Row> { notAppliedDataRow }.GetEnumerator());
            mockRowSet.Setup(rs => rs.FirstOrDefault()).Returns(notAppliedDataRow);
            mockRowSet.SetupGet(rs => rs.Info).Returns(new ExecutionInfo());
            mockRowSet.SetupGet(rs => rs.Columns).Returns(colDefs);


            var mockPs = new Mock<PreparedStatement>();
            mockPs.Setup(ps => ps.Bind(It.IsAny<object[]>())).Returns(new Mock<BoundStatement>().Object);
            _mockSession.Setup(s => s.PrepareAsync(It.Is<string>(cql => cql.StartsWith("UPDATE \"lwt_test_entities\"") && cql.Contains($"IF {condition}"))))
                        .ReturnsAsync(mockPs.Object);
            _mockSession.Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                        .ReturnsAsync(mockRowSet.Object);

            // Act
            var result = await _service.UpdateAsync(entityToUpdate, ifCondition: condition);

            // Assert
            Assert.False(result.Applied);
            Assert.NotNull(result.Entity);
            Assert.Equal(entityToUpdate.Id, result.Entity!.Id); // ID should match
            Assert.Equal(actualCurrentValueInDb, result.Entity.Value); // Value should be the one from DB
        }

        [Fact]
        public async Task InsertAsync_WithTtlAndIfNotExists_CqlOrder()
        {
            // Arrange
            var entity = new LwtTestEntity { Id = 1, Value = "TTL Test" };
            int ttl = 3600;

            var mockRowSet = new Mock<RowSet>(); // Empty, just to make ExecuteAsync happy
            var appliedRow = CreateLwtAppliedRow(true).Object;
            mockRowSet.As<IEnumerable<Row>>().Setup(rs => rs.GetEnumerator()).Returns(new List<Row> { appliedRow }.GetEnumerator());
             mockRowSet.Setup(rs => rs.FirstOrDefault()).Returns(appliedRow);


            var mockPs = new Mock<PreparedStatement>();
            mockPs.Setup(ps => ps.Bind(It.IsAny<object[]>())).Returns(new Mock<BoundStatement>().Object);
             _mockSession.Setup(s => s.PrepareAsync(It.Is<string>(cql =>
                            cql.StartsWith("INSERT INTO \"lwt_test_entities\"") &&
                            cql.Contains("IF NOT EXISTS USING TTL ?")))) // Corrected order
                        .ReturnsAsync(mockPs.Object).Verifiable();
            _mockSession.Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>())).ReturnsAsync(mockRowSet.Object);


            // Act
            var result = await _service.InsertAsync(entity, ifNotExists: true, ttl: ttl);

            // Assert
            Assert.True(result.Applied);
            _mockSession.Verify();
        }

        [Fact]
        public async Task UpdateAsync_WithTtlAndIfCondition_CqlOrder()
        {
            // Arrange
            var entity = new LwtTestEntity { Id = 1, Value = "Update TTL Test" };
            int ttl = 1800;
            var condition = "\"value\" = 'OldValue'";

            var mockRowSet = new Mock<RowSet>();
            var appliedRow = CreateLwtAppliedRow(true).Object;
            mockRowSet.As<IEnumerable<Row>>().Setup(rs => rs.GetEnumerator()).Returns(new List<Row> { appliedRow }.GetEnumerator());
            mockRowSet.Setup(rs => rs.FirstOrDefault()).Returns(appliedRow);


            var mockPs = new Mock<PreparedStatement>();
            mockPs.Setup(ps => ps.Bind(It.IsAny<object[]>())).Returns(new Mock<BoundStatement>().Object);
            _mockSession.Setup(s => s.PrepareAsync(It.Is<string>(cql =>
                            cql.StartsWith("UPDATE \"lwt_test_entities\" USING TTL ? SET") &&
                            cql.EndsWith($"IF {condition}"))))
                        .ReturnsAsync(mockPs.Object).Verifiable();
            _mockSession.Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>())).ReturnsAsync(mockRowSet.Object);

            // Act
            var result = await _service.UpdateAsync(entity, ttl: ttl, ifCondition: condition);

            // Assert
            Assert.True(result.Applied);
            _mockSession.Verify();
        }
    }
}
