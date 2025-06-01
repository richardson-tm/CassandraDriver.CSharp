using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Cassandra;
using CassandraDriver.Mapping;
using CassandraDriver.Mapping.Attributes;
using CassandraDriver.Services;
using CassandraDriver.Resilience; // For ResilienceOptions
using Moq;
using Xunit;
using System.Collections.Generic;

// Assuming TestModel from CassandraDriver.Tests.Queries is accessible
using CassandraDriver.Tests.Queries;

namespace CassandraDriver.Tests.Services
{
    // Test-specific entity for CassandraMapperServiceTests to avoid conflicts
    // and to precisely define mapping for these tests.
    [Table("mapped_entities")]
    public class MappedEntity
    {
        [PartitionKey]
        [Column("entity_key")]
        public Guid Key { get; set; }

        [Column("description")]
        public string? Description { get; set; }

        [Column("value_count")]
        public int ValueCount { get; set; }

        [Ignore]
        public string? NotMapped { get; set; }

        // A property that is not a key, for update tests
        [Column("last_updated")]
        public DateTimeOffset? LastUpdated { get; set; }
    }


    public class CassandraMapperServiceTests
    {
        private readonly Mock<TableMappingResolver> _mockMappingResolver;
        private readonly Mock<CassandraService> _mockCassandraService; // CassandraService is concrete, so mocking it directly
        private readonly CassandraMapperService _mapperService;
        private readonly TableMappingInfo _mappedEntityTableInfo;

        public CassandraMapperServiceTests()
        {
            _mockMappingResolver = new Mock<TableMappingResolver>();

            // CassandraService has many dependencies in its constructor.
            // For these unit tests, we're testing CassandraMapperService, so we mock CassandraService itself.
            // We don't need a fully functional CassandraService, just its relevant methods for CRUD ops.
            // The actual constructor of CassandraService is complex, so using loose mocks.
            _mockCassandraService = new Mock<CassandraService>(MockBehavior.Loose,
                null, null, null, null /* IOptions, ILogger, TableMappingResolver, ILoggerFactory */);

            _mapperService = new CassandraMapperService(_mockMappingResolver.Object, _mockCassandraService.Object);

            // Setup default mapping for MappedEntity
            var properties = new List<PropertyMappingInfo>
            {
                new PropertyMappingInfo(typeof(MappedEntity).GetProperty("Key")!, "entity_key", true, 0, false, 0, false, false, null, false, null, "uuid"),
                new PropertyMappingInfo(typeof(MappedEntity).GetProperty("Description")!, "description", false, 0, false, 0, false, false, null, false, null, "text"),
                new PropertyMappingInfo(typeof(MappedEntity).GetProperty("ValueCount")!, "value_count", false, 0, false, 0, false, false, null, false, null, "int"),
                new PropertyMappingInfo(typeof(MappedEntity).GetProperty("LastUpdated")!, "last_updated", false, 0, false, 0, false, false, null, false, null, "timestamp"),
                new PropertyMappingInfo(typeof(MappedEntity).GetProperty("NotMapped")!, "notmapped", false, 0, false, 0, true, false, null, false, null, "text")
            };
            _mappedEntityTableInfo = new TableMappingInfo("mapped_entities", typeof(MappedEntity), properties);

            _mockMappingResolver.Setup(r => r.GetMappingInfo(typeof(MappedEntity))).Returns(_mappedEntityTableInfo);
        }

        [Fact]
        public async Task InsertAsync_CallsCassandraServiceInsertAsync_WithCorrectParameters()
        {
            // Arrange
            var entity = new MappedEntity { Key = Guid.NewGuid(), Description = "Test Insert", ValueCount = 10, LastUpdated = DateTimeOffset.UtcNow };
            var expectedConsistency = ConsistencyLevel.Quorum;
            var expectedSerialConsistency = ConsistencyLevel.LocalSerial;
            var resilienceOptions = new ResilienceOptions { Profile = ResiliencePolicyProfile.IdempotentRetry, IsIdempotent = true };

            _mockCassandraService.Setup(s => s.InsertAsync<MappedEntity>(
                entity, false, null, expectedConsistency, expectedSerialConsistency, resilienceOptions))
                .Returns(Task.CompletedTask)
                .Verifiable();

            // Act
            await _mapperService.InsertAsync(entity, false, null, expectedConsistency, expectedSerialConsistency, resilienceOptions);

            // Assert
            _mockCassandraService.Verify(); // Verifies the setup was called
        }

        [Fact]
        public async Task UpdateAsync_CallsCassandraServiceUpdateAsync_WithCorrectParameters()
        {
            // Arrange
            var entity = new MappedEntity { Key = Guid.NewGuid(), Description = "Test Update", ValueCount = 20, LastUpdated = DateTimeOffset.UtcNow };
            var expectedConsistency = ConsistencyLevel.EachQuorum;
            var resilienceOptions = new ResilienceOptions { Profile = ResiliencePolicyProfile.DefaultRetry, IsIdempotent = false };


            _mockCassandraService.Setup(s => s.UpdateAsync<MappedEntity>(
                entity, null, expectedConsistency, false, null, resilienceOptions))
                .Returns(Task.CompletedTask)
                .Verifiable();

            // Act
            await _mapperService.UpdateAsync(entity, null, expectedConsistency, false, null, resilienceOptions);

            // Assert
            _mockCassandraService.Verify();
        }

        [Fact]
        public async Task UpdateAsync_Lwt_CallsCassandraServiceUpdateAsync_WithCorrectSerialConsistency()
        {
            // Arrange
            var entity = new MappedEntity { Key = Guid.NewGuid(), Description = "Test LWT Update", ValueCount = 25, LastUpdated = DateTimeOffset.UtcNow };
            var expectedConsistency = ConsistencyLevel.Quorum;
            var expectedSerialConsistency = ConsistencyLevel.Serial;
            var resilienceOptions = new ResilienceOptions { Profile = ResiliencePolicyProfile.DefaultRetry, IsIdempotent = false };


            _mockCassandraService.Setup(s => s.UpdateAsync<MappedEntity>(
                entity, null, expectedConsistency, true, expectedSerialConsistency, resilienceOptions)) // ifExists = true
                .Returns(Task.CompletedTask)
                .Verifiable();

            // Act
            await _mapperService.UpdateAsync(entity, null, expectedConsistency, true, expectedSerialConsistency, resilienceOptions);

            // Assert
            _mockCassandraService.Verify();
        }


        [Fact]
        public async Task DeleteAsync_Entity_CallsCassandraServiceDeleteAsync_WithCorrectParameters()
        {
            // Arrange
            var entity = new MappedEntity { Key = Guid.NewGuid(), Description = "Test Delete" };
            var expectedConsistency = ConsistencyLevel.One;
            var resilienceOptions = new ResilienceOptions { Profile = ResiliencePolicyProfile.DefaultRetry, IsIdempotent = false };


             _mockCassandraService.Setup(s => s.DeleteAsync<MappedEntity>(
                entity, expectedConsistency, false, null, resilienceOptions))
                .Returns(Task.CompletedTask)
                .Verifiable();

            // Act
            await _mapperService.DeleteAsync(entity, expectedConsistency, false, null, resilienceOptions);

            // Assert
            _mockCassandraService.Verify();
        }

        [Fact]
        public async Task DeleteAsync_Entity_Lwt_CallsCassandraServiceDeleteAsync_WithCorrectSerialConsistency()
        {
            // Arrange
            var entity = new MappedEntity { Key = Guid.NewGuid(), Description = "Test LWT Delete" };
            var expectedConsistency = ConsistencyLevel.Quorum;
            var expectedSerialConsistency = ConsistencyLevel.Serial;
            var resilienceOptions = new ResilienceOptions { Profile = ResiliencePolicyProfile.DefaultRetry, IsIdempotent = false };


             _mockCassandraService.Setup(s => s.DeleteAsync<MappedEntity>(
                entity, expectedConsistency, true, expectedSerialConsistency, resilienceOptions)) // ifExists = true
                .Returns(Task.CompletedTask)
                .Verifiable();

            // Act
            await _mapperService.DeleteAsync(entity, expectedConsistency, true, expectedSerialConsistency, resilienceOptions);

            // Assert
            _mockCassandraService.Verify();
        }
    }
}
