using System;
using System.Collections.Generic;
using CassandraDriver.Mapping;
using CassandraDriver.Mapping.Attributes;
using CassandraDriver.Schema;
using Xunit;
using System.Linq.Expressions;

namespace CassandraDriver.Tests.Schema
{
    // Sample Entities for Schema Generation Tests
    [Table("simple_entities")]
    public class SimpleEntity
    {
        [PartitionKey] public Guid Id { get; set; }
        [Column("name")] public string? Name { get; set; }
        public int Age { get; set; } // Implicit column name "age"
        public DateTimeOffset CreatedAt { get; set; } // Implicit "createdat"
    }

    [Table("composite_pk_entities")]
    public class CompositePkEntity
    {
        [PartitionKey(0)] public int PartKey1 { get; set; }
        [PartitionKey(1)] public string? PartKey2 { get; set; }
        [ClusteringKey(0)] public DateTime Timestamp { get; set; } // Implicit "timestamp"
        // Example for clustering order - needs attribute enhancement in real code
        // [ClusteringKey(1, Order = KeySortOrder.Descending)] public Guid EventId { get; set; }
        [ClusteringKey(1)] public Guid EventId { get; set; } // Implicit "eventid"
        public double Value { get; set; }
    }

    [Table("all_types_entities")]
    public class AllTypesEntity
    {
        [PartitionKey] public int Id { get; set; }
        public string? TextCol { get; set; }
        public long BigintCol { get; set; }
        public Guid UuidCol { get; set; }
        public DateTime TimestampCol { get; set; }
        public DateTimeOffset DtOffsetCol { get; set; }
        public bool BooleanCol { get; set; }
        public double DoubleCol { get; set; }
        public float FloatCol { get; set; }
        public decimal DecimalCol { get; set; }
        public byte[]? BlobCol { get; set; }
        public TestEnum EnumCol { get; set; } // Enums mapped to text by default
        public List<string>? ListTextCol { get; set; }
        public Dictionary<int, string>? MapIntTextCol { get; set; }
        public List<int>? NullableListIntCol { get; set; } // Test Nullable generic
        public int? NullableIntCol { get; set; }
    }
    public enum TestEnum { Val1, Val2 }


    public class SchemaGeneratorTests
    {
        private readonly TableMappingResolver _resolver = new TableMappingResolver();

        [Fact]
        public void GetCreateTableCql_SimpleKey_GeneratesCorrectCql()
        {
            // Act
            var cql = SchemaGenerator.GetCreateTableCql<SimpleEntity>(_resolver);

            // Assert
            string expected = "CREATE TABLE IF NOT EXISTS \"simple_entities\" (\"id\" uuid, \"name\" text, \"age\" int, \"createdat\" timestamp, PRIMARY KEY (\"id\"));";
            Assert.Equal(expected, cql);
        }

        [Fact]
        public void GetCreateTableCql_CompositePartitionKey_AndClusteringKeys()
        {
            // Act
            var cql = SchemaGenerator.GetCreateTableCql<CompositePkEntity>(_resolver);

            // Assert
            // Note: Clustering order part is illustrative as attribute isn't fully implemented for order yet.
            string expected = "CREATE TABLE IF NOT EXISTS \"composite_pk_entities\" (\"partkey1\" int, \"partkey2\" text, \"timestamp\" timestamp, \"eventid\" uuid, \"value\" double, PRIMARY KEY ((\"partkey1\", \"partkey2\"), \"timestamp\", \"eventid\"));";
            Assert.Equal(expected, cql);
        }

        [Fact]
        public void GetCreateTableCql_NoIfNotExists()
        {
            var cql = SchemaGenerator.GetCreateTableCql<SimpleEntity>(_resolver, ifNotExists: false);
            Assert.StartsWith("CREATE TABLE \"simple_entities\"", cql);
            Assert.DoesNotContain("IF NOT EXISTS", cql);
        }


        [Fact]
        public void GetCreateTableCql_MapsAllSupportedTypes()
        {
            // Act
            var cql = SchemaGenerator.GetCreateTableCql<AllTypesEntity>(_resolver);

            // Assert
            // Check for correct type mappings
            Assert.Contains("\"id\" int", cql);
            Assert.Contains("\"textcol\" text", cql);
            Assert.Contains("\"bigintcol\" bigint", cql);
            Assert.Contains("\"uuidcol\" uuid", cql);
            Assert.Contains("\"timestampcol\" timestamp", cql);
            Assert.Contains("\"dtoffsetcol\" timestamp", cql);
            Assert.Contains("\"booleancol\" boolean", cql);
            Assert.Contains("\"doublecol\" double", cql);
            Assert.Contains("\"floatcol\" float", cql);
            Assert.Contains("\"decimalcol\" decimal", cql);
            Assert.Contains("\"blobcol\" blob", cql);
            Assert.Contains("\"enumcol\" text", cql); // Default for enums
            Assert.Contains("\"listtextcol\" list<text>", cql);
            Assert.Contains("\"mapinttextcol\" map<int, text>", cql);
            Assert.Contains("\"nullablelistintcol\" list<int>", cql);
            Assert.Contains("\"nullableintcol\" int", cql);
            Assert.Contains("PRIMARY KEY (\"id\")", cql);
        }

        [Fact]
        public void GetCreateIndexCql_GeneratesCorrectCql()
        {
            // Act
            Expression<Func<SimpleEntity, object?>> propertyExp = e => e.Name;
            var cql = SchemaGenerator.GetCreateIndexCql<SimpleEntity>(_resolver, propertyExp);

            // Assert
            string expected = "CREATE INDEX IF NOT EXISTS \"simple_entities_name_idx\" ON \"simple_entities\" (\"name\");";
            Assert.Equal(expected, cql);
        }

        [Fact]
        public void GetCreateIndexCql_CustomName_NoIfNotExists()
        {
            Expression<Func<SimpleEntity, object?>> propertyExp = e => e.Age;
            var cql = SchemaGenerator.GetCreateIndexCql<SimpleEntity>(_resolver, propertyExp, "my_custom_age_idx", ifNotExists: false);
            Assert.Equal("CREATE INDEX \"my_custom_age_idx\" ON \"simple_entities\" (\"age\");", cql);
        }


        [Fact]
        public void GetDropTableCql_GeneratesCorrectCql()
        {
            // Act
            var cql = SchemaGenerator.GetDropTableCql<SimpleEntity>(_resolver);

            // Assert
            Assert.Equal("DROP TABLE IF EXISTS \"simple_entities\";", cql);
        }

        [Fact]
        public void GetDropTableCql_NoIfExists()
        {
            var cql = SchemaGenerator.GetDropTableCql<SimpleEntity>(_resolver, ifExists: false);
            Assert.Equal("DROP TABLE \"simple_entities\";", cql);
        }


        [Fact]
        public void GetDropIndexCql_GeneratesCorrectCql()
        {
            // Act
            var cql = SchemaGenerator.GetDropIndexCql("my_index_name");

            // Assert
            Assert.Equal("DROP INDEX IF EXISTS \"my_index_name\";", cql);
        }

        [Fact]
        public void GetDropIndexCql_NoIfExists()
        {
            var cql = SchemaGenerator.GetDropIndexCql("my_index_name", ifExists: false);
            Assert.Equal("DROP INDEX \"my_index_name\";", cql);
        }

        [Fact]
        public void GetCassandraType_ThrowsForUnsupportedType()
        {
            // Arrange
            var resolver = new TableMappingResolver(); // Not directly used by GetCassandraType but good for context

            // Act & Assert
            Assert.Throws<NotSupportedException>(() => SchemaGenerator.GetCreateTableCql<UnsupportedTypeEntity>(resolver));
        }

        [Table("unsupported_type_entities")]
        public class UnsupportedTypeEntity
        {
            [PartitionKey]
            public int Id { get; set; }
            public System.Drawing.Point UnsupportedTestType { get; set; } // System.Drawing.Point is not supported
        }
    }
}
