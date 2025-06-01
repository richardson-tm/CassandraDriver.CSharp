using System;
using System.Collections.Generic;
using System.Linq;
using CassandraDriver.Mapping;
using CassandraDriver.Mapping.Attributes;
using Xunit;

namespace CassandraDriver.Tests.Mapping
{
    // --- Test Models for TableMappingResolverTests ---

    // Convention-based
    public class ConventionEntity
    {
        public Guid Id { get; set; } // Convention: "id"
        public string FullName { get; set; } // Convention: "fullname"
        public DateTime DateOfBirth { get; set; } // Convention: "dateofbirth" -> Cassandra: timestamp
        public List<string> Tags { get; set; } // Convention: "tags" -> Cassandra: list<text>
        public Dictionary<int, string> Preferences { get; set; } // Convention: "preferences" -> Cassandra: map<int, text>
    }

    // Attribute-based
    [Table("custom_entities")]
    public class AttributeEntity
    {
        [PartitionKey]
        [Column("entity_id")]
        public Guid EntityId { get; set; }

        [Column("custom_name", TypeName = "varchar")]
        public string Name { get; set; }

        [Ignore]
        public string TempData { get; set; }

        [Computed("ttl(data)")]
        [Column("data_ttl")]
        public int DataTtl { get; private set; }
    }

    // UDT Test Models
    public class AddressUdt
    {
        public string Street { get; set; }
        public string City { get; set; }
        [Column("zip_code")]
        public string ZipCode { get; set; }
    }

    [Table("entities_with_udt")]
    public class EntityWithUdt
    {
        [PartitionKey]
        public Guid Id { get; set; }

        [Udt("address")] // This property is of a UDT type named "address" in Cassandra
        [Column("main_address")]
        public AddressUdt MainAddress { get; set; } // C# type is AddressUdt

        [Udt("contact_numbers")] // UDT attribute
        [Column("phone_numbers", TypeName = "list<frozen<text>>")] // Explicit Cassandra type
        public List<string> PhoneNumbers { get; set; }
    }

    // Model for specific type conversions
    public class TypeConversionEntity
    {
        public Guid Id { get; set; } // uuid
        public DateTimeOffset TimestampOff { get; set; } // timestamp
        public decimal Price { get; set; } // decimal
        public byte[] DataBlob { get; set; } // blob
        public bool IsSet { get; set; } // boolean
        public float FloatVal { get; set; } // float
        public double DoubleVal { get; set; } // double
        public long BigIntVal { get; set; } // bigint
        public int? NullableInt { get; set; } // int (Cassandra doesn't distinguish nullable at type level)
    }


    public class TableMappingResolverTests
    {
        private readonly TableMappingResolver _resolver = new TableMappingResolver();

        [Fact]
        public void GetMappingInfo_ConventionBased_CorrectMapping()
        {
            // Act
            var mappingInfo = _resolver.GetMappingInfo(typeof(ConventionEntity));

            // Assert
            Assert.Equal("conventionentity", mappingInfo.TableName); // Default convention is lowercase class name

            var idMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "Id");
            Assert.Equal("id", idMap.ColumnName); // Default convention is lowercase property name
            Assert.Equal("uuid", idMap.CassandraTypeName);

            var nameMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "FullName");
            Assert.Equal("fullname", nameMap.ColumnName);
            Assert.Equal("text", nameMap.CassandraTypeName);

            var dobMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "DateOfBirth");
            Assert.Equal("dateofbirth", dobMap.ColumnName);
            Assert.Equal("timestamp", dobMap.CassandraTypeName);

            var tagsMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "Tags");
            Assert.Equal("tags", tagsMap.ColumnName);
            Assert.Equal("list<text>", tagsMap.CassandraTypeName);

            var prefsMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "Preferences");
            Assert.Equal("preferences", prefsMap.ColumnName);
            Assert.Equal("map<int, text>", prefsMap.CassandraTypeName);
        }

        [Fact]
        public void GetMappingInfo_AttributeBased_OverridesConvention()
        {
            // Act
            var mappingInfo = _resolver.GetMappingInfo(typeof(AttributeEntity));

            // Assert
            Assert.Equal("custom_entities", mappingInfo.TableName);

            var idMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "EntityId");
            Assert.Equal("entity_id", idMap.ColumnName);
            Assert.True(idMap.IsPartitionKey);

            var nameMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "Name");
            Assert.Equal("custom_name", nameMap.ColumnName);
            Assert.Equal("varchar", nameMap.CassandraTypeName); // TypeName from ColumnAttribute

            var tempDataMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "TempData");
            Assert.True(tempDataMap.IsIgnored);

            var computedMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "DataTtl");
            Assert.True(computedMap.IsComputed);
            Assert.Equal("ttl(data)", computedMap.ComputedExpression);
            Assert.Equal("data_ttl", computedMap.ColumnName);
        }

        [Fact]
        public void GetMappingInfo_UdtMapping_CorrectlyIdentifiesUdt()
        {
            // Act
            var mappingInfo = _resolver.GetMappingInfo(typeof(EntityWithUdt));

            // Assert
            Assert.Equal("entities_with_udt", mappingInfo.TableName);

            var addressMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "MainAddress");
            Assert.True(addressMap.IsUdt);
            Assert.Equal("address", addressMap.UdtName);
            Assert.Equal("main_address", addressMap.ColumnName);
            // The CassandraTypeName for a UDT property itself is the UDT name (possibly frozen based on driver conventions)
            Assert.Equal("address", addressMap.CassandraTypeName);

            // Test UDT property with explicit TypeName override (though UdtAttribute usually defines the Cassandra type)
            var phoneMap = mappingInfo.Properties.First(p => p.PropertyInfo.Name == "PhoneNumbers");
            Assert.True(phoneMap.IsUdt); // UdtAttribute is present
            Assert.Equal("contact_numbers", phoneMap.UdtName);
            Assert.Equal("list<frozen<text>>", phoneMap.CassandraTypeName); // Explicit TypeName takes precedence
            Assert.Equal("phone_numbers", phoneMap.ColumnName);
        }

        [Fact]
        public void GetMappingInfo_SpecificTypeConversions_AreCorrect()
        {
            var mappingInfo = _resolver.GetMappingInfo(typeof(TypeConversionEntity));

            Assert.Equal("uuid", mappingInfo.Properties.First(p=>p.PropertyInfo.Name == "Id").CassandraTypeName);
            Assert.Equal("timestamp", mappingInfo.Properties.First(p=>p.PropertyInfo.Name == "TimestampOff").CassandraTypeName);
            Assert.Equal("decimal", mappingInfo.Properties.First(p=>p.PropertyInfo.Name == "Price").CassandraTypeName);
            Assert.Equal("blob", mappingInfo.Properties.First(p=>p.PropertyInfo.Name == "DataBlob").CassandraTypeName);
            Assert.Equal("boolean", mappingInfo.Properties.First(p=>p.PropertyInfo.Name == "IsSet").CassandraTypeName);
            Assert.Equal("float", mappingInfo.Properties.First(p=>p.PropertyInfo.Name == "FloatVal").CassandraTypeName);
            Assert.Equal("double", mappingInfo.Properties.First(p=>p.PropertyInfo.Name == "DoubleVal").CassandraTypeName);
            Assert.Equal("bigint", mappingInfo.Properties.First(p=>p.PropertyInfo.Name == "BigIntVal").CassandraTypeName);
            Assert.Equal("int", mappingInfo.Properties.First(p=>p.PropertyInfo.Name == "NullableInt").CassandraTypeName);
        }

        // Example of what an IsFrozen property on UdtAttribute might look like, if added:
        // In UdtAttribute.cs: public bool IsFrozen { get; set; } = true; // Default to frozen
        // In TableMappingResolver.cs, when reading UdtAttribute: udtAttribute?.IsFrozen ?? true
        // Then CassandraTypeName for UDT could be: $"{(isFrozen ? "frozen<" : "")}{udtAttribute.UdtName}{(isFrozen ? ">" : "")}"
        // The current UdtAttribute does not have IsFrozen. The test for PhoneNumbers uses explicit TypeName.
    }
}
