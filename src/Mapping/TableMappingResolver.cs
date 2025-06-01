using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using CassandraDriver.Mapping.Attributes;

namespace CassandraDriver.Mapping
{
    public class TableMappingResolver
    {
        private readonly ConcurrentDictionary<Type, TableMappingInfo> _cache = new();

        public TableMappingInfo GetMappingInfo(Type entityType)
        {
            return _cache.GetOrAdd(entityType, CreateMappingInfo);
        }

        private TableMappingInfo CreateMappingInfo(Type entityType)
        {
            var tableAttribute = entityType.GetCustomAttribute<TableAttribute>();
            var tableName = tableAttribute?.Name ?? entityType.Name.ToLowerInvariant(); // Default to lowercase class name

            var properties = new List<PropertyMappingInfo>();
            foreach (var propertyInfo in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!propertyInfo.CanRead || !propertyInfo.CanWrite) // Basic check, might need adjustment
                {
                    continue;
                }

                var columnAttribute = propertyInfo.GetCustomAttribute<ColumnAttribute>();
                var columnName = columnAttribute?.Name ?? propertyInfo.Name.ToLowerInvariant(); // Default to lowercase property name

                var partitionKeyAttribute = propertyInfo.GetCustomAttribute<PartitionKeyAttribute>();
                var clusteringKeyAttribute = propertyInfo.GetCustomAttribute<ClusteringKeyAttribute>();
                var ignoreAttribute = propertyInfo.GetCustomAttribute<IgnoreAttribute>();
                var computedAttribute = propertyInfo.GetCustomAttribute<ComputedAttribute>();
                var udtAttribute = propertyInfo.GetCustomAttribute<UdtAttribute>();

                if (ignoreAttribute != null && (partitionKeyAttribute != null || clusteringKeyAttribute != null))
                {
                    throw new InvalidOperationException($"Property {propertyInfo.Name} on type {entityType.FullName} cannot be both a key and ignored.");
                }

                if (computedAttribute != null && (partitionKeyAttribute != null || clusteringKeyAttribute != null))
                {
                    throw new InvalidOperationException($"Property {propertyInfo.Name} on type {entityType.FullName} cannot be both a key and computed.");
                }

                if (udtAttribute != null && computedAttribute != null)
                {
                     throw new InvalidOperationException($"Property {propertyInfo.Name} on type {entityType.FullName} cannot be both UDT and computed.");
                }

                string cassandraTypeName = GetCassandraTypeName(propertyInfo.PropertyType, columnAttribute?.TypeName, udtAttribute);


                properties.Add(new PropertyMappingInfo(
                    propertyInfo,
                    columnName,
                    partitionKeyAttribute != null,
                    partitionKeyAttribute?.Order ?? 0,
                    clusteringKeyAttribute != null,
                    clusteringKeyAttribute?.Order ?? 0,
                    ignoreAttribute != null,
                    computedAttribute != null,
                    computedAttribute?.Expression,
                    udtAttribute != null,
                    udtAttribute?.UdtName,
                    cassandraTypeName
                ));
            }

            return new TableMappingInfo(tableName, entityType, properties);
        }

        private string GetCassandraTypeName(Type propertyType, string? explicitTypeName, UdtAttribute? udtAttribute)
        {
            if (!string.IsNullOrWhiteSpace(explicitTypeName))
            {
                return explicitTypeName;
            }

            if (udtAttribute != null)
            {
                // For UDTs, the type name is the UDT name itself, often frozen.
                // The exact representation (e.g., frozen<udt_name>) might depend on Cassandra version and usage.
                // Here, we'll store the UDT name, and assume 'frozen' is handled by the query generation if needed.
                return udtAttribute.UdtName;
            }

            // Handle nullable underlying types
            var type = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

            if (type == typeof(Guid)) return "uuid";
            if (type == typeof(DateTime)) return "timestamp";
            if (type == typeof(DateTimeOffset)) return "timestamp";
            if (type == typeof(string)) return "text";
            if (type == typeof(int)) return "int";
            if (type == typeof(long)) return "bigint";
            if (type == typeof(float)) return "float";
            if (type == typeof(double)) return "double";
            if (type == typeof(decimal)) return "decimal";
            if (type == typeof(bool)) return "boolean";
            if (type == typeof(byte[])) return "blob";

            if (type.IsGenericType)
            {
                var genericTypeDef = type.GetGenericTypeDefinition();
                if (genericTypeDef == typeof(Dictionary<,>))
                {
                    var keyType = GetCassandraTypeName(type.GetGenericArguments()[0], null, null);
                    var valueType = GetCassandraTypeName(type.GetGenericArguments()[1], null, null);
                    return $"map<{keyType}, {valueType}>";
                }
                if (genericTypeDef == typeof(List<>) || genericTypeDef == typeof(IEnumerable<>))
                {
                    var elementType = GetCassandraTypeName(type.GetGenericArguments()[0], null, null);
                    return $"list<{elementType}>";
                }
                // Add Set<T> if needed: return $"set<{elementType}>";
            }

            // Default or throw
            // Consider if this should throw an error or default to something like 'blob' or 'text'
            throw new NotSupportedException($"Type {propertyType.FullName} is not supported for Cassandra mapping directly. Specify TypeName in ColumnAttribute or handle as UDT.");
        }
    }
}
