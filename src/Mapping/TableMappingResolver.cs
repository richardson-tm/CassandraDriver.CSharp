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

                if (ignoreAttribute != null && (partitionKeyAttribute != null || clusteringKeyAttribute != null))
                {
                    throw new InvalidOperationException($"Property {propertyInfo.Name} on type {entityType.FullName} cannot be both a key and ignored.");
                }

                if (computedAttribute != null && (partitionKeyAttribute != null || clusteringKeyAttribute != null))
                {
                    throw new InvalidOperationException($"Property {propertyInfo.Name} on type {entityType.FullName} cannot be both a key and computed.");
                }

                properties.Add(new PropertyMappingInfo(
                    propertyInfo,
                    columnName,
                    partitionKeyAttribute != null,
                    partitionKeyAttribute?.Order ?? 0,
                    clusteringKeyAttribute != null,
                    clusteringKeyAttribute?.Order ?? 0,
                    ignoreAttribute != null,
                    computedAttribute != null,
                    computedAttribute?.Expression
                ));
            }

            return new TableMappingInfo(tableName, entityType, properties);
        }
    }
}
