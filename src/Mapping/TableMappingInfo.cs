using System;
using System.Collections.Generic;
using System.Linq;

namespace CassandraDriver.Mapping
{
    public class TableMappingInfo
    {
        public string TableName { get; }
        public Type EntityType { get; }
        public IReadOnlyList<PropertyMappingInfo> Properties { get; }
        public IReadOnlyList<PropertyMappingInfo> PartitionKeys { get; }
        public IReadOnlyList<PropertyMappingInfo> ClusteringKeys { get; }
        public IReadOnlyList<PropertyMappingInfo> Columns { get; } // All non-ignored, non-computed properties

        public TableMappingInfo(string tableName, Type entityType, List<PropertyMappingInfo> properties)
        {
            TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            Properties = properties ?? throw new ArgumentNullException(nameof(properties));

            PartitionKeys = properties.Where(p => p.IsPartitionKey).OrderBy(p => p.PartitionKeyOrder).ToList().AsReadOnly();
            ClusteringKeys = properties.Where(p => p.IsClusteringKey).OrderBy(p => p.ClusteringKeyOrder).ToList().AsReadOnly();

            // Columns for insert/update/select (non-ignored, non-computed for insert/update, potentially computed for select)
            Columns = properties.Where(p => !p.IsIgnored).ToList().AsReadOnly();

            if (!PartitionKeys.Any())
            {
                throw new InvalidOperationException($"Entity type {entityType.FullName} must have at least one partition key defined.");
            }
        }
    }
}
