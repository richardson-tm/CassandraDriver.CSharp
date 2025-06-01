using System;
using System.Reflection;
using CassandraDriver.Mapping.Attributes;

namespace CassandraDriver.Mapping
{
    public class PropertyMappingInfo
    {
        public PropertyInfo PropertyInfo { get; }
        public string ColumnName { get; }
        public bool IsPartitionKey { get; }
        public int PartitionKeyOrder { get; }
        public bool IsClusteringKey { get; }
        public int ClusteringKeyOrder { get; }
        public bool IsIgnored { get; }
        public bool IsComputed { get; }
        public string? ComputedExpression { get; }
        public bool IsUdt { get; }
        public string? UdtName { get; }
        public string CassandraTypeName { get; } // Store the target Cassandra type name

        public PropertyMappingInfo(PropertyInfo propertyInfo, string columnName,
            bool isPartitionKey, int partitionKeyOrder,
            bool isClusteringKey, int clusteringKeyOrder,
            bool isIgnored, bool isComputed, string? computedExpression,
            bool isUdt, string? udtName, string cassandraTypeName)
        {
            PropertyInfo = propertyInfo ?? throw new ArgumentNullException(nameof(propertyInfo));
            ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
            CassandraTypeName = cassandraTypeName ?? throw new ArgumentNullException(nameof(cassandraTypeName));
            IsPartitionKey = isPartitionKey;
            PartitionKeyOrder = partitionKeyOrder;
            IsClusteringKey = isClusteringKey;
            ClusteringKeyOrder = clusteringKeyOrder;
            IsIgnored = isIgnored;
            IsComputed = isComputed;
            ComputedExpression = computedExpression;
            IsUdt = isUdt;
            UdtName = udtName;

            if (IsComputed && string.IsNullOrWhiteSpace(ComputedExpression))
            {
                throw new ArgumentException("Computed expression must be provided if IsComputed is true.", nameof(computedExpression));
            }
            if (IsUdt && string.IsNullOrWhiteSpace(UdtName))
            {
                throw new ArgumentException("UDT name must be provided if IsUdt is true.", nameof(udtName));
            }
             if (IsUdt && !string.IsNullOrWhiteSpace(ComputedExpression))
            {
                 throw new InvalidOperationException($"Property {propertyInfo.Name} cannot be both UDT and computed.");
            }
        }
    }
}
