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

        public PropertyMappingInfo(PropertyInfo propertyInfo, string columnName,
            bool isPartitionKey, int partitionKeyOrder,
            bool isClusteringKey, int clusteringKeyOrder,
            bool isIgnored, bool isComputed, string? computedExpression)
        {
            PropertyInfo = propertyInfo ?? throw new ArgumentNullException(nameof(propertyInfo));
            ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
            IsPartitionKey = isPartitionKey;
            PartitionKeyOrder = partitionKeyOrder;
            IsClusteringKey = isClusteringKey;
            ClusteringKeyOrder = clusteringKeyOrder;
            IsIgnored = isIgnored;
            IsComputed = isComputed;
            ComputedExpression = computedExpression;

            if (IsComputed && string.IsNullOrWhiteSpace(ComputedExpression))
            {
                throw new ArgumentException("Computed expression must be provided if IsComputed is true.", nameof(computedExpression));
            }
        }
    }
}
