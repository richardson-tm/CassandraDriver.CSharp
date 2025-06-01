using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using CassandraDriver.Mapping;
using CassandraDriver.Queries.Expressions; // For ExpressionHelper

namespace CassandraDriver.Schema
{
    public static class SchemaGenerator
    {
        private static string GetCassandraType(Type csharpType, bool isFrozen = false)
        {
            csharpType = Nullable.GetUnderlyingType(csharpType) ?? csharpType;

            if (csharpType == typeof(string)) return "text";
            if (csharpType == typeof(int)) return "int";
            if (csharpType == typeof(long)) return "bigint";
            if (csharpType == typeof(Guid)) return "uuid";
            if (csharpType == typeof(DateTime) || csharpType == typeof(DateTimeOffset)) return "timestamp";
            if (csharpType == typeof(bool)) return "boolean";
            if (csharpType == typeof(double)) return "double";
            if (csharpType == typeof(float)) return "float";
            if (csharpType == typeof(decimal)) return "decimal";
            if (csharpType == typeof(byte[])) return "blob";

            if (csharpType.IsEnum) return "text"; // Store enums as text by default

            if (typeof(System.Collections.IDictionary).IsAssignableFrom(csharpType) && csharpType.IsGenericType)
            {
                var keyType = GetCassandraType(csharpType.GetGenericArguments()[0]);
                var valueType = GetCassandraType(csharpType.GetGenericArguments()[1]);
                var mapType = $"map<{keyType}, {valueType}>";
                return isFrozen ? $"frozen<{mapType}>" : mapType;
            }

            if (typeof(System.Collections.IEnumerable).IsAssignableFrom(csharpType) && csharpType.IsGenericType)
            {
                // Defaulting to list. Could use an attribute to specify set.
                // For sets, the C# type would typically be ISet<T> or HashSet<T>.
                var elementType = GetCassandraType(csharpType.GetGenericArguments()[0]);
                var listType = $"list<{elementType}>";
                return isFrozen ? $"frozen<{listType}>" : listType;
            }

            // Basic UDT mapping by convention (if a class has TableAttribute it's a table, otherwise could be UDT)
            // This is a simplification; real UDTs need their own CREATE TYPE statements.
            // For now, if it's a complex type not a table, and used as a property, it might need to be frozen.
            // This part is advanced and typically requires explicit UDT definition.
            // For now, we'll throw if it's not a known type.
            // if (!csharpType.IsPrimitive && csharpType != typeof(string) && ... can check for UDT marker attribute)
            // {
            //     return $"frozen<{csharpType.Name.ToLowerInvariant()}>"; // By convention
            // }


            throw new NotSupportedException($"C# type {csharpType.FullName} is not supported for Cassandra mapping without a specific UDT definition or attribute.");
        }

        public static string GetCreateTableCql<T>(TableMappingResolver mappingResolver, bool ifNotExists = true) where T : class
        {
            var mappingInfo = mappingResolver.GetMappingInfo(typeof(T));
            var sb = new StringBuilder();

            sb.Append($"CREATE TABLE {(ifNotExists ? "IF NOT EXISTS " : "")}\"{mappingInfo.TableName}\" (");

            List<string> columnDefinitions = new List<string>();
            foreach (var propMap in mappingInfo.Properties.Where(p => !p.IsIgnored && !p.IsComputed))
            {
                // For UDTs or collections that should be frozen, an attribute on the property would be ideal
                // e.g., [CassandraFrozen] or based on TableMappingInfo for the property type.
                // For now, assuming non-frozen for collections/maps by default for simplicity.
                columnDefinitions.Add($"\"{propMap.ColumnName}\" {GetCassandraType(propMap.PropertyInfo.PropertyType)}");
            }
            sb.Append(string.Join(", ", columnDefinitions));

            // Primary Key
            var partitionKeys = mappingInfo.PartitionKeys.OrderBy(pk => pk.PartitionKeyOrder).Select(pk => $"\"{pk.ColumnName}\"").ToList();
            var clusteringKeys = mappingInfo.ClusteringKeys.OrderBy(ck => ck.ClusteringKeyOrder).Select(ck => $"\"{ck.ColumnName}\"").ToList();

            if (!partitionKeys.Any())
                throw new InvalidOperationException($"Entity {typeof(T).FullName} must have at least one partition key.");

            sb.Append(", PRIMARY KEY (");
            if (partitionKeys.Count > 1)
            {
                sb.Append($"({string.Join(", ", partitionKeys)})");
            }
            else
            {
                sb.Append(partitionKeys.First());
            }

            if (clusteringKeys.Any())
            {
                sb.Append($", {string.Join(", ", clusteringKeys)}");
            }
            sb.Append("))");

            // Clustering Order (Example, full implementation requires attributes on properties for order)
            var clusteringOrders = new List<string>();
            foreach (var ck in mappingInfo.ClusteringKeys.Where(c => c.ClusteringKeyOrder != 0)) // Assuming 0 is default/none
            {
                 // This needs an attribute on the property to specify ASC/DESC, e.g. [ClusteringKey(0, Order=KeySortOrder.Descending)]
                 // For now, defaulting to ASC or relying on Cassandra default if not specified.
                 // Let's assume a hypothetical KeySortOrder enum on ClusteringKeyAttribute. For now, this part is illustrative.
                 // string order = ck.SortOrder == KeySortOrder.Descending ? "DESC" : "ASC";
                 // clusteringOrders.Add($"\"{ck.ColumnName}\" {order}");
            }
            if (clusteringOrders.Any()) // This logic needs to be more robust based on actual attributes
            {
                // sb.Append($" WITH CLUSTERING ORDER BY ({string.Join(", ", clusteringOrders)})");
            }
            // Other table options like compaction, compression can be added here.

            sb.Append(";");
            return sb.ToString();
        }

        public static string GetCreateIndexCql<T>(TableMappingResolver mappingResolver, Expression<Func<T, object?>> propertyExpression, string? indexName = null, bool ifNotExists = true) where T : class
        {
            var mappingInfo = mappingResolver.GetMappingInfo(typeof(T));
            var propertyName = ExpressionHelper.GetPropertyName(propertyExpression);
            var propMap = mappingInfo.Properties.FirstOrDefault(p => p.PropertyInfo.Name == propertyName);

            if (propMap == null)
                throw new ArgumentException($"Property {propertyName} not found or not mapped for type {typeof(T).FullName}.");
            if (propMap.IsPartitionKey || propMap.IsClusteringKey)
                throw new ArgumentException($"Cannot create a secondary index on a primary key column: {propMap.ColumnName}.");
            if (propMap.IsIgnored || propMap.IsComputed)
                 throw new ArgumentException($"Cannot create index on ignored or computed property: {propertyName}.");


            string finalIndexName = indexName ?? $"{mappingInfo.TableName}_{propMap.ColumnName}_idx";

            // Basic secondary index. For SASI, use: USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = { ... }
            return $"CREATE INDEX {(ifNotExists ? "IF NOT EXISTS " : "")}\"{finalIndexName}\" ON \"{mappingInfo.TableName}\" (\"{(propMap.ColumnName)}\");";
        }

        public static string GetDropTableCql<T>(TableMappingResolver mappingResolver, bool ifExists = true) where T : class
        {
            var mappingInfo = mappingResolver.GetMappingInfo(typeof(T));
            return $"DROP TABLE {(ifExists ? "IF EXISTS " : "")}\"{mappingInfo.TableName}\";";
        }

        public static string GetDropIndexCql(string indexName, bool ifExists = true) // Index names are typically unique per keyspace.
        {
            if (string.IsNullOrWhiteSpace(indexName))
                throw new ArgumentNullException(nameof(indexName));
            return $"DROP INDEX {(ifExists ? "IF EXISTS " : "")}\"{indexName}\";";
        }
    }
}
