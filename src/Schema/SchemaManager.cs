using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CassandraDriver.Mapping;
using CassandraDriver.Queries.Expressions; // For ExpressionHelper
using CassandraDriver.Services; // Required for CassandraService
using Microsoft.Extensions.Logging; // Optional: if SchemaManager needs its own logger

namespace CassandraDriver.Schema
{
    public class SchemaManager
    {
        private readonly CassandraService _cassandraService;
        private readonly TableMappingResolver _mappingResolver;
        private readonly ILogger<SchemaManager>? _logger; // Optional logger

        public SchemaManager(CassandraService cassandraService, TableMappingResolver mappingResolver, ILogger<SchemaManager>? logger = null)
        {
            _cassandraService = cassandraService ?? throw new ArgumentNullException(nameof(cassandraService));
            _mappingResolver = mappingResolver ?? throw new ArgumentNullException(nameof(mappingResolver));
            _logger = logger;
        }

        public virtual async Task CreateTableAsync<T>(bool ifNotExists = true) where T : class
        {
            var cql = SchemaGenerator.GetCreateTableCql<T>(_mappingResolver, ifNotExists);
            _logger?.LogInformation("Executing CreateTableAsync for type {EntityType}: {CqlQuery}", typeof(T).FullName, cql);
            await _cassandraService.ExecuteAsync(cql).ConfigureAwait(false);
            _logger?.LogInformation("CreateTableAsync for type {EntityType} completed.", typeof(T).FullName);
        }

        public virtual async Task CreateIndexAsync<T>(Expression<Func<T, object?>> propertyExpression, string? indexName = null, bool ifNotExists = true) where T : class
        {
            var cql = SchemaGenerator.GetCreateIndexCql<T>(_mappingResolver, propertyExpression, indexName, ifNotExists);
            var propertyName = ExpressionHelper.GetPropertyName(propertyExpression); // For logging
            _logger?.LogInformation("Executing CreateIndexAsync for {EntityType}.{PropertyName} (Index: {IndexName}): {CqlQuery}",
                typeof(T).FullName, propertyName, indexName ?? "generated", cql);
            await _cassandraService.ExecuteAsync(cql).ConfigureAwait(false);
            _logger?.LogInformation("CreateIndexAsync for {EntityType}.{PropertyName} completed.", typeof(T).FullName, propertyName);
        }

        public virtual async Task DropTableAsync<T>(bool ifExists = true) where T : class
        {
            var cql = SchemaGenerator.GetDropTableCql<T>(_mappingResolver, ifExists);
            _logger?.LogInformation("Executing DropTableAsync for type {EntityType}: {CqlQuery}", typeof(T).FullName, cql);
            await _cassandraService.ExecuteAsync(cql).ConfigureAwait(false);
            _logger?.LogInformation("DropTableAsync for type {EntityType} completed.", typeof(T).FullName);
        }

        public virtual async Task DropIndexAsync(string indexName, bool ifExists = true)
        {
            // Note: Dropping an index might require keyspace context if not fully qualified,
            // but standard DROP INDEX index_name should work if index_name is unique within keyspace.
            var cql = SchemaGenerator.GetDropIndexCql(indexName, ifExists);
             _logger?.LogInformation("Executing DropIndexAsync for index {IndexName}: {CqlQuery}", indexName, cql);
            await _cassandraService.ExecuteAsync(cql).ConfigureAwait(false);
            _logger?.LogInformation("DropIndexAsync for index {IndexName} completed.", indexName);
        }
    }
}
