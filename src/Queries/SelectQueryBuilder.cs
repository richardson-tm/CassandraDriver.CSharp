using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices; // For [EnumeratorCancellation]
using System.Text;
using System.Threading; // For CancellationToken
using System.Threading.Tasks;
using Cassandra;
using CassandraDriver.Mapping;
using CassandraDriver.Queries.Expressions;
using CassandraDriver.Services; // To access CassandraService

namespace CassandraDriver.Queries
{
    public class SelectQueryBuilder<T> where T : class, new()
    {
        private readonly CassandraService _cassandraService;
        private readonly TableMappingResolver _mappingResolver;
        private readonly TableMappingInfo _tableMapping;

        private List<string>? _selectColumns;
        private List<string> _selectExpressions = new List<string>(); // For property expressions
        private readonly List<(string Condition, object[] Values)> _whereClauses = new();
        private readonly List<(string Column, bool Ascending)> _orderByClauses = new();
        private int? _limit;
        private int? _pageSize;

        public SelectQueryBuilder(CassandraService cassandraService, TableMappingResolver mappingResolver)
        {
            _cassandraService = cassandraService ?? throw new ArgumentNullException(nameof(cassandraService));
            _mappingResolver = mappingResolver ?? throw new ArgumentNullException(nameof(mappingResolver));
            _tableMapping = _mappingResolver.GetMappingInfo(typeof(T));
        }

        public SelectQueryBuilder<T> Select(params string[] columns)
        {
            if (columns == null || columns.Length == 0)
            {
                _selectColumns = null; // Indicates select all mapped non-ignored columns
                _selectExpressions.Clear();
            }
            else
            {
                _selectColumns = new List<string>(columns);
                _selectExpressions.Clear();
            }
            return this;
        }

        public SelectQueryBuilder<T> Select<TProperty>(Expression<Func<T, TProperty>> propertyExpression)
        {
            var propertyName = ExpressionHelper.GetPropertyName(propertyExpression);
            var propertyMap = _tableMapping.Properties.FirstOrDefault(p => p.PropertyInfo.Name == propertyName);

            if (propertyMap == null)
                throw new ArgumentException($"Property {propertyName} is not mapped for type {typeof(T).FullName}.");

            if (propertyMap.IsIgnored)
                throw new ArgumentException($"Property {propertyName} is ignored and cannot be selected for type {typeof(T).FullName}.");

            if (_selectColumns == null && !_selectExpressions.Any()) // Transitioning from SELECT *
            {
                 _selectExpressions = new List<string>(); // Initialize if it was implicitly all columns
            }
            _selectColumns = null; // Cannot mix raw string select with expression select for simplicity here

            if (propertyMap.IsComputed)
            {
                _selectExpressions.Add($"{propertyMap.ComputedExpression} AS \"{propertyMap.ColumnName}\"");
            }
            else
            {
                _selectExpressions.Add($"\"{propertyMap.ColumnName}\"");
            }
            return this;
        }

        public SelectQueryBuilder<T> Where(string condition, params object[] values)
        {
            if (string.IsNullOrWhiteSpace(condition))
                throw new ArgumentNullException(nameof(condition));
            _whereClauses.Add((condition, values ?? Array.Empty<object>()));
            return this;
        }

        public SelectQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertyExpression, QueryOperator op, TProperty value)
        {
            var propertyName = ExpressionHelper.GetPropertyName(propertyExpression);
            var propertyMap = _tableMapping.Properties.FirstOrDefault(p => p.PropertyInfo.Name == propertyName);

            if (propertyMap == null || propertyMap.IsIgnored || propertyMap.IsComputed) // Computed fields generally not for WHERE
                throw new ArgumentException($"Property {propertyName} is not suitable for a WHERE clause for type {typeof(T).FullName}.");

            string opString = GetOperatorString(op);

            if (op == QueryOperator.In)
            {
                 _whereClauses.Add(( $"\"{propertyMap.ColumnName}\" {opString} ?", new object[] { value! })); // Driver handles IN with single parameter list
            }
            else
            {
                 _whereClauses.Add(( $"\"{propertyMap.ColumnName}\" {opString} ?", new object[] { value! }));
            }
            return this;
        }

        // Overload for IN operator with IEnumerable
        public SelectQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertyExpression, QueryOperator op, IEnumerable<TProperty> values)
        {
            if (op != QueryOperator.In)
                throw new ArgumentException("This Where overload only supports the IN operator with IEnumerable values.");

            var propertyName = ExpressionHelper.GetPropertyName(propertyExpression);
            var propertyMap = _tableMapping.Properties.FirstOrDefault(p => p.PropertyInfo.Name == propertyName);

            if (propertyMap == null || propertyMap.IsIgnored || propertyMap.IsComputed)
                throw new ArgumentException($"Property {propertyName} is not suitable for a WHERE clause for type {typeof(T).FullName}.");

            // For IN (?, ?, ?), the driver expects individual parameters.
            // However, the C# driver also supports IN ? with a single List/IEnumerable parameter.
            _whereClauses.Add(( $"\"{propertyMap.ColumnName}\" IN ?", new object[] { values }));
            return this;
        }


        public SelectQueryBuilder<T> OrderBy(string column, bool ascending = true)
        {
            if (string.IsNullOrWhiteSpace(column))
                throw new ArgumentNullException(nameof(column));
            _orderByClauses.Add((column, ascending));
            return this;
        }

        public SelectQueryBuilder<T> OrderBy<TProperty>(Expression<Func<T, TProperty>> propertyExpression, bool ascending = true)
        {
            var propertyName = ExpressionHelper.GetPropertyName(propertyExpression);
            var propertyMap = _tableMapping.Properties.FirstOrDefault(p => p.PropertyInfo.Name == propertyName);

            if (propertyMap == null || propertyMap.IsIgnored) // Computed fields might be orderable if aliased
                throw new ArgumentException($"Property {propertyName} is not suitable for an ORDER BY clause for type {typeof(T).FullName}.");

            _orderByClauses.Add(( $"\"{propertyMap.ColumnName}\"", ascending));
            return this;
        }

        public SelectQueryBuilder<T> Limit(int count)
        {
            if (count <= 0)
                throw new ArgumentOutOfRangeException(nameof(count), "Limit must be greater than zero.");
            _limit = count;
            return this;
        }

        public (string Query, List<object> Parameters) Build()
        {
            var statement = BuildStatement();
            var parameters = new List<object>();
            if (statement.QueryValues != null)
            {
                parameters.AddRange(statement.QueryValues);
            }
            return (statement.QueryString, parameters);
        }

        public SimpleStatement BuildStatement()
        {
            var sb = new StringBuilder("SELECT ");
            var parameters = new List<object>();

            if (_selectExpressions.Any())
            {
                sb.Append(string.Join(", ", _selectExpressions));
            }
            else if (_selectColumns != null && _selectColumns.Any())
            {
                sb.Append(string.Join(", ", _selectColumns.Select(c => c.Contains("(") ? c : $"\"{c}\""))); // Quote if not a function call
            }
            else // Select all mapped non-ignored, non-computed (or aliased computed)
            {
                sb.Append(string.Join(", ", _tableMapping.Properties
                    .Where(p => !p.IsIgnored)
                    .Select(p => p.IsComputed ? $"{p.ComputedExpression} AS \"{p.ColumnName}\"" : $"\"{p.ColumnName}\"")));
            }

            sb.Append($" FROM \"{_tableMapping.TableName}\"");

            if (_whereClauses.Any())
            {
                sb.Append(" WHERE ");
                for (int i = 0; i < _whereClauses.Count; i++)
                {
                    if (i > 0) sb.Append(" AND ");
                    sb.Append(_whereClauses[i].Condition);
                    parameters.AddRange(_whereClauses[i].Values);
                }
            }

            if (_orderByClauses.Any())
            {
                sb.Append(" ORDER BY ");
                sb.Append(string.Join(", ", _orderByClauses.Select(ob => $"\"{ob.Column}\" {(ob.Ascending ? "ASC" : "DESC")}")));
            }

            if (_limit.HasValue)
            {
                sb.Append(" LIMIT ?"); // Changed from LIMIT _limit.Value to LIMIT ? for parameterized query
                parameters.Add(_limit.Value);
            }

            return new SimpleStatement(sb.ToString(), parameters.ToArray());
        }

        public async Task<List<T>> ToListAsync()
        {
            var statement = BuildStatement();
            var rowSet = await _cassandraService.ExecuteAsync(statement);
            var results = new List<T>();
            foreach (var row in rowSet)
            {
                results.Add(_cassandraService.MapRowToEntity<T>(row, _tableMapping)); // MapRowToEntity needs to be public or internal in CassandraService
            }
            return results;
        }

        public async Task<T?> FirstOrDefaultAsync()
        {
            Limit(1); // Apply LIMIT 1
            var statement = BuildStatement();
            var rowSet = await _cassandraService.ExecuteAsync(statement);
            var firstRow = rowSet.FirstOrDefault();
            return firstRow != null ? _cassandraService.MapRowToEntity<T>(firstRow, _tableMapping) : null;
        }

        private string GetOperatorString(QueryOperator op) => op switch
        {
            QueryOperator.Equal => "=",
            QueryOperator.NotEqual => "!=",
            QueryOperator.GreaterThan => ">",
            QueryOperator.GreaterThanOrEqual => ">=",
            QueryOperator.LessThan => "<",
            QueryOperator.LessThanOrEqual => "<=",
            QueryOperator.In => "IN",
            _ => throw new ArgumentOutOfRangeException(nameof(op), $"Unsupported query operator: {op}")
        };

        public SelectQueryBuilder<T> PageSize(int size)
        {
            if (size <= 0)
                throw new ArgumentOutOfRangeException(nameof(size), "Page size must be greater than zero.");
            _pageSize = size;
            return this;
        }

        public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var statement = BuildStatement();

            // Apply page size if set, otherwise use driver default or QueryOptions default
            if (_pageSize.HasValue)
            {
                statement.SetPageSize(_pageSize.Value);
            }
            // The C# driver enables auto-paging by default if a page size is set.
            // Explicitly calling statement.SetAutoPage(true) is usually not needed if page size is > 0.
            // However, if _pageSize is null, we might rely on a global default from QueryOptions
            // or a driver default. For explicit control with AutoPageAsync, ensure a page size is set.
            // If _pageSize is null here, and no global default is applied by the driver, paging might not work as expected.
            // For robustness, if using AutoPageAsync, ensure a page size is always set.
            if (!_pageSize.HasValue && statement.PageSize <=0) // If no specific page size and statement default is not conducive to paging
            {
                 statement.SetPageSize(100); // Sensible default if nothing else is configured.
            }


            // Execute the initial query to get the first RowSet
            // The ExecuteAsync in CassandraService already handles metrics for the initial call.
            // Subsequent automatic page fetches by the driver might not be captured by those specific metrics.
            var rowSet = await _cassandraService.ExecuteAsync(statement).ConfigureAwait(false);

            // Manual paging for driver v3
            foreach (var row in rowSet)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return _cassandraService.MapRowToEntity<T>(row, _tableMapping);
            }
            
            // Handle subsequent pages
            while (rowSet.PagingState != null && !cancellationToken.IsCancellationRequested)
            {
                statement.SetPagingState(rowSet.PagingState);
                rowSet = await _cassandraService.ExecuteAsync(statement).ConfigureAwait(false);
                
                foreach (var row in rowSet)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return _cassandraService.MapRowToEntity<T>(row, _tableMapping);
                }
            }
        }
    }
}
