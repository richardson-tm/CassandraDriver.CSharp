// InsertQueryBuilder.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace QueryBuilder.Queries
{
    public class InsertQueryBuilder<T>
    {
        private readonly List<(string ColumnName, object Value)> _values = new List<(string, object)>();
        private string _tableName;

        public InsertQueryBuilder<T> Into(string tableName)
        {
            _tableName = tableName;
            return this;
        }

        public InsertQueryBuilder<T> Value<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value)
        {
            var memberExpression = (MemberExpression)propertySelector.Body;
            var columnName = memberExpression.Member.Name;
            _values.Add((columnName, value));
            return this;
        }

        // Changed return type from string to (string Query, List<object> Parameters)
        public (string Query, List<object> Parameters) Build()
        {
            if (string.IsNullOrEmpty(_tableName))
            {
                throw new InvalidOperationException("Table name must be specified.");
            }

            if (!_values.Any())
            {
                throw new InvalidOperationException("At least one value must be specified.");
            }

            var columnNames = string.Join(", ", _values.Select(v => v.ColumnName));
            var valuePlaceholders = string.Join(", ", _values.Select(_ => "?")); // Keep this one
            // Removed duplicate: var valuePlaceholders = string.Join(", ", _values.Select(_ => "?"));
            var queryParameters = _values.Select(v => v.Value ?? DBNull.Value).ToList<object>();

            return ($"INSERT INTO {_tableName} ({columnNames}) VALUES ({valuePlaceholders})", queryParameters);
        }

        // public (string Query, List<object> Parameters) Build() was duplicated, removed outer one.
        // The correctly modified Build method is above.

        // FormatValue is no longer needed here if all values are parameterized.
        // private string FormatValue(object value) ...
    }
}
