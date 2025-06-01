// DeleteQueryBuilder.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace CassandraDriver.Queries
{
    public class DeleteQueryBuilder<T>
    {
        private readonly List<string> _whereClauses = new List<string>();
        private string _tableName = string.Empty;

        public DeleteQueryBuilder<T> From(string tableName)
        {
            _tableName = tableName;
            return this;
        }

        public DeleteQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value)
        {
            var memberExpression = (MemberExpression)propertySelector.Body;
            var columnName = memberExpression.Member.Name;
            _whereClauses.Add($"{columnName} = {FormatValue(value!)}");
            return this;
        }

        // Overload for different comparison operators
        public DeleteQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertySelector, string comparisonOperator, TProperty value)
        {
            var memberExpression = (MemberExpression)propertySelector.Body;
            var columnName = memberExpression.Member.Name;
            _whereClauses.Add($"{columnName} {comparisonOperator} {FormatValue(value!)}");
            return this;
        }

        public string Build()
        {
            if (string.IsNullOrEmpty(_tableName))
            {
                throw new InvalidOperationException("Table name must be specified.");
            }

            var whereClause = _whereClauses.Any() ? $" WHERE {string.Join(" AND ", _whereClauses)}" : "";

            return $"DELETE FROM {_tableName}{whereClause}";
        }

        private string FormatValue(object value)
        {
            if (value == null)
            {
                return "NULL";
            }

            if (value is string || value is Guid)
            {
                return $"'{value}'";
            }

            if (value is DateTime dateTime)
            {
                return $"'{dateTime:yyyy-MM-dd HH:mm:ss}'";
            }

            return value.ToString() ?? string.Empty;
        }
    }
}
