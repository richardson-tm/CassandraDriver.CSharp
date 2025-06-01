// UpdateQueryBuilder.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace CassandraDriver.Queries
{
    public class UpdateQueryBuilder<T>
    {
        private readonly List<(string ColumnName, object Value)> _setValues = new List<(string, object)>();
        // Store where clauses as (ColumnName, Operator, Value) to build parameterized queries
        private readonly List<(string ColumnName, string Operator, object Value)> _whereClauses = new List<(string, string, object)>();
        private string _tableName = string.Empty;

        public UpdateQueryBuilder<T> Table(string tableName)
        {
            _tableName = tableName;
            return this;
        }

        public UpdateQueryBuilder<T> Set<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value)
        {
            var memberExpression = (MemberExpression)propertySelector.Body;
            var columnName = memberExpression.Member.Name;
            _setValues.Add((columnName, value!));
            return this;
        }

        public UpdateQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertySelector, TProperty value)
        {
            return Where(propertySelector, "=", value);
        }

        // Overload for different comparison operators
        public UpdateQueryBuilder<T> Where<TProperty>(Expression<Func<T, TProperty>> propertySelector, string comparisonOperator, TProperty value)
        {
            var memberExpression = (MemberExpression)propertySelector.Body;
            var columnName = memberExpression.Member.Name;
            _whereClauses.Add((columnName, comparisonOperator, value!));
            return this;
        }

        public bool HasSetValues => _setValues.Any();

        public (string Query, List<object> Parameters) Build()
        {
            if (string.IsNullOrEmpty(_tableName))
            {
                throw new InvalidOperationException("Table name must be specified.");
            }

            if (!_setValues.Any())
            {
                throw new InvalidOperationException("At least one SET value must be specified.");
            }

            var parameters = new List<object>();
            var setClauses = new List<string>();
            foreach (var (columnName, value) in _setValues)
            {
                setClauses.Add($"{columnName} = ?");
                parameters.Add(value);
            }
            var setClauseString = string.Join(", ", setClauses);

            var whereClauseString = "";
            if (_whereClauses.Any())
            {
                var whereConditions = new List<string>();
                foreach (var (columnName, op, value) in _whereClauses)
                {
                    whereConditions.Add($"{columnName} {op} ?");
                    parameters.Add(value);
                }
                whereClauseString = $" WHERE {string.Join(" AND ", whereConditions)}";
            }

            return ($"UPDATE {_tableName} SET {setClauseString}{whereClauseString}", parameters);
        }

        // FormatValue is no longer needed here if all values are parameterized.
        // private string FormatValue(object value) ...
    }
}
