// src/Linq/CassandraQueryable.cs
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace QueryBuilder.Linq
{
    public class CassandraQueryable<T> : IOrderedQueryable<T>
    {
        public Expression Expression { get; }
        public IQueryProvider Provider { get; }

        // Constructor used by CassandraQueryProvider
        public CassandraQueryable(IQueryProvider provider, Expression expression)
        {
            Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        // Constructor for creating a new query from a data context or similar entry point
        public CassandraQueryable(CassandraQueryProvider provider)
        {
            Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            Expression = Expression.Constant(this);
        }

        public Type ElementType => typeof(T);

        public IEnumerator<T> GetEnumerator()
        {
            return Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
