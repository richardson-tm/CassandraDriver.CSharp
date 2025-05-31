using System;
using System.Linq.Expressions;
using System.Reflection;

namespace CassandraDriver.Queries.Expressions
{
    public static class ExpressionHelper
    {
        public static string GetPropertyName<TSource, TProperty>(Expression<Func<TSource, TProperty>> propertyLambda)
        {
            if (propertyLambda.Body is MemberExpression memberExpression)
            {
                return memberExpression.Member.Name;
            }

            // Handles cases where the property is accessed via a cast (e.g., (object)x.Property)
            if (propertyLambda.Body is UnaryExpression unaryExpression && unaryExpression.Operand is MemberExpression unaryMemberExpression)
            {
                return unaryMemberExpression.Member.Name;
            }

            throw new ArgumentException("Invalid expression. Expression must be a direct member access.", nameof(propertyLambda));
        }

        public static MemberInfo GetMemberInfo<TSource, TProperty>(Expression<Func<TSource, TProperty>> propertyLambda)
        {
            if (propertyLambda.Body is MemberExpression memberExpression)
            {
                return memberExpression.Member;
            }
            if (propertyLambda.Body is UnaryExpression unaryExpression && unaryExpression.Operand is MemberExpression unaryMemberExpression)
            {
                return unaryMemberExpression.Member;
            }
            throw new ArgumentException("Invalid expression. Expression must be a direct member access.", nameof(propertyLambda));
        }
    }
}
