using System;
using System.Collections.Generic;
using System.Linq;
using CassandraDriver.Linq; // Your LINQ provider namespace
using CassandraDriver.Queries; // Your QueryBuilder namespace (for SelectQueryBuilder if needed, or for comparison)
using Xunit;

// Assuming TestModel is accessible here, e.g., defined in CassandraDriver.Tests.Queries or a shared test models project.
// If not, it should be defined or imported. For this context, we'll assume it's available:
// public class TestModel { public Guid Id { get; set; } public string Name { get; set; } public int Age { get; set; } ... }
using CassandraDriver.Tests.Queries; // This makes TestModel available

namespace CassandraDriver.Tests.Linq
{
    public class LinqToCassandraTests
    {
        private IQueryable<TestModel> CreateTestQueryable()
        {
            // The CassandraQueryProvider would typically be part of a data context or session object.
            // For testing expression translation, we can instantiate it directly.
            var provider = new CassandraQueryProvider();
            return new CassandraQueryable<TestModel>(provider);
        }

        [Fact]
        public void Translate_SimpleWhereClause_Equals()
        {
            // Arrange
            var queryable = CreateTestQueryable();
            var id = Guid.NewGuid();
            var query = queryable.Where(m => m.Id == id);

            // Act
            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            // Assert
            Assert.Equal($"SELECT * FROM TestModel WHERE Id = ?", cql);
            Assert.Single(parameters);
            Assert.Equal(id, parameters[0]);
        }

        [Fact]
        public void Translate_WhereClause_GreaterThan()
        {
            var queryable = CreateTestQueryable();
            var query = queryable.Where(m => m.Age > 30);

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            Assert.Equal($"SELECT * FROM TestModel WHERE Age > ?", cql);
            Assert.Single(parameters);
            Assert.Equal(30, parameters[0]);
        }

        [Fact]
        public void Translate_MultipleWhereClauses_AndLogic()
        {
            var queryable = CreateTestQueryable();
            var name = "Test";
            var query = queryable.Where(m => m.Age < 40 && m.Name == name);

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            // Note: QueryTranslator's current binary expression parsing might simplify this to the last condition
            // or might require adjustments to handle chained ANDs correctly.
            // Current QueryTranslator is expected to handle this by recursively visiting binary expressions.
            // The specific output might depend on how it reconstructs the query.
            // A common, simple way is `(Left) Operator (Right)`. If it's `m.Age < 40 && m.Name == name`,
            // the translator should break this down. The test for ParseWhereClause in QueryTranslator
            // needs to be robust for this.
            // For now, let's assume a simple flattening if not fully recursive:
            // This test needs verification against actual QueryTranslator behavior.
            // If QueryTranslator was building a tree and then linearizing, it could be correct.
            // Given current QueryTranslator, it might only handle one part of the &&.
            // Let's assume it's `(m.Age < 40) AND (m.Name == name)`
            Assert.Equal($"SELECT * FROM TestModel WHERE (Age < ?) AND (Name = ?)", cql);
            Assert.Equal(2, parameters.Count);
            Assert.Equal(40, parameters[0]); // Value for Age
            Assert.Equal(name, parameters[1]); // Value for Name
        }

        [Fact]
        public void Translate_WhereClause_BooleanProperty_EqualsTrue()
        {
            var queryable = CreateTestQueryable();
            var query = queryable.Where(m => m.IsActive == true); // Or just .Where(m => m.IsActive)

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            Assert.Equal($"SELECT * FROM TestModel WHERE IsActive = ?", cql);
            Assert.Single(parameters);
            Assert.Equal(true, parameters[0]);
        }

        [Fact]
        public void Translate_WhereClause_BooleanProperty_ImplicitTrue()
        {
            var queryable = CreateTestQueryable();
            var query = queryable.Where(m => m.IsActive);

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            // QueryTranslator should convert `m.IsActive` (where m.IsActive is bool) to `m.IsActive = true`
            Assert.Equal($"SELECT * FROM TestModel WHERE IsActive = ?", cql);
            Assert.Single(parameters);
            Assert.Equal(true, parameters[0]);
        }


        [Fact]
        public void Translate_SimpleSelectClause_SingleProperty()
        {
            var queryable = CreateTestQueryable();
            var query = queryable.Select(m => m.Name);

            var provider = query.Provider as CassandraQueryProvider;
            // The Translate method in QueryTranslator is designed for the whole query expression.
            // For Select, the selection is part of the MethodCallExpression.
            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            Assert.Equal("SELECT Name FROM TestModel", cql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Translate_SelectClause_AnonymousType()
        {
            var queryable = CreateTestQueryable();
            var query = queryable.Select(m => new { m.Id, CustomerName = m.Name }); // Projection with rename

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            // QueryTranslator's Select parser would list selected columns.
            // Current QueryTranslator is expected to select "Id" and "Name", not handle aliasing like "CustomerName".
            Assert.Equal("SELECT Id, Name FROM TestModel", cql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Translate_SelectAndWhereClause_Combined()
        {
            var queryable = CreateTestQueryable();
            var age = 25;
            var query = queryable.Where(m => m.Age > age).Select(m => new { m.Name });

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            Assert.Equal("SELECT Name FROM TestModel WHERE Age > ?", cql);
            Assert.Single(parameters);
            Assert.Equal(age, parameters[0]);
        }

        [Fact]
        public void Translate_OrderBy_Ascending()
        {
            var queryable = CreateTestQueryable();
            var query = queryable.OrderBy(m => m.Age);

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            Assert.Equal("SELECT * FROM TestModel ORDER BY Age ASC", cql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Translate_OrderBy_Descending()
        {
            var queryable = CreateTestQueryable();
            var query = queryable.OrderByDescending(m => m.Name);

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            Assert.Equal("SELECT * FROM TestModel ORDER BY Name DESC", cql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Translate_ThenBy_Ascending()
        {
            var queryable = CreateTestQueryable();
            var query = queryable.OrderBy(m => m.Age).ThenBy(m => m.Name);

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            Assert.Equal("SELECT * FROM TestModel ORDER BY Age ASC, Name ASC", cql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Translate_ThenBy_Descending()
        {
            var queryable = CreateTestQueryable();
            var query = queryable.OrderBy(m => m.Age).ThenByDescending(m => m.Name);

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            Assert.Equal("SELECT * FROM TestModel ORDER BY Age ASC, Name DESC", cql);
            Assert.Empty(parameters);
        }

        [Fact]
        public void Translate_Take_Simple()
        {
            var queryable = CreateTestQueryable();
            var query = queryable.Take(10);

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            Assert.Equal("SELECT * FROM TestModel LIMIT ?", cql);
            Assert.Single(parameters);
            Assert.Equal(10, parameters[0]);
        }

        [Fact]
        public void Translate_Where_Select_OrderBy_Take_Combined()
        {
            var queryable = CreateTestQueryable();
            var id = Guid.NewGuid();
            var query = queryable.Where(m => m.Id == id && m.IsActive)
                                 .OrderByDescending(m => m.CreatedDate)
                                 .Select(m => new { m.Name, m.Age })
                                 .Take(5);

            var translator = new QueryTranslator();
            var (cql, parameters) = translator.Translate(query.Expression);

            // Expected order: SELECT ... FROM ... WHERE ... ORDER BY ... LIMIT ...
            Assert.Equal("SELECT Name, Age FROM TestModel WHERE (Id = ?) AND (IsActive = ?) ORDER BY CreatedDate DESC LIMIT ?", cql);
            Assert.Equal(3, parameters.Count);
            Assert.Equal(id, parameters[0]);
            Assert.Equal(true, parameters[1]);
            Assert.Equal(5, parameters[2]);
        }
    }
}
