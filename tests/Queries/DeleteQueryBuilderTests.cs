using System;
using System.Collections.Generic;
using System.Linq;
using QueryBuilder.Queries;
using Xunit;

// Using TestModel from SelectQueryBuilderTests.cs

namespace CassandraDriver.Tests.Queries
{
    public class DeleteQueryBuilderTests
    {
        private DeleteQueryBuilder<TestModel> CreateBuilder()
        {
            return new DeleteQueryBuilder<TestModel>();
        }

        [Fact]
        public void Build_SimpleDelete_WithWhereClause()
        {
            // Arrange
            var builder = CreateBuilder().From("TestModels");
            var id = Guid.NewGuid();

            builder.Where(m => m.Id, id);

            // Act
            // Assuming DeleteQueryBuilder.Build() also returns (string, List<object>)
            // similar to the refactored SelectQueryBuilder and desired state for other builders.
            var (query, parameters) = builder.Build();

            // Assert
            Assert.Equal("DELETE FROM TestModels WHERE Id = ?", query);
            Assert.Single(parameters);
            Assert.Equal(id, parameters[0]);
        }

        [Fact]
        public void Build_Delete_WithMultipleWhereClauses()
        {
            // Arrange
            var builder = CreateBuilder().From("TestModels");
            var name = "Obsolete Name";
            var age = 99;

            builder.Where(m => m.Name, name)
                   .Where(m => m.Age, ">", age);

            // Act
            var (query, parameters) = builder.Build();

            // Assert
            Assert.Equal("DELETE FROM TestModels WHERE Name = ? AND Age > ?", query);
            Assert.Equal(2, parameters.Count);
            Assert.Equal(name, parameters[0]);
            Assert.Equal(age, parameters[1]);
        }

        [Fact]
        public void Build_Delete_WhereClauseWithOperator()
        {
            // Arrange
            var builder = CreateBuilder().From("TestModels");
            builder.Where(m => m.Age, "<=", 18);

            // Act
            var (query, parameters) = builder.Build();

            // Assert
            Assert.Equal("DELETE FROM TestModels WHERE Age <= ?", query);
            Assert.Single(parameters);
            Assert.Equal(18, parameters[0]);
        }

        [Fact]
        public void Build_ThrowsException_WhenNoTableNameSpecified()
        {
            // Arrange
            var builder = CreateBuilder();
            builder.Where(m => m.Name, "Test");

            // Act & Assert
            var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("Table name must be specified.", ex.Message);
        }

        [Fact]
        public void Build_Delete_NoWhereClause_ShouldBuildCorrectly()
        {
            // Arrange
            var builder = CreateBuilder().From("TestModels");

            // Act
            var (query, parameters) = builder.Build();

            // Assert
            // This test assumes it's valid to delete without a WHERE clause (deletes all rows).
            // Depending on application requirements or database features (e.g., TRUNCATE vs DELETE FROM),
            // this might be desired or disallowed (e.g., throw exception if no WHERE).
            // Current DeleteQueryBuilder allows this.
            Assert.Equal("DELETE FROM TestModels", query);
            Assert.Empty(parameters);
        }
    }
}
