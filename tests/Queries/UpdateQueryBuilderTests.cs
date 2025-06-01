using System;
using System.Collections.Generic;
using System.Linq;
using QueryBuilder.Queries;
using Xunit;

// Using TestModel from SelectQueryBuilderTests.cs

namespace CassandraDriver.Tests.Queries
{
    public class UpdateQueryBuilderTests
    {
        private UpdateQueryBuilder<TestModel> CreateBuilder()
        {
            return new UpdateQueryBuilder<TestModel>();
        }

        [Fact]
        public void Build_SimpleUpdate_CorrectQueryAndParameters()
        {
            // Arrange
            var builder = CreateBuilder().Table("TestModels");
            var id = Guid.NewGuid();
            var newName = "Updated Name";
            var newAge = 35;

            builder.Set(m => m.Name, newName)
                   .Set(m => m.Age, newAge)
                   .Where(m => m.Id, id);

            // Act
            // Assuming UpdateQueryBuilder.Build() was refactored to return (string, List<object>)
            // based on the comment in the previous step with InsertQueryBuilder.
            // If not, this test will need to be adjusted.
            var (query, parameters) = builder.Build();


            // Assert
            Assert.Equal("UPDATE TestModels SET Name = ?, Age = ? WHERE Id = ?", query);
            Assert.Equal(3, parameters.Count);
            Assert.Equal(newName, parameters[0]);
            Assert.Equal(newAge, parameters[1]);
            Assert.Equal(id, parameters[2]);
        }

        [Fact]
        public void Build_Update_WithMultipleWhereClauses()
        {
            // Arrange
            var builder = CreateBuilder().Table("TestModels");
            var name = "Old Name";
            var newAge = 40;

            builder.Set(m => m.Age, newAge)
                   .Where(m => m.Name, name)
                   .Where(m => m.IsActive, true); // Assuming IsActive is a boolean property

            // Act
            var (query, parameters) = builder.Build();

            // Assert
            Assert.Equal("UPDATE TestModels SET Age = ? WHERE Name = ? AND IsActive = ?", query);
            Assert.Equal(3, parameters.Count);
            Assert.Equal(newAge, parameters[0]);
            Assert.Equal(name, parameters[1]);
            Assert.Equal(true, parameters[2]);
        }

        [Fact]
        public void Build_Update_WhereClauseWithOperator()
        {
            // Arrange
            var builder = CreateBuilder().Table("TestModels");
            builder.Set(m => m.Age, 50)
                   .Where(m => m.Age, ">=", 40);

            // Act
            var (query, parameters) = builder.Build();

            // Assert
            Assert.Equal("UPDATE TestModels SET Age = ? WHERE Age >= ?", query);
            Assert.Equal(2, parameters.Count);
            Assert.Equal(50, parameters[0]);
            Assert.Equal(40, parameters[1]);
        }


        [Fact]
        public void Build_ThrowsException_WhenNoTableNameSpecified()
        {
            // Arrange
            var builder = CreateBuilder();
            builder.Set(m => m.Name, "Test");

            // Act & Assert
            var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("Table name must be specified.", ex.Message);
        }

        [Fact]
        public void Build_ThrowsException_WhenNoSetValuesSpecified()
        {
            // Arrange
            var builder = CreateBuilder().Table("TestModels");
            builder.Where(m => m.Id, Guid.NewGuid());

            // Act & Assert
            var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("At least one SET value must be specified.", ex.Message);
        }

        [Fact]
        public void Build_Update_NoWhereClause_ShouldBuildCorrectly()
        {
            // Arrange
            var builder = CreateBuilder().Table("TestModels");
            builder.Set(m => m.Name, "Global Update");

            // Act
            var (query, parameters) = builder.Build();

            // Assert
            // This test assumes it's valid to update without a WHERE clause (updates all rows).
            // Depending on application requirements, this might be desired or disallowed.
            Assert.Equal("UPDATE TestModels SET Name = ?", query);
            Assert.Single(parameters);
            Assert.Equal("Global Update", parameters[0]);
        }

        [Fact]
        public void HasSetValues_Property_WorksCorrectly()
        {
            // Arrange
            var builder = CreateBuilder().Table("TestModels");

            // Assert initial state
            Assert.False(builder.HasSetValues);

            builder.Set(m => m.Name, "Test");

            // Assert state after adding a value
            Assert.True(builder.HasSetValues);
        }
    }
}
