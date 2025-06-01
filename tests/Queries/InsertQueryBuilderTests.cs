using System;
using System.Collections.Generic;
using System.Linq;
using CassandraDriver.Queries; // Assuming this is the namespace for the builders
using Xunit;

// Using the same TestModel from SelectQueryBuilderTests for consistency
// public class TestModel { public Guid Id { get; set; } public string Name { get; set; } ... }

namespace CassandraDriver.Tests.Queries
{
    public class InsertQueryBuilderTests
    {
        private InsertQueryBuilder<TestModel> CreateBuilder()
        {
            return new InsertQueryBuilder<TestModel>();
        }

        [Fact]
        public void Build_SingleValueInsert_CorrectQueryAndParameters()
        {
            // Arrange
            var builder = CreateBuilder().Into("TestModels");
            var model = new TestModel { Id = Guid.NewGuid(), Name = "Test Name", Age = 30, CreatedDate = new DateTime(2023, 1, 1, 10, 0, 0, DateTimeKind.Utc) };

            builder.Value(m => m.Id, model.Id);
            builder.Value(m => m.Name, model.Name);
            builder.Value(m => m.Age, model.Age);
            builder.Value(m => m.CreatedDate, model.CreatedDate);

            // Act
            var query = builder.Build(); // Build method in InsertQueryBuilder returns string directly

            // Assert
            // Note: InsertQueryBuilder's Build() currently returns a string query with formatted values, not (query, params)
            // This needs to be aligned with other builders if parameterization is desired.
            // Assert (New: for parameterized query)
            var (queryString, queryParams) = builder.Build(); // Build now returns a tuple
            Assert.Equal($"INSERT INTO TestModels (Id, Name, Age, CreatedDate) VALUES (?, ?, ?, ?)", queryString);
            Assert.Equal(4, queryParams.Count);
            Assert.Equal(model.Id, queryParams[0]);
            Assert.Equal(model.Name, queryParams[1]);
            Assert.Equal(model.Age, queryParams[2]);
            Assert.Equal(model.CreatedDate, queryParams[3]);
        }

        [Fact]
        public void Build_Insert_NullValue()
        {
            // Arrange
            var builder = CreateBuilder().Into("TestModels");
            var model = new TestModel { Id = Guid.NewGuid(), Name = null, Age = 25 };

            builder.Value(m => m.Id, model.Id);
            builder.Value(m => m.Name, model.Name); // Name is null

            // Act
            var (queryString, queryParams) = builder.Build();

            // Assert
            Assert.Equal($"INSERT INTO TestModels (Id, Name) VALUES (?, ?)", queryString);
            Assert.Equal(2, queryParams.Count);
            Assert.Equal(model.Id, queryParams[0]);
            Assert.Null(queryParams[1]); // Name should be null
        }

        [Fact]
        public void Build_ThrowsException_WhenNoTableNameSpecified()
        {
            // Arrange
            var builder = CreateBuilder();
            builder.Value(m => m.Name, "Test");

            // Act & Assert
            var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("Table name must be specified.", ex.Message);
        }

        [Fact]
        public void Build_ThrowsException_WhenNoValuesSpecified()
        {
            // Arrange
            var builder = CreateBuilder().Into("TestModels");

            // Act & Assert
            var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
            Assert.Equal("At least one value must be specified.", ex.Message);
        }

        [Fact]
        public void Build_ValueEscapesStringLiterals()
        {
            // Arrange
            var builder = CreateBuilder().Into("TestModels");
            var trickyName = "Test's Name";
            var expectedEscapedName = "Test''s Name";
            builder.Value(m => m.Name, trickyName);

            // Act
            var (queryString, queryParams) = builder.Build();

            // Assert
            Assert.Equal($"INSERT INTO TestModels (Name) VALUES (?)", queryString);
            Assert.Single(queryParams);
            Assert.Equal(trickyName, queryParams[0]); // Parameter value should be the original, unescaped string
        }
    }
}
