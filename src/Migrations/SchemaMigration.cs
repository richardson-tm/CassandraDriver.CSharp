using System;
using CassandraDriver.Mapping.Attributes; // Assuming this is the correct namespace for your attributes

namespace CassandraDriver.Migrations
{
    // This class will be mapped to the schema_migrations table.
    // The actual table name can be configured via MigrationConfiguration.
    // For simplicity in mapping, we assume the default name "schema_migrations" here
    // or rely on the CassandraService's GetAsync<T> to correctly use the configured name if we make it flexible.
    // For now, let's make it usable with the ORM features if the table name matches.
    [Table("schema_migrations")] // Default, can be overridden by configuration at runtime
    public class SchemaMigration
    {
        [PartitionKey]
        [Column("version")] // Matches the Cassandra column name
        public string Version { get; set; } = string.Empty;

        [Column("applied_on")]
        public DateTimeOffset AppliedOn { get; set; }

        [Column("description")]
        public string Description { get; set; } = string.Empty;
    }
}
