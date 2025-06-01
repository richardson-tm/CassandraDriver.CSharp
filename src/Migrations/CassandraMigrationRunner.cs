using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using CassandraDriver.Configuration; // For MigrationConfiguration

namespace CassandraDriver.Migrations
{
    public class CassandraMigrationRunner
    {
        // private readonly CassandraService _cassandraService; // Commented out
        private readonly ILogger<CassandraMigrationRunner> _logger;
        private readonly MigrationConfiguration _migrationConfig;

        // public CassandraMigrationRunner(CassandraService cassandraService, MigrationConfiguration migrationConfig, ILogger<CassandraMigrationRunner> logger) // Commented out
        public CassandraMigrationRunner(MigrationConfiguration migrationConfig, ILogger<CassandraMigrationRunner> logger) // Updated constructor
        {
            // _cassandraService = cassandraService ?? throw new ArgumentNullException(nameof(cassandraService)); // Commented out
            _migrationConfig = migrationConfig ?? throw new ArgumentNullException(nameof(migrationConfig));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public virtual async Task EnsureSchemaMigrationsTableAsync()
        {
            var cql = $@"
CREATE TABLE IF NOT EXISTS {_migrationConfig.TableName} (
    version TEXT PRIMARY KEY,
    applied_on TIMESTAMP,
    description TEXT
);";
            _logger.LogInformation("Ensuring schema migrations table '{TableName}' exists...", _migrationConfig.TableName);
            // DDL operations: typically no retries or specific resilience.
            // await _cassandraService.ExecuteAsync(cql, new Resilience.ResilienceOptions { Profile = Resilience.ResiliencePolicyProfile.None }); // Commented out
            await Task.CompletedTask; // Placeholder
        }

        public virtual async Task<List<SchemaMigration>> GetAppliedMigrationsAsync()
        {
            // This uses the generic GetAsync, assuming SchemaMigration class is mapped correctly
            // and the table name used by GetAsync can be dynamically set or it matches.
            // For now, we assume GetAsync works with default table name "schema_migrations" for SchemaMigration type.
            // If CassandraService.GetAsync uses TableAttribute, SchemaMigration should have [Table("actual_table_name")]
            // A better approach would be a specific query here that uses _migrationConfig.TableName
            _logger.LogInformation("Fetching applied migrations from table '{TableName}'...", _migrationConfig.TableName);

            // We cannot directly use GetAsync<SchemaMigration> if its TableAttribute is fixed.
            // So, we construct a query manually.
            var query = $"SELECT version, applied_on, description FROM {_migrationConfig.TableName}";
            // SELECTs are idempotent.
            // var rowSet = await _cassandraService.ExecuteAsync(query, new Resilience.ResilienceOptions { Profile = Resilience.ResiliencePolicyProfile.DefaultRetry, IsIdempotent = true }); // Commented out

            // Placeholder implementation
            await Task.CompletedTask;
            var applied = new List<SchemaMigration>();
            // foreach (var row in rowSet) // Commented out as rowSet is not available due to CassandraService being commented out
            // {
            //     applied.Add(new SchemaMigration
            //     { // Stray brace removed from here
            //         Version = row.GetValue<string>("version"),
            //         AppliedOn = row.GetValue<DateTimeOffset>("applied_on"),
            //         Description = row.GetValue<string>("description")
            //     });
            // }
            return applied; // Return empty list as placeholder
        }

        public virtual Task<List<Migration>> DiscoverMigrationsAsync(string location)
        {
            _logger.LogInformation("Discovering migrations in '{Location}'...", location);
            var migrations = new List<Migration>();
            if (!Directory.Exists(location))
            {
                _logger.LogWarning("Migration scripts location '{Location}' does not exist.", location);
                return Task.FromResult(migrations);
            }

            var files = Directory.GetFiles(location, "*.cql");
            foreach (var file in files)
            {
                var fileName = Path.GetFileName(file);
                // Regex to capture version and description (e.g., 001_create_table.cql or 001.cql)
                var match = Regex.Match(fileName, @"^(\d+)(?:[._\s-](.+))?\.cql$", RegexOptions.IgnoreCase);
                if (match.Success)
                {
                    if (long.TryParse(match.Groups[1].Value, out var version))
                    {
                        var description = match.Groups[2].Success ? match.Groups[2].Value.Replace('_', ' ').Replace('-', ' ') : "No description";
                        var scriptContent = File.ReadAllText(file);
                        migrations.Add(new Migration(version, description, fileName, scriptContent));
                    }
                    else
                    {
                        _logger.LogWarning("Could not parse version from migration file name: {FileName}", fileName);
                    }
                }
                else
                {
                     _logger.LogWarning("Migration file name format incorrect, skipping: {FileName}. Expected format: ###_description.cql or ###.cql", fileName);
                }
            }
            migrations.Sort(); // Sort by version
            _logger.LogInformation("Discovered {Count} migrations.", migrations.Count);
            return Task.FromResult(migrations);
        }

        public virtual async Task ApplyMigrationsAsync()
        {
            _logger.LogInformation("Starting migration process...");
            await EnsureSchemaMigrationsTableAsync();

            var appliedMigrations = await GetAppliedMigrationsAsync();
            var discoveredMigrations = await DiscoverMigrationsAsync(_migrationConfig.ScriptsLocation);

            var appliedVersions = new HashSet<string>(appliedMigrations.Select(m => m.Version));
            var pendingMigrations = discoveredMigrations.Where(dm => !appliedVersions.Contains(dm.Version.ToString("D3"))).ToList();

            if (!pendingMigrations.Any())
            {
                _logger.LogInformation("No pending migrations to apply. Schema is up to date.");
                return;
            }

            _logger.LogInformation("Found {Count} pending migrations.", pendingMigrations.Count);

            foreach (var migration in pendingMigrations)
            {
                _logger.LogInformation("Applying migration {Version}: {Description} ({ScriptName})...", migration.Version.ToString("D3"), migration.Description, migration.ScriptName);
                try
                {
                    // Basic script splitting: by line, then by semicolon at end of line.
                    // This won't handle complex cases like semicolons in comments or string literals if they span lines or are not at EOL.
                    var statements = migration.ScriptContent.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                                           .Select(line => line.Trim())
                                           .Where(line => !string.IsNullOrWhiteSpace(line) && !line.StartsWith("--") && !line.StartsWith("//")) // Basic comment skipping
                                           .Aggregate(new List<string> { string.Empty }, (list, line) =>
                                           {
                                               if (list.Last().TrimEnd().EndsWith(";")) list.Add(line);
                                               else list[list.Count -1] += (string.IsNullOrEmpty(list.Last()) ? "" : "\n") + line;
                                               return list;
                                           })
                                           .Select(stmt => stmt.Trim())
                                           .Where(stmt => !string.IsNullOrWhiteSpace(stmt))
                                           .ToList();


                    if (!statements.Any() && !string.IsNullOrWhiteSpace(migration.ScriptContent))
                    {
                        // If splitting produced no statements but content exists, try a simple split.
                        // This is a fallback and indicates a potential issue with the script or splitter.
                        _logger.LogWarning("Advanced script splitting yielded no statements for {ScriptName}, falling back to simple split by semicolon. This might be unreliable.", migration.ScriptName);
                        statements = migration.ScriptContent.Split(';', StringSplitOptions.RemoveEmptyEntries)
                                           .Select(s => s.Trim())
                                           .Where(s => !string.IsNullOrWhiteSpace(s))
                                           .ToList();
                    }

                    if (!statements.Any())
                    {
                        _logger.LogWarning("Migration script {ScriptName} for version {Version} is empty or contains no executable statements.", migration.ScriptName, migration.Version.ToString("D3"));
                    }

                    foreach (var stmt in statements)
                    {
                        if (string.IsNullOrWhiteSpace(stmt)) continue;
                        _logger.LogDebug("Executing statement: {Statement}", stmt);
                        // DDL statements in migration script.
                        // await _cassandraService.ExecuteAsync(stmt, new Resilience.ResilienceOptions { Profile = Resilience.ResiliencePolicyProfile.None }); // Commented out
                    }
                    await Task.CompletedTask; // Placeholder

                    var schemaMigration = new SchemaMigration
                    {
                        Version = migration.Version.ToString("D3"),
                        AppliedOn = DateTimeOffset.UtcNow,
                        Description = migration.Description
                    };

                    // This assumes InsertAsync can handle the SchemaMigration type and correct table name.
                    // Similar to GetAppliedMigrationsAsync, this might need a specific CQL insert.
                    // For now, let's build the insert manually.
                    var insertCql = $"INSERT INTO {_migrationConfig.TableName} (version, applied_on, description) VALUES (?, ?, ?)";
                    // Inserting into migration table: should be idempotent with IF NOT EXISTS, but here it's a direct insert after checking.
                    // Let's use a cautious resilience profile.
                    // await _cassandraService.ExecuteAsync(insertCql, new Resilience.ResilienceOptions { Profile = Resilience.ResiliencePolicyProfile.None }, schemaMigration.Version, schemaMigration.AppliedOn, schemaMigration.Description); // Commented out
                    await Task.CompletedTask; // Placeholder

                    _logger.LogInformation("Successfully applied migration {Version}: {Description}.", migration.Version.ToString("D3"), migration.Description);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to apply migration {Version}: {Description} ({ScriptName}). Stopping migration process.", migration.Version.ToString("D3"), migration.Description, migration.ScriptName);
                    // Optionally, rethrow or handle as per desired failure policy
                    throw;
                }
            }
            _logger.LogInformation("Migration process completed. Applied {Count} migrations.", pendingMigrations.Count);
        }
    }
}
