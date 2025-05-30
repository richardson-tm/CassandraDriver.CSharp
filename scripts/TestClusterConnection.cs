using System;
using System.Threading;
using System.Threading.Tasks;
using CassandraDriver.Configuration;
using CassandraDriver.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CassandraDriver.Scripts
{
    /// <summary>
    /// Simple program to test Cassandra cluster connection
    /// Run with: dotnet run --project scripts/TestClusterConnection.csproj
    /// </summary>
    public class TestClusterConnection
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("=== Cassandra Cluster Connection Test ===\n");

            // Configure connection
            var config = new CassandraConfiguration
            {
                Seeds = new List<string> { "localhost:9042", "localhost:9043", "localhost:9044" },
                Keyspace = null // Connect without keyspace initially
            };

            var options = Options.Create(config);
            var logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<CassandraService>();
            
            using var cassandraService = new CassandraService(options, logger);

            try
            {
                // Start the service
                Console.WriteLine("Connecting to Cassandra cluster...");
                await cassandraService.StartAsync(CancellationToken.None);
                Console.WriteLine("✓ Connected successfully!\n");

                // Get cluster information
                var cluster = cassandraService.Cluster;
                var metadata = cluster.Metadata;
                
                Console.WriteLine($"Cluster name: {metadata.ClusterName}");
                Console.WriteLine($"Cassandra version: {cluster.AllHosts().First().CassandraVersion}");
                Console.WriteLine($"Connected hosts: {cluster.AllHosts().Count(h => h.IsUp)}");
                Console.WriteLine($"Keyspaces: {string.Join(", ", metadata.GetKeyspaces())}\n");

                // Switch to test keyspace
                await cassandraService.ExecuteAsync("USE test_keyspace");
                Console.WriteLine("✓ Switched to test_keyspace\n");

                // Test queries
                await TestUserQueries(cassandraService);
                await TestProductQueries(cassandraService);
                await TestEventQueries(cassandraService);
                await TestTimeSeriesQueries(cassandraService);

                Console.WriteLine("\n✓ All tests completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n✗ Error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
            finally
            {
                await cassandraService.StopAsync(CancellationToken.None);
            }
        }

        private static async Task TestUserQueries(CassandraService cassandra)
        {
            Console.WriteLine("Testing user queries:");
            
            // Count users
            var countResult = await cassandra.ExecuteAsync("SELECT COUNT(*) as count FROM users");
            var userCount = countResult.First().GetValue<long>("count");
            Console.WriteLine($"  - Total users: {userCount}");

            // Query specific user by ID
            var userResult = await cassandra.ExecuteAsync(
                "SELECT * FROM users WHERE user_id = ?", 
                Guid.Parse("550e8400-e29b-41d4-a716-446655440001"));
            
            Console.WriteLine($"  - User query returned {userResult.Count()} rows");
            
            if (userResult.Any())
            {
                var user = userResult.First();
                Console.WriteLine($"  - Found user: {user.GetValue<string>("username")} ({user.GetValue<string>("email")})");
            }

            // Query all active users
            var activeResult = await cassandra.ExecuteAsync(
                "SELECT username, email FROM users WHERE is_active = true ALLOW FILTERING");
            Console.WriteLine($"  - Active users: {activeResult.Count()}");
        }

        private static async Task TestProductQueries(CassandraService cassandra)
        {
            Console.WriteLine("\nTesting product queries:");
            
            // Get all products
            var allProducts = await cassandra.ExecuteAsync("SELECT * FROM products");
            Console.WriteLine($"  - Total products: {allProducts.Count()}");

            // Get in-stock products
            var inStockResult = await cassandra.ExecuteAsync(
                "SELECT name, price FROM products WHERE in_stock = true ALLOW FILTERING");
            
            foreach (var product in inStockResult)
            {
                Console.WriteLine($"  - In stock: {product.GetValue<string>("name")} - ${product.GetValue<decimal>("price")}");
            }
        }

        private static async Task TestEventQueries(CassandraService cassandra)
        {
            Console.WriteLine("\nTesting event queries:");
            
            // Get events for a specific user
            var userId = Guid.Parse("550e8400-e29b-41d4-a716-446655440001");
            var eventResult = await cassandra.ExecuteAsync(
                "SELECT * FROM events WHERE user_id = ? LIMIT 5",
                userId);
            
            Console.WriteLine($"  - Events for user {userId}: {eventResult.Count()}");
            
            foreach (var evt in eventResult)
            {
                Console.WriteLine($"    - {evt.GetValue<string>("event_type")} at {evt.GetValue<DateTime>("created_at")}");
            }
        }

        private static async Task TestTimeSeriesQueries(CassandraService cassandra)
        {
            Console.WriteLine("\nTesting time series queries:");
            
            // Get recent metrics
            var metricsResult = await cassandra.ExecuteAsync(
                "SELECT * FROM time_series WHERE metric_name = 'cpu_usage' LIMIT 10");
            
            Console.WriteLine($"  - CPU usage metrics: {metricsResult.Count()}");
            
            // Get all metric names
            var metricNames = await cassandra.ExecuteAsync(
                "SELECT DISTINCT metric_name FROM time_series");
            
            var names = metricNames.Select(r => r.GetValue<string>("metric_name")).ToList();
            Console.WriteLine($"  - Available metrics: {string.Join(", ", names)}");
        }
    }
}