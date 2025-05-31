using System.Diagnostics.Metrics;

namespace CassandraDriver.Telemetry
{
    public static class DriverMetrics
    {
        public static readonly string MeterName = "CassandraDriver";
        public static readonly string MeterVersion = "1.0.0"; // Or align with assembly version

        public static readonly Meter Meter = new Meter(MeterName, MeterVersion);

        public static readonly Histogram<double> QueryDurationMilliseconds = Meter.CreateHistogram<double>(
            name: "cassandra.driver.query.duration",
            unit: "ms",
            description: "Duration of Cassandra query execution");

        public static readonly Counter<long> QueriesStarted = Meter.CreateCounter<long>(
            name: "cassandra.driver.queries.started",
            unit: "{queries}", // Using { } as per OpenTelemetry semantic conventions for units
            description: "Number of queries started");

        public static readonly Counter<long> QueriesSucceeded = Meter.CreateCounter<long>(
            name: "cassandra.driver.queries.succeeded",
            unit: "{queries}",
            description: "Number of queries successfully executed");

        public static readonly Counter<long> QueriesFailed = Meter.CreateCounter<long>(
            name: "cassandra.driver.queries.failed",
            unit: "{queries}",
            description: "Number of queries failed");

        // Example tag keys - define them as constants if used frequently
        public static class TagKeys
        {
            public const string CqlOperation = "db.cassandra.cql.operation"; // Following OpenTelemetry DB conventions
            public const string ExceptionType = "exception.type";
        }
    }
}
