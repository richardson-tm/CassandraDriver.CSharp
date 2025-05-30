# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build the solution
dotnet build

# Run all tests (requires Docker cluster to be running)
./scripts/setup-test-cluster.sh  # Start the 3-node Cassandra cluster first
dotnet test

# Run only unit tests (no Docker required)
dotnet test --filter "Category!=Integration"

# Run only integration tests
dotnet test --filter "Category=Integration"

# Create NuGet package
dotnet pack -c Release

# Quick connection test
dotnet run --project scripts/TestClusterConnection.csproj
```

## Architecture Overview

This is a C# port of a Java Ratpack Cassandra module, providing a production-ready Cassandra driver integration for .NET applications.

### Key Design Patterns

1. **Dependency Injection**: Full integration with Microsoft.Extensions.DependencyInjection
   - Services registered via `AddCassandra()` extension method
   - CassandraService is a singleton for connection pooling

2. **Hosted Service**: CassandraService implements IHostedService
   - Manages cluster lifecycle (connect on start, dispose on stop)
   - Integrates with ASP.NET Core application lifecycle

3. **Configuration**: Uses Options pattern with `IOptions<CassandraConfiguration>`
   - JSON configuration via appsettings.json
   - Supports contact points, keyspace, SSL/TLS, load balancing policies

4. **Health Checks**: Implements ASP.NET Core health checks
   - Validates cluster connectivity
   - Checks keyspace accessibility

### Testing Infrastructure

- **Docker Compose**: 3-node Cassandra 5.0 cluster for integration testing
  - Sequential startup prevents token collisions
  - Ports: 9042 (seed), 9043 (node1), 9044 (node2)
- **Test Categories**: Unit tests vs Integration tests (use filters)
- **Test Data**: Automated creation of test keyspace and tables

### Important Implementation Notes

1. **Speculative Execution**: Uses ConstantSpeculativeExecutionPolicy (PercentileSpeculativeExecutionPolicy not available in C# driver)

2. **EC2 Translation**: Currently logs warnings for EC2MultiRegionAddressTranslator - not fully implemented

3. **SSL/TLS**: Expects .pfx certificate files (different from Java's .jks format)

4. **Session Management**: 
   - Single ISession instance per CassandraService (singleton)
   - Prepared statements are cached internally by the driver

5. **Async Operations**: All database operations are async-only (no sync methods provided)