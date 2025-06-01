# Changelog

All notable changes to the Cassandra C# Driver will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2025-01-06

### Added

#### Core Features
- **Object Mapping System**: Complete attribute-based entity mapping
  - `[Table]`, `[PartitionKey]`, `[ClusteringKey]`, `[Column]` attributes
  - `[Ignore]` for excluding properties from mapping
  - `[Computed]` for calculated columns
  - `[Udt]` for User-Defined Type support
  - Automatic property-to-column mapping with naming conventions

- **Query Builders**: Type-safe, fluent API for all CRUD operations
  - `SelectQueryBuilder<T>` with Where, OrderBy, Limit, paging support
  - `InsertQueryBuilder<T>` with TTL and IF NOT EXISTS support
  - `UpdateQueryBuilder<T>` with conditional updates
  - `DeleteQueryBuilder<T>` for safe deletions
  - Expression-based property selection

- **LINQ Provider**: Full LINQ to CQL translation
  - IQueryable implementation for Cassandra
  - Support for Where, Select, OrderBy, Take operations
  - Expression tree to CQL query translation
  - Async query execution

#### Advanced Features
- **Schema Management**:
  - `SchemaGenerator` for automatic CREATE TABLE statements from entities
  - `SchemaManager` for schema synchronization
  - Migration system with version tracking
  - Support for SimpleStrategy and NetworkTopologyStrategy
  - Automatic keyspace creation with configurable replication

- **Resilience Patterns**:
  - Polly-based retry policies with exponential backoff
  - Circuit breaker pattern implementation
  - Configurable failure thresholds and break durations
  - Transient exception detection

- **Telemetry & Metrics**:
  - OpenTelemetry integration with `DriverMetrics`
  - Query performance metrics (start, success, failure counts)
  - Query duration histograms
  - Prepared statement cache metrics
  - Tagged metrics for operation types

- **Advanced Cassandra Features**:
  - Lightweight Transactions (LWT) with `LwtResult<T>`
  - TTL support on INSERT and UPDATE operations
  - User-Defined Types (UDT) mapping
  - Consistency level configuration (per-query and default)
  - Serial consistency level support

- **Developer Experience**:
  - `CassandraMapperService` for high-level CRUD operations
  - `CassandraStartupInitializer` for automatic schema setup
  - Reactive Extensions (Rx.NET) integration
  - IAsyncEnumerable support for streaming large datasets
  - Comprehensive IntelliSense documentation

### Changed
- **Breaking**: Refactored `CassandraService` architecture
  - Moved mapping logic to dedicated `TableMappingResolver`
  - Added dependency on `ILoggerFactory` for migration logging
  - Enhanced with generic CRUD methods
  - Added `Query<T>()` method for fluent queries

- **Breaking**: Enhanced configuration structure
  - Added `RetryPolicy` configuration section
  - Added `CircuitBreaker` configuration section
  - Added `Migrations` configuration section
  - Extended `Pooling` and `QueryOptions` configurations

- Improved error handling throughout the codebase
- Updated all projects to use nullable reference types
- Enhanced logging with structured logging patterns

### Fixed
- Cassandra driver v3 API compatibility issues
  - Removed unsupported pooling options (MinRequestsPerConnectionThreshold, MaxQueueSize)
  - Fixed nullable ConsistencyLevel handling
  - Updated to use driver v3 method signatures
- Statement caching thread-safety with ConcurrentDictionary
- Proper disposal of resources in all services

### Security
- No changes to security model

## [1.0.0] - 2024-12-15

### Added
- Initial C# port of Java Ratpack Cassandra driver
- Complete implementation of core components:
  - `CassandraConfiguration` - Configuration management with all Java options
  - `CassandraService` - Connection and query management with async operations
  - `CassandraHealthCheck` - ASP.NET Core health check integration
  - `ServiceCollectionExtensions` - Clean DI registration
- Comprehensive test suite:
  - 29 unit tests covering all components
  - 4 integration tests for real Cassandra operations
  - Mock-based testing for isolated unit tests
- Docker testing environment:
  - 3-node Cassandra 5.0 cluster configuration
  - Automated setup scripts
  - Test data generation
  - Sequential node startup to prevent token collisions
- Documentation:
  - Main README with quick start guide
  - Detailed C# port implementation guide
  - Docker testing documentation
  - Session summary with technical details
  - Quick start guide for rapid testing

### Changed
- Adapted Java APIs to C# idioms:
  - `Promise<ResultSet>` → `Task<RowSet>`
  - Google Guice → Microsoft.Extensions.DependencyInjection
  - JKS certificates → X509Certificate2
  - Builder patterns → Constructor initialization
- Modified speculative execution:
  - Java's `PercentileSpeculativeExecutionPolicy` → C#'s `ConstantSpeculativeExecutionPolicy`
  - Maintained functionality with different implementation

### Fixed
- DataStax C# driver API compatibility issues:
  - `DCAwareRoundRobinPolicy` constructor usage
  - SSL options method naming (`withSSLOptions` → `WithSSL`)
  - Protocol version configuration
- Integration test failures:
  - Fixed data conflicts by dropping tables before tests
  - Updated TestClusterConnection to handle keyspace switching
- Docker cluster startup issues:
  - Implemented sequential node startup using health checks
  - Fixed bootstrap token collision errors

### Security
- Full SSL/TLS support with certificate authentication
- Secure credential handling through configuration
- No hardcoded secrets or credentials

### Known Limitations
- EC2 address translation not fully implemented (logs warning)
- Host class mocking limitations in unit tests
- Speculative execution uses constant delay instead of percentile-based

## [0.1.0] - 2024-12-01

### Added
- Project initialization
- Basic project structure
- Initial documentation

[2.0.0]: https://github.com/richardson-tm/CassandraDriver.CSharp/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/richardson-tm/CassandraDriver.CSharp/releases/tag/v1.0.0
[0.1.0]: https://github.com/richardson-tm/CassandraDriver.CSharp/releases/tag/v0.1.0