# Changelog

All notable changes to the Cassandra C# Driver will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-05-30

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
- No reactive extensions support (RxCassandraService not ported)
- No migration service (CassandraMigrationService not ported)

## Future Releases

### [1.1.0] - Planned
- Complete EC2 multi-region address translation
- Add System.Reactive support for RxCassandraService
- Implement connection resilience patterns (circuit breaker, retry)
- Add performance metrics and monitoring integration

### [1.2.0] - Planned
- Port CassandraMigrationService for schema management
- Add distributed tracing support
- Implement advanced load balancing strategies
- Add connection pool tuning options