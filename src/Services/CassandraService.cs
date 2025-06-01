using Cassandra;
using CassandraDriver.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
// using Cassandra.Mapping; // This is the driver's mapping; we are creating a custom one. Keep for now.
// Exception types are in the main Cassandra namespace
using CassandraDriver.Mapping; // Our new mapping namespace
using System.Text; // For StringBuilder
using System.Linq; // For Linq operations
using System.Reflection; // For Activator.CreateInstance
using CassandraDriver.Queries; // For SelectQueryBuilder
using System.Collections.Concurrent; // For prepared statement cache
using CassandraDriver.Migrations; // For Migration Support
using CassandraDriver.Telemetry; // For DriverMetrics
using System.Diagnostics; // For Stopwatch
using System.Collections.Generic; // For KeyValuePair in metrics
using CassandraDriver.Results; // For LwtResult<T>

namespace CassandraDriver.Services;

public class CassandraService : IHostedService, IDisposable
{
    private readonly CassandraConfiguration _configuration;
    private readonly ILogger<CassandraService> _logger;
    private readonly ILoggerFactory _loggerFactory; // For MigrationRunner
    private readonly ConsistencyLevel? _defaultConsistencyLevel;
    private readonly ConsistencyLevel? _defaultSerialConsistencyLevel;
    private readonly IAsyncPolicy<RowSet> _retryPolicy;
    private readonly TableMappingResolver _mappingResolver;
    private readonly ConcurrentDictionary<string, PreparedStatement> _preparedStatementCache;
    private ICluster? _cluster;
    private ISession? _session;
    private readonly string[] _cipherSuites = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" };

    public CassandraService(
        IOptions<CassandraConfiguration> configuration,
        ILogger<CassandraService> logger,
        TableMappingResolver mappingResolver,
        ILoggerFactory loggerFactory)
    {
        _configuration = configuration.Value;
        _logger = logger;
        _loggerFactory = loggerFactory;
        _mappingResolver = mappingResolver;
        _preparedStatementCache = new ConcurrentDictionary<string, PreparedStatement>();
        _defaultConsistencyLevel = _configuration.DefaultConsistencyLevel;
        _defaultSerialConsistencyLevel = _configuration.DefaultSerialConsistencyLevel;

        var retryPolicyConfig = _configuration.RetryPolicy;

        if (retryPolicyConfig.Enabled)
        {
            _retryPolicy = Policy<RowSet>.Handle<DriverException>(ex => ex is OperationTimedOutException ||
                                                              ex is UnavailableException ||
                                                              ex is OverloadedException ||
                                                              ex is WriteTimeoutException ||
                                                              ex is ReadTimeoutException)
                .WaitAndRetryAsync(
                    retryPolicyConfig.MaxRetries,
                    retryAttempt => TimeSpan.FromMilliseconds(Math.Min(Math.Pow(2, retryAttempt) * retryPolicyConfig.DelayMilliseconds, retryPolicyConfig.MaxDelayMilliseconds)),
                    onRetry: (outcome, timeSpan, retryCount, context) =>
                    {
                        _logger.LogWarning(outcome.Exception, "Retry {RetryCount} due to {ExceptionType}. Waiting {TimeSpanTotalSeconds}s before next attempt",
                            retryCount, outcome.Exception?.GetType().Name ?? "Unknown", timeSpan.TotalSeconds);
                    }
                );
        }
        else
        {
            _retryPolicy = Policy.NoOpAsync<RowSet>();
        }
    }

    public virtual ISession Session
    {
        get
        {
            if (_session == null)
                throw new InvalidOperationException("Session is not initialized. Ensure StartAsync has been called.");
            return _session;
        }
    }

    public virtual ICluster Cluster
    {
        get
        {
            if (_cluster == null)
                throw new InvalidOperationException("Cluster is not initialized. Ensure StartAsync has been called.");
            return _cluster;
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await Task.Run(() => Connect(), cancellationToken).ConfigureAwait(false);

        if (_configuration.Migrations.Enabled)
        {
            _logger.LogInformation("Cassandra migrations enabled. Starting migration process...");
            try
            {
                var migrationRunnerLogger = _loggerFactory.CreateLogger<CassandraMigrationRunner>();
                var migrationRunner = new CassandraMigrationRunner(this, _configuration.Migrations, migrationRunnerLogger);
                await migrationRunner.ApplyMigrationsAsync().ConfigureAwait(false);
                _logger.LogInformation("Cassandra migration process completed.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Cassandra migration process failed.");
            }
        }
        else
        {
            _logger.LogInformation("Cassandra migrations are disabled.");
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_session != null)
        {
            await Task.Run(() => _session.Dispose(), cancellationToken).ConfigureAwait(false);
        }

        if (_cluster != null)
        {
            await Task.Run(() => _cluster.Dispose(), cancellationToken).ConfigureAwait(false);
        }
    }

    protected virtual void Connect()
    {
        var builder = CreateClusterBuilder();
        var dcAwarePolicy = new DCAwareRoundRobinPolicy();
        builder.WithLoadBalancingPolicy(new TokenAwarePolicy(dcAwarePolicy));

        if (_configuration.SpeculativeExecutionEnabled)
        {
            var speculativePolicy = new ConstantSpeculativeExecutionPolicy(
                delay: 100,
                maxSpeculativeExecutions: _configuration.MaxSpeculativeExecutions);
            builder.WithSpeculativeExecutionPolicy(speculativePolicy);
        }

        foreach (var seed in _configuration.Seeds)
        {
            if (seed.Contains(":"))
            {
                var tokens = seed.Split(':');
                builder.AddContactPoint(tokens[0]).WithPort(int.Parse(tokens[1]));
            }
            else
            {
                builder.AddContactPoint(seed);
            }
        }

        if (_configuration.Ec2TranslationEnabled)
        {
            _logger.LogWarning("EC2 address translation requested but not implemented in C# port");
        }

        if (_configuration.Truststore != null && _configuration.Keystore != null)
        {
            try
            {
                var sslOptions = ConfigureSsl(_configuration.Truststore, _configuration.Keystore);
                builder.WithSSL(sslOptions);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Couldn't add SSL to the cluster builder.");
            }
        }

        if (!string.IsNullOrEmpty(_configuration.User))
        {
            builder.WithCredentials(_configuration.User, _configuration.Password);
        }

        if (_configuration.ProtocolVersion.HasValue)
        {
            builder.WithMaxProtocolVersion((ProtocolVersion)_configuration.ProtocolVersion.Value);
        }

        if (_configuration.Pooling != null)
        {
            builder = ConfigurePoolingOptions(builder, _configuration.Pooling);
        }

        if (_configuration.QueryOptions != null)
        {
            builder = ConfigureQueryOptions(builder, _configuration.QueryOptions);
        }

        _cluster = builder.Build();

        if (!string.IsNullOrEmpty(_configuration.Keyspace))
        {
            _session = _cluster.Connect(_configuration.Keyspace);
        }
        else
        {
            _session = _cluster.Connect();
        }
        _logger.LogInformation("Successfully connected to Cassandra cluster");
    }

    protected virtual Builder CreateClusterBuilder()
    {
        return Cassandra.Cluster.Builder();
    }

    protected virtual Builder ConfigurePoolingOptions(Builder builder, PoolingOptionsConfiguration poolingConfig)
    {
        var poolingOptions = new Cassandra.PoolingOptions();
        if (poolingConfig.CoreConnectionsPerHostLocal.HasValue)
            poolingOptions.SetCoreConnectionsPerHost(HostDistance.Local, poolingConfig.CoreConnectionsPerHostLocal.Value);
        if (poolingConfig.MaxConnectionsPerHostLocal.HasValue)
            poolingOptions.SetMaxConnectionsPerHost(HostDistance.Local, poolingConfig.MaxConnectionsPerHostLocal.Value);
        // ... (rest of pooling options as before)
        if (poolingConfig.CoreConnectionsPerHostRemote.HasValue)
            poolingOptions.SetCoreConnectionsPerHost(HostDistance.Remote, poolingConfig.CoreConnectionsPerHostRemote.Value);
        if (poolingConfig.MaxConnectionsPerHostRemote.HasValue)
            poolingOptions.SetMaxConnectionsPerHost(HostDistance.Remote, poolingConfig.MaxConnectionsPerHostRemote.Value);
        // MinRequestsPerConnectionThreshold not available in driver v3
        // if (poolingConfig.MinRequestsPerConnectionThresholdLocal.HasValue)
        //     poolingOptions.SetMinRequestsPerConnectionThreshold(HostDistance.Local, poolingConfig.MinRequestsPerConnectionThresholdLocal.Value);
        // if (poolingConfig.MinRequestsPerConnectionThresholdRemote.HasValue)
        //     poolingOptions.SetMinRequestsPerConnectionThreshold(HostDistance.Remote, poolingConfig.MinRequestsPerConnectionThresholdRemote.Value);
        if (poolingConfig.MaxRequestsPerConnection.HasValue)
            poolingOptions.SetMaxRequestsPerConnection(poolingConfig.MaxRequestsPerConnection.Value);
        // MaxQueueSize not available in driver v3
        // if (poolingConfig.MaxQueueSize.HasValue)
        //     poolingOptions.SetMaxQueueSize(poolingConfig.MaxQueueSize.Value);
        // HeartbeatInterval configured at SocketOptions level in driver v3
        // if (poolingConfig.HeartbeatIntervalMillis.HasValue)
        //     poolingOptions.SetHeartbeatInterval(poolingConfig.HeartbeatIntervalMillis.Value);
        // SetPoolTimeoutMillis not available in driver v3
        // if (poolingConfig.PoolTimeoutMillis.HasValue)
        //     poolingOptions.SetPoolTimeoutMillis(poolingConfig.PoolTimeoutMillis.Value);
        return builder.WithPoolingOptions(poolingOptions);
    }

    protected virtual Builder ConfigureQueryOptions(Builder builder, QueryOptionsConfiguration queryConfig)
    {
        var queryOptions = new Cassandra.QueryOptions();
        if (queryConfig.DefaultPageSize.HasValue)
            queryOptions.SetPageSize(queryConfig.DefaultPageSize.Value);
        if (queryConfig.PrepareStatementsOnAllHosts.HasValue)
            queryOptions.SetPrepareOnAllHosts(queryConfig.PrepareStatementsOnAllHosts.Value);
        if (queryConfig.ReprepareStatementsOnUp.HasValue)
            queryOptions.SetReprepareOnUp(queryConfig.ReprepareStatementsOnUp.Value);
        // MetadataSyncEnabled not available on QueryOptions in driver v3
        // if (queryConfig.DefaultMetadataSyncEnabled.HasValue)
        //     queryOptions.SetMetadataSyncEnabled(queryConfig.DefaultMetadataSyncEnabled.Value);
        return builder.WithQueryOptions(queryOptions);
    }

    private SSLOptions ConfigureSsl(SslConfiguration truststore, SslConfiguration keystore)
    {
        if (string.IsNullOrEmpty(truststore.Path) || string.IsNullOrEmpty(keystore.Path))
        {
            throw new ArgumentException("SSL configuration requires both truststore and keystore paths");
        }
        var sslOptions = new SSLOptions();
        var clientCert = new X509Certificate2(keystore.Path, keystore.Password);
        sslOptions.SetCertificateCollection(new X509CertificateCollection { clientCert });
        sslOptions.SetRemoteCertValidationCallback((sender, certificate, chain, errors) =>
        {
            if (errors == SslPolicyErrors.None) return true;
            _logger.LogWarning("SSL certificate validation errors: {Errors}", errors);
            return errors == SslPolicyErrors.RemoteCertificateChainErrors;
        });
        return sslOptions;
    }

    private static string GetCqlOperationType(IStatement statement)
    {
        if (statement is SimpleStatement simpleStatement) return GetCqlOperationType(simpleStatement.QueryString);
        if (statement is BoundStatement)
        {
            // In driver v3, we can't access the CQL from BoundStatement
            return "BOUND";
        }
        return "unknown";
    }

    private static string GetCqlOperationType(string? cql)
    {
        if (string.IsNullOrWhiteSpace(cql)) return "unknown";
        var trimmedCql = cql.TrimStart();
        if (trimmedCql.StartsWith("--") || trimmedCql.StartsWith("//"))
        {
            var newlineIndex = trimmedCql.IndexOfAny(new[] { '\r', '\n' });
            if (newlineIndex != -1) trimmedCql = trimmedCql.Substring(newlineIndex + 1).TrimStart();
            else return "comment";
        }
        if (trimmedCql.StartsWith("/*"))
        {
            var endIndex = trimmedCql.IndexOf("*/");
            if (endIndex != -1) trimmedCql = trimmedCql.Substring(endIndex + 2).TrimStart();
            else return "comment";
        }
        var firstWordEndIndex = trimmedCql.IndexOfAny(new[] { ' ', '\r', '\n', '\t', ';' });
        var firstWord = (firstWordEndIndex == -1 ? trimmedCql : trimmedCql.Substring(0, firstWordEndIndex)).ToUpperInvariant();
        return firstWord switch
        {
            "SELECT" => "SELECT", "INSERT" => "INSERT", "UPDATE" => "UPDATE", "DELETE" => "DELETE",
            "BATCH" => "BATCH", "CREATE" => "CREATE", "ALTER" => "ALTER", "DROP" => "DROP",
            "TRUNCATE" => "TRUNCATE", _ => "OTHER"
        };
    }

    public virtual async Task<RowSet> ExecuteAsync(IStatement statement)
    {
        return await ExecuteAsync(statement, null, null).ConfigureAwait(false);
    }

    public virtual async Task<RowSet> ExecuteAsync(string cql, params object[] values)
    {
        return await ExecuteAsync(cql, null, null, values).ConfigureAwait(false);
    }

    public virtual async Task<RowSet> ExecuteAsync(IStatement statement, ConsistencyLevel? consistencyLevel, ConsistencyLevel? serialConsistencyLevel)
    {
        if (Session == null)
            throw new InvalidOperationException("Session is not initialized. Ensure StartAsync has been called.");

        DriverMetrics.QueriesStarted.Add(1);
        var stopwatch = Stopwatch.StartNew();
        string operationType = "unknown";
        try
        {
            operationType = GetCqlOperationType(statement);
            if (consistencyLevel.HasValue || _defaultConsistencyLevel.HasValue)
                statement.SetConsistencyLevel((consistencyLevel ?? _defaultConsistencyLevel)!.Value);
            if (serialConsistencyLevel.HasValue || _defaultSerialConsistencyLevel.HasValue)
                statement.SetSerialConsistencyLevel((serialConsistencyLevel ?? _defaultSerialConsistencyLevel)!.Value);
            var result = await _retryPolicy.ExecuteAsync(async () => await Session.ExecuteAsync(statement).ConfigureAwait(false));
            DriverMetrics.QueriesSucceeded.Add(1, new KeyValuePair<string, object?>(DriverMetrics.TagKeys.CqlOperation, operationType));
            return result;
        }
        catch (Exception ex)
        {
            DriverMetrics.QueriesFailed.Add(1,
                new KeyValuePair<string, object?>(DriverMetrics.TagKeys.CqlOperation, operationType),
                new KeyValuePair<string, object?>(DriverMetrics.TagKeys.ExceptionType, ex.GetType().Name));
            throw;
        }
        finally
        {
            stopwatch.Stop();
            DriverMetrics.QueryDurationMilliseconds.Record(stopwatch.Elapsed.TotalMilliseconds,
                new KeyValuePair<string, object?>(DriverMetrics.TagKeys.CqlOperation, operationType));
        }
    }

    public virtual async Task<RowSet> ExecuteAsync(string cql, ConsistencyLevel? consistencyLevel, ConsistencyLevel? serialConsistencyLevel, params object[] values)
    {
        if (Session == null)
            throw new InvalidOperationException("Session is not initialized. Ensure StartAsync has been called.");

        DriverMetrics.QueriesStarted.Add(1);
        var stopwatch = Stopwatch.StartNew();
        string operationType = GetCqlOperationType(cql);
        try
        {
            if (!_preparedStatementCache.TryGetValue(cql, out PreparedStatement? preparedStatement))
            {
                preparedStatement = await Session.PrepareAsync(cql).ConfigureAwait(false);
                _preparedStatementCache[cql] = preparedStatement;
            }
            var boundStatement = preparedStatement.Bind(values);
            if (consistencyLevel.HasValue || _defaultConsistencyLevel.HasValue)
                boundStatement.SetConsistencyLevel((consistencyLevel ?? _defaultConsistencyLevel)!.Value);
            if (serialConsistencyLevel.HasValue || _defaultSerialConsistencyLevel.HasValue)
                boundStatement.SetSerialConsistencyLevel((serialConsistencyLevel ?? _defaultSerialConsistencyLevel)!.Value);
            var result = await _retryPolicy.ExecuteAsync(async () => await Session.ExecuteAsync(boundStatement).ConfigureAwait(false));
            DriverMetrics.QueriesSucceeded.Add(1, new KeyValuePair<string, object?>(DriverMetrics.TagKeys.CqlOperation, operationType));
            return result;
        }
        catch (Exception ex)
        {
            DriverMetrics.QueriesFailed.Add(1,
               new KeyValuePair<string, object?>(DriverMetrics.TagKeys.CqlOperation, operationType),
               new KeyValuePair<string, object?>(DriverMetrics.TagKeys.ExceptionType, ex.GetType().Name));
            throw;
        }
        finally
        {
            stopwatch.Stop();
            DriverMetrics.QueryDurationMilliseconds.Record(stopwatch.Elapsed.TotalMilliseconds,
                new KeyValuePair<string, object?>(DriverMetrics.TagKeys.CqlOperation, operationType));
        }
    }

    private Task<LwtResult<T>> ParseLwtResult<T>(RowSet rowSet, TableMappingInfo mappingInfo, T? originalEntity = null) where T : class, new()
    {
        var firstRow = rowSet.FirstOrDefault();
        bool applied = false;
        T? currentEntity = null;

        if (firstRow != null)
        {
            try
            {
                applied = firstRow.GetValue<bool>("[applied]");
            }
            catch
            {
                // Column doesn't exist
            }
            if (!applied)
            {
                // If not applied, Cassandra returns current values of columns in IF condition for updates
                // For inserts, if IF NOT EXISTS fails, it might return existing data or just [applied]=false
                // We try to map this row back to T.
                currentEntity = MapRowToEntity<T>(firstRow, mappingInfo);
            }
        }
        else if (firstRow != null) // LWT rowset but no [applied] column? Or regular DML.
        {
            _logger.LogWarning("LWT operation was expected to return an '[applied]' column, but it was not found. Assuming not applied or non-LWT operation.");
            // This could be a non-LWT operation that happened to return one row.
            // Or an LWT that succeeded without returning [applied] (very unlikely for IF conditions).
            // If it's an insert without IF NOT EXISTS, it's "applied" by definition if no error.
            // For safety, assume not applied if [applied] is missing in an LWT context.
            // However, the caller (InsertAsync/UpdateAsync) will know if it was an LWT.
            // The methods below will set 'applied' based on their specific logic.
        }

        return Task.FromResult(new LwtResult<T>(applied, rowSet, currentEntity ?? originalEntity));
    }


    public void Dispose()
    {
        _session?.Dispose();
        _cluster?.Dispose();
        _preparedStatementCache.Clear();
    }

    public virtual async Task<T?> GetAsync<T>(params object[] primaryKeyComponents) where T : class, new()
    {
        var mappingInfo = _mappingResolver.GetMappingInfo(typeof(T));
        var selectColumns = string.Join(", ", mappingInfo.Properties
            .Where(p => !p.IsIgnored)
            .Select(p => p.IsComputed ? $"{p.ComputedExpression} AS \"{p.ColumnName}\"" : $"\"{p.ColumnName}\"")); // Quote column names
        var queryBuilder = new StringBuilder($"SELECT {selectColumns} FROM \"{mappingInfo.TableName}\""); // Quote table name
        var whereClause = new StringBuilder();
        var parameters = new List<object>();
        var allKeys = mappingInfo.PartitionKeys.Concat(mappingInfo.ClusteringKeys)
            .OrderBy(k => k.IsPartitionKey ? k.PartitionKeyOrder : 1000 + k.ClusteringKeyOrder)
            .ToList();

        if (primaryKeyComponents.Length != allKeys.Count)
        {
            throw new ArgumentException($"Incorrect number of primary key components for {typeof(T).FullName}. Expected {allKeys.Count}, got {primaryKeyComponents.Length}.", nameof(primaryKeyComponents));
        }

        for (int i = 0; i < allKeys.Count; i++)
        {
            var keyProperty = allKeys[i];
            if (i > 0) whereClause.Append(" AND ");
            whereClause.Append($"\"{keyProperty.ColumnName}\" = ?");
            parameters.Add(primaryKeyComponents[i]);
        }

        if (whereClause.Length > 0) queryBuilder.Append($" WHERE {whereClause}");
        else _logger.LogWarning("Executing GetAsync for type {EntityType} without where clause.", typeof(T).FullName);

        var statement = new SimpleStatement(queryBuilder.ToString(), parameters.ToArray());
        var rowSet = await ExecuteAsync(statement);
        var firstRow = rowSet.FirstOrDefault();
        return firstRow != null ? MapRowToEntity<T>(firstRow, mappingInfo) : null;
    }

    public virtual async Task<LwtResult<T>> InsertAsync<T>(T entity, bool ifNotExists = false, int? ttl = null, ConsistencyLevel? consistencyLevel = null, ConsistencyLevel? serialConsistencyLevel = null) where T : class, new()
    {
        var mappingInfo = _mappingResolver.GetMappingInfo(typeof(T));
        var columnsForInsert = mappingInfo.Properties.Where(p => !p.IsIgnored && !p.IsComputed).ToList();
        var columnNames = string.Join(", ", columnsForInsert.Select(p => $"\"{p.ColumnName}\""));
        var valuePlaceholders = string.Join(", ", columnsForInsert.Select(_ => "?"));

        var queryBuilder = new StringBuilder($"INSERT INTO \"{mappingInfo.TableName}\" ({columnNames}) VALUES ({valuePlaceholders})");
        List<object?> queryParameters = columnsForInsert.Select(p => p.PropertyInfo.GetValue(entity)).ToList();

        if (ifNotExists) queryBuilder.Append(" IF NOT EXISTS");
        if (ttl.HasValue)
        {
            queryBuilder.Append($" USING TTL ?");
            queryParameters.Add(ttl.Value);
        }

        var cql = queryBuilder.ToString();
        var effectiveSerialConsistency = ifNotExists ? (serialConsistencyLevel ?? _defaultSerialConsistencyLevel ?? ConsistencyLevel.Serial) : serialConsistencyLevel;

        RowSet rowSet = await ExecuteAsync(cql, consistencyLevel, effectiveSerialConsistency, queryParameters.ToArray());

        if (ifNotExists)
        {
            return await ParseLwtResult<T>(rowSet, mappingInfo, entity);
        }
        // For non-LWT inserts, assume applied if no exception. Entity in LwtResult will be the input entity.
        return new LwtResult<T>(true, rowSet, entity);
    }

    public virtual async Task<LwtResult<T>> UpdateAsync<T>(T entity, int? ttl = null, string? ifCondition = null, ConsistencyLevel? consistencyLevel = null, ConsistencyLevel? serialConsistencyLevel = null) where T : class, new()
    {
        var mappingInfo = _mappingResolver.GetMappingInfo(typeof(T));
        var setClause = new StringBuilder();
        var whereClause = new StringBuilder();
        var setParameters = new List<object?>();
        var whereParameters = new List<object?>();

        var nonKeyColumns = mappingInfo.Properties.Where(p => !p.IsPartitionKey && !p.IsClusteringKey && !p.IsIgnored && !p.IsComputed).ToList();

        if (!nonKeyColumns.Any())
        {
            _logger.LogWarning("UpdateAsync called for type {EntityType} with no non-key columns to update.", typeof(T).FullName);
            // Return a non-applied LWT result as no operation was performed.
            return new LwtResult<T>(false, new RowSet(), entity);
        }

        foreach (var propMap in nonKeyColumns)
        {
            if (setParameters.Any()) setClause.Append(", ");
            setClause.Append($"\"{propMap.ColumnName}\" = ?");
            setParameters.Add(propMap.PropertyInfo.GetValue(entity));
        }

        var allKeys = mappingInfo.PartitionKeys.Concat(mappingInfo.ClusteringKeys).OrderBy(k => k.IsPartitionKey ? k.PartitionKeyOrder : 1000 + k.ClusteringKeyOrder).ToList();
        for (int i = 0; i < allKeys.Count; i++)
        {
            var keyProp = allKeys[i];
            if (i > 0) whereClause.Append(" AND ");
            whereClause.Append($"\"{keyProp.ColumnName}\" = ?");
            whereParameters.Add(keyProp.PropertyInfo.GetValue(entity));
        }

        var queryBuilder = new StringBuilder($"UPDATE \"{mappingInfo.TableName}\"");
        List<object?> finalParameters = new List<object?>();

        if (ttl.HasValue)
        {
            queryBuilder.Append($" USING TTL ?");
            finalParameters.Add(ttl.Value);
        }
        queryBuilder.Append($" SET {setClause} WHERE {whereClause}");
        finalParameters.AddRange(setParameters); // SET parameters come after TTL
        finalParameters.AddRange(whereParameters); // WHERE parameters last

        if (!string.IsNullOrWhiteSpace(ifCondition))
        {
            queryBuilder.Append($" IF {ifCondition}");
        }

        var cql = queryBuilder.ToString();
        var effectiveSerialConsistency = !string.IsNullOrWhiteSpace(ifCondition) ? (serialConsistencyLevel ?? _defaultSerialConsistencyLevel ?? ConsistencyLevel.Serial) : serialConsistencyLevel;

        RowSet rowSet = await ExecuteAsync(cql, consistencyLevel, effectiveSerialConsistency, finalParameters.ToArray());

        if (!string.IsNullOrWhiteSpace(ifCondition))
        {
            return await ParseLwtResult<T>(rowSet, mappingInfo, entity);
        }
        // For non-LWT updates, assume applied if no exception.
        return new LwtResult<T>(true, rowSet, entity);
    }

    public virtual async Task DeleteAsync<T>(params object[] primaryKeyComponents) where T : class
    {
        var mappingInfo = _mappingResolver.GetMappingInfo(typeof(T));
        var queryBuilder = new StringBuilder($"DELETE FROM \"{mappingInfo.TableName}\""); // Quote table name
        var whereClause = new StringBuilder();
        var allKeys = mappingInfo.PartitionKeys.Concat(mappingInfo.ClusteringKeys)
            .OrderBy(k => k.IsPartitionKey ? k.PartitionKeyOrder : 1000 + k.ClusteringKeyOrder)
            .ToList();

        if (primaryKeyComponents.Length != allKeys.Count)
        {
            throw new ArgumentException($"Incorrect number of primary key components for {typeof(T).FullName}. Expected {allKeys.Count}, got {primaryKeyComponents.Length}.", nameof(primaryKeyComponents));
        }

        for (int i = 0; i < allKeys.Count; i++)
        {
            var keyProperty = allKeys[i];
            if (i > 0) whereClause.Append(" AND ");
            whereClause.Append($"\"{keyProperty.ColumnName}\" = ?"); // Quote column names
        }

        if (whereClause.Length == 0)
        {
            throw new InvalidOperationException($"Delete operation for type {typeof(T).FullName} must have a WHERE clause based on primary keys.");
        }
        queryBuilder.Append($" WHERE {whereClause}");
        await ExecuteAsync(queryBuilder.ToString(), null, null, primaryKeyComponents);
    }

    public virtual async Task DeleteAsync<T>(T entity, ConsistencyLevel? consistencyLevel = null) where T : class
    {
        var mappingInfo = _mappingResolver.GetMappingInfo(typeof(T));
        var pkPropertyValues = mappingInfo.PartitionKeys.Concat(mappingInfo.ClusteringKeys)
            .OrderBy(k => k.IsPartitionKey ? k.PartitionKeyOrder : 1000 + k.ClusteringKeyOrder)
            .Select(p => p.PropertyInfo.GetValue(entity))
            .ToArray();

        if (pkPropertyValues.Any(v => v == null))
        {
            throw new ArgumentException("Primary key components cannot be null for delete by entity operation.", nameof(entity));
        }

        var whereClause = new StringBuilder();
        var allKeys = mappingInfo.PartitionKeys.Concat(mappingInfo.ClusteringKeys)
            .OrderBy(k => k.IsPartitionKey ? k.PartitionKeyOrder : 1000 + k.ClusteringKeyOrder)
            .ToList();
        for (int i = 0; i < allKeys.Count; i++)
        {
            if (i > 0) whereClause.Append(" AND ");
            whereClause.Append($"\"{allKeys[i].ColumnName}\" = ?"); // Quote column names
        }
        var cql = $"DELETE FROM \"{mappingInfo.TableName}\" WHERE {whereClause}"; // Quote table name
        await ExecuteAsync(cql, consistencyLevel, null, pkPropertyValues!);
    }

    public T MapRowToEntity<T>(Row row, TableMappingInfo mappingInfo) where T : class
    {
        var entity = (T)Activator.CreateInstance(typeof(T))!;
        foreach (var propMap in mappingInfo.Properties.Where(p => !p.IsIgnored))
        {
            string effectiveColumnName = propMap.ColumnName;
            try
            {
                object? cassandraValue;
                try
                {
                    cassandraValue = row.GetValue(propMap.PropertyInfo.PropertyType, effectiveColumnName);
                }
                catch (InvalidCastException ex)
                {
                    _logger.LogWarning(ex, "Failed to cast Cassandra column {ColumnName} to property {PropertyName} of type {PropertyType} for entity {EntityName}",
                        effectiveColumnName, propMap.PropertyInfo.Name, propMap.PropertyInfo.PropertyType.FullName, typeof(T).FullName);
                    continue;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error getting value for Cassandra column {ColumnName} for property {PropertyName} of type {PropertyType} for entity {EntityName}",
                        effectiveColumnName, propMap.PropertyInfo.Name, propMap.PropertyInfo.PropertyType.FullName, typeof(T).FullName);
                    continue;
                }

                if (cassandraValue != null)
                {
                    if (propMap.PropertyInfo.PropertyType.IsEnum)
                    {
                        // ... (enum mapping logic as before)
                        if (cassandraValue is string stringValue && !string.IsNullOrEmpty(stringValue))
                        {
                            try { propMap.PropertyInfo.SetValue(entity, Enum.Parse(propMap.PropertyInfo.PropertyType, stringValue, true)); }
                            catch (ArgumentException argEx) { _logger.LogWarning(argEx, "Failed to parse string '{StringValue}' to enum {EnumName}", stringValue, propMap.PropertyInfo.PropertyType.FullName); }
                        }
                        else if (cassandraValue is int intValue)
                        {
                            try { propMap.PropertyInfo.SetValue(entity, Enum.ToObject(propMap.PropertyInfo.PropertyType, intValue)); }
                            catch (ArgumentException argEx) { _logger.LogWarning(argEx, "Failed to parse int '{IntValue}' to enum {EnumName}", intValue, propMap.PropertyInfo.PropertyType.FullName); }
                        }
                        else
                        {
                             _logger.LogWarning("Cannot map value of type {ValueType} for column {ColumnName} to enum property {PropertyName} of type {PropertyType} for entity {EntityName}",
                                cassandraValue.GetType().FullName, effectiveColumnName, propMap.PropertyInfo.Name, propMap.PropertyInfo.PropertyType.FullName, typeof(T).FullName);
                        }
                    }
                    else
                    {
                        try { propMap.PropertyInfo.SetValue(entity, cassandraValue); }
                        catch (ArgumentException argEx)
                        {
                            _logger.LogWarning(argEx, "Failed to set property {PropertyName} with value from column {ColumnName}. Value Type: {CassandraValueType}, Property Type: {PropertyType}",
                                propMap.PropertyInfo.Name, effectiveColumnName, cassandraValue.GetType().FullName, propMap.PropertyInfo.PropertyType.FullName);
                        }
                    }
                }
            }
            catch
            {
                if (propMap.IsComputed)
                {
                    _logger.LogDebug("Computed column {ColumnName} (Expression: {ComputedExpression}) not found in RowSet for entity {EntityName}. Property {PropertyName} will not be set.",
                        propMap.ColumnName, propMap.ComputedExpression, typeof(T).FullName, propMap.PropertyInfo.Name);
                }
            }
        }
        return entity;
    }

    public virtual SelectQueryBuilder<T> Query<T>() where T : class, new()
    {
        return new SelectQueryBuilder<T>(this, _mappingResolver);
    }
}