using Cassandra;
using CassandraDriver.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Cassandra.Mapping; // This is the driver's mapping, we are creating a custom one. Keep for now.
using Cassandra.Exceptions;
using CassandraDriver.Mapping; // Our new mapping namespace
using System.Text; // For StringBuilder
using System.Linq; // For Linq operations
using System.Reflection; // For Activator.CreateInstance
using CassandraDriver.Queries; // For SelectQueryBuilder

namespace CassandraDriver.Services;

public class CassandraService : IHostedService, IDisposable
{
    private readonly CassandraConfiguration _configuration;
    private readonly ILogger<CassandraService> _logger;
    private readonly ConsistencyLevel? _defaultConsistencyLevel;
    private readonly ConsistencyLevel? _defaultSerialConsistencyLevel;
    private readonly AsyncRetryPolicy<RowSet> _retryPolicy;
    private readonly TableMappingResolver _mappingResolver;
    private ICluster? _cluster;
    private ISession? _session;
    private readonly string[] _cipherSuites = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" };

    public CassandraService(IOptions<CassandraConfiguration> configuration, ILogger<CassandraService> logger, TableMappingResolver mappingResolver)
    {
        _configuration = configuration.Value;
        _logger = logger;
        _mappingResolver = mappingResolver; // Injected
        _defaultConsistencyLevel = _configuration.DefaultConsistencyLevel;
        _defaultSerialConsistencyLevel = _configuration.DefaultSerialConsistencyLevel;

        var retryPolicyConfig = _configuration.RetryPolicy;

        if (retryPolicyConfig.Enabled)
        {
            _retryPolicy = Policy.Handle<DriverException>(ex => ex is OperationTimedOutException ||
                                                              ex is UnavailableException ||
                                                              ex is OverloadedException ||
                                                              ex is WriteTimeoutException ||
                                                              ex is ReadTimeoutException)
                .WaitAndRetryAsync(
                    retryPolicyConfig.MaxRetries,
                    retryAttempt => TimeSpan.FromMilliseconds(Math.Min(Math.Pow(2, retryAttempt) * retryPolicyConfig.DelayMilliseconds, retryPolicyConfig.MaxDelayMilliseconds)),
                    (exception, timeSpan, retryCount, context) =>
                    {
                        _logger.LogWarning(exception, "Retry {RetryCount} due to {ExceptionType}. Waiting {TimeSpanTotalSeconds}s before next attempt. Operation: {OperationKey}",
                            retryCount, exception.GetType().Name, timeSpan.TotalSeconds, context.OperationKey);
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
        await Task.Run(() => Connect(), cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_session != null)
        {
            await Task.Run(() => _session.Dispose(), cancellationToken);
        }
        
        if (_cluster != null)
        {
            await Task.Run(() => _cluster.Dispose(), cancellationToken);
        }
    }

    private void Connect()
    {
        var builder = Cassandra.Cluster.Builder();

        // Configure load balancing
        var dcAwarePolicy = new DCAwareRoundRobinPolicy();
        builder.WithLoadBalancingPolicy(new TokenAwarePolicy(dcAwarePolicy));

        // Configure speculative execution
        if (_configuration.SpeculativeExecutionEnabled)
        {
            // Note: PercentileSpeculativeExecutionPolicy is not available in C# driver
            // Using ConstantSpeculativeExecutionPolicy as an alternative
            var speculativePolicy = new ConstantSpeculativeExecutionPolicy(
                delay: 100, // milliseconds delay
                maxSpeculativeExecutions: _configuration.MaxSpeculativeExecutions);
            builder.WithSpeculativeExecutionPolicy(speculativePolicy);
        }

        // Add contact points
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

        // Configure EC2 address translation
        if (_configuration.Ec2TranslationEnabled)
        {
            // Note: EC2MultiRegionAddressTranslator requires separate AWS SDK package
            // For now, we'll skip this feature or use a simple pass-through translator
            _logger.LogWarning("EC2 address translation requested but not implemented in C# port");
        }

        // Configure SSL/TLS
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

        // Configure authentication
        if (!string.IsNullOrEmpty(_configuration.User))
        {
            builder.WithCredentials(_configuration.User, _configuration.Password);
        }

        // Configure protocol version
        if (_configuration.ProtocolVersion.HasValue)
        {
            builder.WithMaxProtocolVersion((ProtocolVersion)_configuration.ProtocolVersion.Value);
        }

        _cluster = builder.Build();

        // Connect to keyspace if specified
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

    private SSLOptions ConfigureSsl(SslConfiguration truststore, SslConfiguration keystore)
    {
        if (string.IsNullOrEmpty(truststore.Path) || string.IsNullOrEmpty(keystore.Path))
        {
            throw new ArgumentException("SSL configuration requires both truststore and keystore paths");
        }

        var sslOptions = new SSLOptions();
        
        // Load client certificate
        var clientCert = new X509Certificate2(keystore.Path, keystore.Password);
        sslOptions.SetCertificateCollection(new X509CertificateCollection { clientCert });

        // Configure certificate validation
        sslOptions.SetRemoteCertValidationCallback((sender, certificate, chain, errors) =>
        {
            if (errors == SslPolicyErrors.None)
                return true;

            // In production, you might want to validate against specific CA or thumbprint
            _logger.LogWarning("SSL certificate validation errors: {Errors}", errors);
            
            // For development/testing, you might accept self-signed certificates
            // In production, return false here and properly validate certificates
            return errors == SslPolicyErrors.RemoteCertificateChainErrors;
        });

        // Note: SslProtocol is read-only in C# driver, it uses the system default
        // which should be TLS 1.2 or higher on modern systems

        return sslOptions;
    }

    public virtual async Task<RowSet> ExecuteAsync(IStatement statement)
    {
        return await ExecuteAsync(statement, null, null);
    }

    public virtual async Task<RowSet> ExecuteAsync(string cql, params object[] values)
    {
        return await ExecuteAsync(cql, null, null, values);
    }

    public virtual async Task<RowSet> ExecuteAsync(IStatement statement, ConsistencyLevel? consistencyLevel, ConsistencyLevel? serialConsistencyLevel)
    {
        if (_session == null)
            throw new InvalidOperationException("Session is not initialized");

        statement.SetConsistencyLevel(consistencyLevel ?? _defaultConsistencyLevel);
        statement.SetSerialConsistencyLevel(serialConsistencyLevel ?? _defaultSerialConsistencyLevel);

        // Note: Users should ensure queries are idempotent if retries are enabled.
        return await _retryPolicy.ExecuteAsync(async () => await Session.ExecuteAsync(statement));
    }

    public virtual async Task<RowSet> ExecuteAsync(string cql, ConsistencyLevel? consistencyLevel, ConsistencyLevel? serialConsistencyLevel, params object[] values)
    {
        if (_session == null)
            throw new InvalidOperationException("Session is not initialized");

        var statement = new SimpleStatement(cql, values);
        statement.SetConsistencyLevel(consistencyLevel ?? _defaultConsistencyLevel);
        statement.SetSerialConsistencyLevel(serialConsistencyLevel ?? _defaultSerialConsistencyLevel);

        // Note: Users should ensure queries are idempotent if retries are enabled.
        return await _retryPolicy.ExecuteAsync(async () => await Session.ExecuteAsync(statement));
    }

    public void Dispose()
    {
        _session?.Dispose();
        _cluster?.Dispose();
    }

    // --- Mapping Methods ---

    public virtual async Task<T?> GetAsync<T>(params object[] primaryKeyComponents) where T : class, new()
    {
        var mappingInfo = _mappingResolver.GetMappingInfo(typeof(T));

        var selectColumns = string.Join(", ", mappingInfo.Properties
            .Where(p => !p.IsIgnored)
            .Select(p => p.IsComputed ? $"{p.ComputedExpression} AS {p.ColumnName}" : p.ColumnName));

        var queryBuilder = new StringBuilder($"SELECT {selectColumns} FROM {mappingInfo.TableName}");
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
            whereClause.Append($"\"{keyProperty.ColumnName}\" = ?"); // Enclose column names in quotes
            parameters.Add(primaryKeyComponents[i]);
        }

        if (whereClause.Length > 0)
        {
            queryBuilder.Append($" WHERE {whereClause}");
        }
        else
        {
             _logger.LogWarning("Executing GetAsync for type {EntityType} without where clause (no primary keys specified or table has no keys).", typeof(T).FullName);
        }

        var statement = new SimpleStatement(queryBuilder.ToString(), parameters.ToArray());
        var rowSet = await ExecuteAsync(statement);
        var firstRow = rowSet.FirstOrDefault();

        return firstRow != null ? MapRowToEntity<T>(firstRow, mappingInfo) : null;
    }

    public virtual async Task InsertAsync<T>(T entity, bool ifNotExists = false, int? ttl = null, ConsistencyLevel? consistencyLevel = null, ConsistencyLevel? serialConsistencyLevel = null) where T : class
    {
        var mappingInfo = _mappingResolver.GetMappingInfo(typeof(T));

        var columnsForInsert = mappingInfo.Properties
            .Where(p => !p.IsIgnored && !p.IsComputed)
            .ToList();

        var columnNames = string.Join(", ", columnsForInsert.Select(p => $"\"{p.ColumnName}\"")); // Enclose column names in quotes
        var valuePlaceholders = string.Join(", ", columnsForInsert.Select(_ => "?"));

        var queryBuilder = new StringBuilder($"INSERT INTO {mappingInfo.TableName} ({columnNames}) VALUES ({valuePlaceholders})");

        if (ifNotExists)
        {
            queryBuilder.Append(" IF NOT EXISTS");
        }

        List<object?> queryParameters = columnsForInsert.Select(p => p.PropertyInfo.GetValue(entity)).ToList();

        if (ttl.HasValue)
        {
            queryBuilder.Append($" USING TTL ?");
            queryParameters.Add(ttl.Value);
        }

        var statement = new SimpleStatement(queryBuilder.ToString(), queryParameters.ToArray());

        await ExecuteAsync(statement, consistencyLevel, ifNotExists ? (serialConsistencyLevel ?? _defaultSerialConsistencyLevel ?? ConsistencyLevel.Serial) : serialConsistencyLevel);
    }

    public virtual async Task UpdateAsync<T>(T entity, int? ttl = null, ConsistencyLevel? consistencyLevel = null) where T : class
    {
        var mappingInfo = _mappingResolver.GetMappingInfo(typeof(T));

        var setClause = new StringBuilder();
        var whereClause = new StringBuilder();
        var parameters = new List<object?>(); // Changed to List<object?>

        var nonKeyColumns = mappingInfo.Properties
            .Where(p => !p.IsPartitionKey && !p.IsClusteringKey && !p.IsIgnored && !p.IsComputed)
            .ToList();

        if (!nonKeyColumns.Any())
        {
            _logger.LogWarning("UpdateAsync called for type {EntityType} with no non-key columns to update.", typeof(T).FullName);
            return;
        }

        foreach (var propMap in nonKeyColumns)
        {
            if (parameters.Count > 0) setClause.Append(", ");
            setClause.Append($"\"{propMap.ColumnName}\" = ?"); // Enclose column names in quotes
            parameters.Add(propMap.PropertyInfo.GetValue(entity));
        }

        var allKeys = mappingInfo.PartitionKeys.Concat(mappingInfo.ClusteringKeys).ToList();
        foreach (var keyProp in allKeys) // Iterate through allKeys to add to parameters list
        {
            parameters.Add(keyProp.PropertyInfo.GetValue(entity));
        }

        for (int i = 0; i < allKeys.Count; i++) // Build WHERE clause separately
        {
            var keyProp = allKeys[i];
            if (i > 0) whereClause.Append(" AND ");
            whereClause.Append($"\"{keyProp.ColumnName}\" = ?"); // Enclose column names in quotes
        }

        var queryBuilder = new StringBuilder($"UPDATE {mappingInfo.TableName}");
        List<object?> finalParameters = new List<object?>();

        if (ttl.HasValue)
        {
            queryBuilder.Append($" USING TTL ?");
            finalParameters.Add(ttl.Value);
        }
        finalParameters.AddRange(parameters); // Add SET parameters first, then WHERE key parameters. Order matters.


        queryBuilder.Append($" SET {setClause} WHERE {whereClause}");

        // Reorder parameters: SET parameters first, then TTL (if any), then WHERE parameters.
        // The current 'parameters' list contains SET params, then Key params. TTL needs to be inserted.
        List<object?> statementParams = new List<object?>();
        int setParamCount = nonKeyColumns.Count;
        statementParams.AddRange(parameters.Take(setParamCount)); // SET clause parameters
        if (ttl.HasValue) { statementParams.Add(ttl.Value); }
        statementParams.AddRange(parameters.Skip(setParamCount)); // WHERE clause parameters


        var statement = new SimpleStatement(queryBuilder.ToString(), statementParams.ToArray());
        await ExecuteAsync(statement, consistencyLevel, null);
    }

    public virtual async Task DeleteAsync<T>(params object[] primaryKeyComponents) where T : class
    {
        var mappingInfo = _mappingResolver.GetMappingInfo(typeof(T));
        var queryBuilder = new StringBuilder($"DELETE FROM {mappingInfo.TableName}");

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
            whereClause.Append($"\"{keyProperty.ColumnName}\" = ?"); // Enclose column names in quotes
            parameters.Add(primaryKeyComponents[i]);
        }

        if (whereClause.Length == 0)
        {
             throw new InvalidOperationException($"Delete operation for type {typeof(T).FullName} must have a WHERE clause based on primary keys.");
        }
        queryBuilder.Append($" WHERE {whereClause}");

        var statement = new SimpleStatement(queryBuilder.ToString(), parameters.ToArray());
        await ExecuteAsync(statement);
    }

    public virtual async Task DeleteAsync<T>(T entity, ConsistencyLevel? consistencyLevel = null) where T : class
    {
        var mappingInfo = _mappingResolver.GetMappingInfo(typeof(T));
        var pkPropertyValues = mappingInfo.PartitionKeys.Concat(mappingInfo.ClusteringKeys)
            .OrderBy(k => k.IsPartitionKey ? k.PartitionKeyOrder : 1000 + k.ClusteringKeyOrder)
            .Select(p => p.PropertyInfo.GetValue(entity))
            .ToArray();

        if (pkPropertyValues.Any(v => v == null)) // Null check for primary key values
        {
            // Allowing null here might be valid if Cassandra allows null PK components, though unusual.
            // For safety, throwing an exception if any PK component is null.
            throw new ArgumentException("Primary key components cannot be null for delete by entity operation.", nameof(entity));
        }
        // Call the other DeleteAsync overload, ensuring pkPropertyValues is not null itself
        await DeleteAsync<T>(pkPropertyValues!);
    }

    // Made public to be accessible by SelectQueryBuilder, consider internal if appropriate
    public T MapRowToEntity<T>(Row row, TableMappingInfo mappingInfo) where T : class
    {
        var entity = (T)Activator.CreateInstance(typeof(T))!; // Ensure T has a parameterless constructor
        foreach (var propMap in mappingInfo.Properties.Where(p => !p.IsIgnored))
        {
            // For computed columns, use ColumnName as it's aliased in SELECT.
            string effectiveColumnName = propMap.ColumnName;

            if (row.ContainsColumn(effectiveColumnName))
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
                        if (cassandraValue is string stringValue && !string.IsNullOrEmpty(stringValue))
                        {
                            try { propMap.PropertyInfo.SetValue(entity, Enum.Parse(propMap.PropertyInfo.PropertyType, stringValue, true)); }
                            catch (ArgumentException ex) { _logger.LogWarning(ex, "Failed to parse string '{StringValue}' to enum {EnumName}", stringValue, propMap.PropertyInfo.PropertyType.FullName); }
                        }
                        else if (cassandraValue is int intValue)
                        {
                            try { propMap.PropertyInfo.SetValue(entity, Enum.ToObject(propMap.PropertyInfo.PropertyType, intValue)); }
                            catch (ArgumentException ex) { _logger.LogWarning(ex, "Failed to parse int '{IntValue}' to enum {EnumName}", intValue, propMap.PropertyInfo.PropertyType.FullName); }
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
                         catch (ArgumentException ex) { // This can happen if types are convertible but not directly assignable, e.g. long to int.
                             _logger.LogWarning(ex, "Failed to set property {PropertyName} with value from column {ColumnName}. Type mismatch or invalid value. Value Type: {CassandraValueType}, Property Type: {PropertyType}",
                                 propMap.PropertyInfo.Name, effectiveColumnName, cassandraValue.GetType().FullName, propMap.PropertyInfo.PropertyType.FullName);
                         }
                    }
                }
            }
            else if (propMap.IsComputed)
            {
                // This case might indicate the computed column (alias) wasn't found in the RowSet.
                // It could be an issue with how it's selected or aliased.
                _logger.LogDebug("Computed column {ColumnName} (Expression: {ComputedExpression}) not found in RowSet for entity {EntityName}. Property {PropertyName} will not be set.",
                    propMap.ColumnName, propMap.ComputedExpression, typeof(T).FullName, propMap.PropertyInfo.Name);
            }
        }
        return entity;
    }

    // --- Query Builder Entry Point ---
    public virtual SelectQueryBuilder<T> Query<T>() where T : class, new()
    {
        return new SelectQueryBuilder<T>(this, _mappingResolver);
    }
}