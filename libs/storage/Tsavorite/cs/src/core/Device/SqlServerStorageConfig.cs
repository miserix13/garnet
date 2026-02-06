// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Data.SqlClient;

namespace Tsavorite.core
{
    /// <summary>
    /// Configuration for SQL Server storage device
    /// </summary>
    public class SqlServerStorageConfig
    {
        /// <summary>
        /// SQL Server connection string
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Table name for storing segments (default: TsavoriteSegments)
        /// </summary>
        public string TableName { get; set; } = "TsavoriteSegments";

        /// <summary>
        /// Chunk size for splitting large segments (default: 256MB)
        /// Must be less than 2GB to fit in VARBINARY(MAX)
        /// </summary>
        public int ChunkSize { get; set; } = 256 * 1024 * 1024; // 256MB

        /// <summary>
        /// Number of pooled SQL connections (default: 8)
        /// </summary>
        public int ConnectionPoolSize { get; set; } = 8;

        /// <summary>
        /// Maximum number of retry attempts for transient failures (default: 3)
        /// </summary>
        public int MaxRetries { get; set; } = 3;

        /// <summary>
        /// Base delay in milliseconds between retry attempts (default: 100ms)
        /// Uses exponential backoff: delay * 2^attemptNumber
        /// </summary>
        public int RetryDelayMs { get; set; } = 100;

        /// <summary>
        /// Timeout in milliseconds to detect hung operations (default: 60000ms = 1 minute)
        /// </summary>
        public int OperationTimeoutMs { get; set; } = 60000;

        /// <summary>
        /// Whether to delete the table when the device is disposed (default: false)
        /// </summary>
        public bool DeleteOnClose { get; set; } = false;

        /// <summary>
        /// Creates a new configuration with the specified connection string
        /// </summary>
        /// <param name="connectionString">SQL Server connection string</param>
        public SqlServerStorageConfig(string connectionString)
        {
            ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            Validate();
        }

        /// <summary>
        /// Validates the configuration
        /// </summary>
        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(ConnectionString))
                throw new ArgumentException("Connection string cannot be empty", nameof(ConnectionString));

            if (string.IsNullOrWhiteSpace(TableName))
                throw new ArgumentException("Table name cannot be empty", nameof(TableName));

            if (ChunkSize <= 0 || ChunkSize >= int.MaxValue)
                throw new ArgumentException("Chunk size must be positive and less than 2GB", nameof(ChunkSize));

            if (ConnectionPoolSize <= 0)
                throw new ArgumentException("Connection pool size must be positive", nameof(ConnectionPoolSize));

            if (MaxRetries < 0)
                throw new ArgumentException("Max retries cannot be negative", nameof(MaxRetries));

            if (RetryDelayMs < 0)
                throw new ArgumentException("Retry delay cannot be negative", nameof(RetryDelayMs));

            if (OperationTimeoutMs <= 0)
                throw new ArgumentException("Operation timeout must be positive", nameof(OperationTimeoutMs));

            // Validate connection string format
            try
            {
                var builder = new SqlConnectionStringBuilder(ConnectionString);
                if (string.IsNullOrWhiteSpace(builder.DataSource))
                    throw new ArgumentException("Connection string must specify a data source", nameof(ConnectionString));
            }
            catch (ArgumentException ex)
            {
                throw new ArgumentException($"Invalid connection string: {ex.Message}", nameof(ConnectionString), ex);
            }
        }
    }
}
