// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// SQL Server storage device factory
    /// </summary>
    public class SqlServerStorageNamedDeviceFactory : INamedDeviceFactory
    {
        private readonly SqlServerStorageConfig config;
        private readonly string baseName;
        private readonly ILogger logger;

        /// <summary>
        /// Create instance of SQL Server storage factory
        /// </summary>
        /// <param name="config">SQL Server storage configuration</param>
        /// <param name="baseName">Base name for storage</param>
        /// <param name="logger">Logger</param>
        public SqlServerStorageNamedDeviceFactory(SqlServerStorageConfig config, string baseName, ILogger logger = null)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            this.baseName = baseName;
            this.logger = logger;
        }

        /// <inheritdoc />
        public IDevice Get(FileDescriptor fileInfo)
        {
            // Create a unique table name combining base name, directory, and file name
            var tableName = SanitizeTableName($"{baseName}_{fileInfo.directoryName}_{fileInfo.fileName}");
            
            // Create a copy of the config with the specific table name
            var deviceConfig = new SqlServerStorageConfig(config.ConnectionString)
            {
                TableName = tableName,
                ChunkSize = config.ChunkSize,
                ConnectionPoolSize = config.ConnectionPoolSize,
                MaxRetries = config.MaxRetries,
                RetryDelayMs = config.RetryDelayMs,
                OperationTimeoutMs = config.OperationTimeoutMs,
                DeleteOnClose = config.DeleteOnClose
            };

            var device = new SqlServerStorageDevice(deviceConfig);
            
            logger?.LogInformation("Created SQL Server storage device for table: {TableName}", tableName);
            
            return device;
        }

        /// <inheritdoc />
        public void Delete(FileDescriptor fileInfo)
        {
            var tableName = SanitizeTableName($"{baseName}_{fileInfo.directoryName}_{fileInfo.fileName}");
            
            try
            {
                using var conn = new SqlConnection(config.ConnectionString);
                conn.Open();
                
                var dropSql = $"DROP TABLE IF EXISTS [{tableName}]";
                using var cmd = new SqlCommand(dropSql, conn);
                cmd.CommandTimeout = 60;
                cmd.ExecuteNonQuery();
                
                logger?.LogInformation("Deleted SQL Server storage table: {TableName}", tableName);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Failed to delete SQL Server storage table: {TableName}", tableName);
            }
        }

        /// <inheritdoc />
        public IEnumerable<FileDescriptor> ListContents(string path)
        {
            // Query SQL Server for tables matching the base name pattern
            var tablePrefix = SanitizeTableName($"{baseName}_{path}");
            
            try
            {
                using var conn = new SqlConnection(config.ConnectionString);
                conn.Open();
                
                var listSql = @"
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE' 
                    AND TABLE_NAME LIKE @TablePrefix + '%'
                    ORDER BY TABLE_NAME DESC";
                
                using var cmd = new SqlCommand(listSql, conn);
                cmd.Parameters.AddWithValue("@TablePrefix", tablePrefix);
                cmd.CommandTimeout = 60;
                
                List<FileDescriptor> results = [];
                using var reader = cmd.ExecuteReader();
                
                while (reader.Read())
                {
                    var tableName = reader.GetString(0);
                    
                    // Parse table name back into directory and file name
                    // Format: {baseName}_{directory}_{fileName}
                    var parts = tableName.Substring(baseName.Length + 1).Split('_', 2);
                    if (parts.Length == 2)
                    {
                        results.Add(new FileDescriptor(parts[0], parts[1]));
                    }
                    else if (parts.Length == 1)
                    {
                        results.Add(new FileDescriptor("", parts[0]));
                    }
                }
                
                return results;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Failed to list SQL Server storage tables for path: {Path}", path);
                return [];
            }
        }

        /// <summary>
        /// Sanitize table name to be SQL Server compliant
        /// </summary>
        private string SanitizeTableName(string name)
        {
            // Replace invalid characters with underscores
            var sanitized = name
                .Replace("/", "_")
                .Replace("\\", "_")
                .Replace(".", "_")
                .Replace("-", "_")
                .Replace(" ", "_");
            
            // Ensure it doesn't start with a number
            if (char.IsDigit(sanitized[0]))
            {
                sanitized = "T" + sanitized;
            }
            
            // Truncate if too long (SQL Server max identifier length is 128)
            if (sanitized.Length > 128)
            {
                sanitized = sanitized.Substring(0, 128);
            }
            
            return sanitized;
        }
    }
}
