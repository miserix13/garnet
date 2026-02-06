// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// SQL Server storage named device factory creator
    /// </summary>
    public class SqlServerStorageNamedDeviceFactoryCreator : INamedDeviceFactoryCreator
    {
        private readonly SqlServerStorageConfig config;
        private readonly ILogger logger;

        /// <summary>
        /// Create instance of SQL Server storage factory creator
        /// </summary>
        /// <param name="connectionString">SQL Server connection string</param>
        /// <param name="logger">Logger</param>
        /// <param name="tableName">Base table name (optional, defaults to "TsavoriteSegments")</param>
        /// <param name="chunkSize">Chunk size in bytes (optional, defaults to 256MB)</param>
        /// <param name="connectionPoolSize">Connection pool size (optional, defaults to 8)</param>
        /// <param name="deleteOnClose">Whether to delete tables on close (optional, defaults to false)</param>
        public SqlServerStorageNamedDeviceFactoryCreator(
            string connectionString,
            ILogger logger = null,
            string tableName = "TsavoriteSegments",
            int chunkSize = 256 * 1024 * 1024,
            int connectionPoolSize = 8,
            bool deleteOnClose = false)
        {
            config = new SqlServerStorageConfig(connectionString)
            {
                TableName = tableName,
                ChunkSize = chunkSize,
                ConnectionPoolSize = connectionPoolSize,
                DeleteOnClose = deleteOnClose
            };
            config.Validate();
            this.logger = logger;
        }

        /// <summary>
        /// Create instance of SQL Server storage factory creator with custom configuration
        /// </summary>
        /// <param name="config">SQL Server storage configuration</param>
        /// <param name="logger">Logger</param>
        public SqlServerStorageNamedDeviceFactoryCreator(SqlServerStorageConfig config, ILogger logger = null)
        {
            this.config = config ?? throw new System.ArgumentNullException(nameof(config));
            config.Validate();
            this.logger = logger;
        }

        /// <inheritdoc />
        public INamedDeviceFactory Create(string baseName)
        {
            return new SqlServerStorageNamedDeviceFactory(config, baseName, logger);
        }
    }
}
