// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Tsavorite.core
{
    /// <summary>
    /// Storage device that persists data to Microsoft SQL Server using chunked VARBINARY storage
    /// </summary>
    public class SqlServerStorageDevice : StorageDeviceBase
    {
        private readonly SqlServerStorageConfig config;
        private AsyncPool<SqlConnection> connectionPool;
        private readonly SafeConcurrentDictionary<int, SegmentMetadata> segmentMetadata;
        private readonly ConcurrentDictionary<long, PendingOperation> pendingOperations;
        private long operationSequence = 0;
        private int pendingCount = 0;
        private Timer hangDetectionTimer;
        private bool disposed = false;
        private readonly CancellationTokenSource disposeCts = new CancellationTokenSource();

        /// <summary>
        /// Metadata about a segment stored in SQL Server
        /// </summary>
        private class SegmentMetadata
        {
            public int ChunkCount;
            public long TotalSize;
        }

        /// <summary>
        /// Tracks a pending asynchronous operation
        /// </summary>
        private struct PendingOperation
        {
            public long OperationId;
            public string OperationType; // "Read", "Write", "Delete"
            public DeviceIOCompletionCallback Callback;
            public object Context;
            public DateTime StartTime;
        }

        /// <summary>
        /// Creates a new SQL Server storage device
        /// </summary>
        /// <param name="config">SQL Server configuration</param>
        /// <param name="sectorSize">Sector size (default 512 bytes, can use 8192 for SQL page alignment)</param>
        /// <param name="capacity">Capacity in bytes, or CAPACITY_UNSPECIFIED</param>
        public SqlServerStorageDevice(SqlServerStorageConfig config, uint sectorSize = 512, long capacity = Devices.CAPACITY_UNSPECIFIED)
            : base(config.TableName, sectorSize, capacity)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            config.Validate();

            segmentMetadata = new SafeConcurrentDictionary<int, SegmentMetadata>();
            pendingOperations = new ConcurrentDictionary<long, PendingOperation>();
        }

        /// <summary>
        /// Initialize the device
        /// </summary>
        public override void Initialize(long segmentSize, LightEpoch epoch, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);

            // Initialize connection pool
            connectionPool = new AsyncPool<SqlConnection>(
                config.ConnectionPoolSize,
                () =>
                {
                    var conn = new SqlConnection(config.ConnectionString);
                    conn.Open();
                    return conn;
                });

            // Ensure database schema exists
            EnsureSchemaAsync().GetAwaiter().GetResult();

            // Start hang detection timer
            hangDetectionTimer = new Timer(
                DetectHangOperations,
                null,
                TimeSpan.FromSeconds(30),
                TimeSpan.FromSeconds(30));
        }

        /// <summary>
        /// Creates the SQL table if it doesn't exist
        /// </summary>
        private async Task EnsureSchemaAsync()
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var conn = await connectionPool.GetAsync().ConfigureAwait(false);
                try
                {
                    var createTableSql = $@"
                        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{config.TableName}')
                        BEGIN
                            CREATE TABLE [{config.TableName}] (
                                SegmentId INT NOT NULL,
                                ChunkIndex INT NOT NULL,
                                Data VARBINARY(MAX) NOT NULL,
                                CONSTRAINT PK_{config.TableName} PRIMARY KEY CLUSTERED (SegmentId, ChunkIndex)
                            );
                            CREATE NONCLUSTERED INDEX IX_{config.TableName}_SegmentId ON [{config.TableName}] (SegmentId);
                        END";

                    using (var cmd = new SqlCommand(createTableSql, conn))
                    {
                        cmd.CommandTimeout = 60;
                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    return true;
                }
                finally
                {
                    connectionPool.Return(conn);
                }
            }, disposeCts.Token).ConfigureAwait(false);
        }

        /// <summary>
        /// Read data from SQL Server asynchronously
        /// </summary>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            if (disposed)
            {
                callback(uint.MaxValue, 0, context);
                return;
            }

            var opId = Interlocked.Increment(ref operationSequence);
            Interlocked.Increment(ref pendingCount);

            var operation = new PendingOperation
            {
                OperationId = opId,
                OperationType = "Read",
                Callback = callback,
                Context = context,
                StartTime = DateTime.UtcNow
            };
            pendingOperations[opId] = operation;

            Task.Run(async () =>
            {
                try
                {
                    await ExecuteWithRetryAsync(async () =>
                    {
                        var conn = await connectionPool.GetAsync(disposeCts.Token).ConfigureAwait(false);
                        try
                        {
                            return await ReadChunkedDataAsync(conn, segmentId, sourceAddress, destinationAddress, readLength).ConfigureAwait(false);
                        }
                        finally
                        {
                            connectionPool.Return(conn);
                        }
                    }, disposeCts.Token).ConfigureAwait(false);

                    pendingOperations.TryRemove(opId, out _);
                    callback(0, readLength, context);
                }
                catch (Exception ex)
                {
                    pendingOperations.TryRemove(opId, out _);
                    callback(GetErrorCode(ex), 0, context);
                }
                finally
                {
                    Interlocked.Decrement(ref pendingCount);
                }
            });
        }

        /// <summary>
        /// Reads chunked data from SQL Server
        /// </summary>
        private async Task<bool> ReadChunkedDataAsync(SqlConnection conn, int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength)
        {
            int startChunk = (int)(sourceAddress / (ulong)config.ChunkSize);
            int endChunk = (int)((sourceAddress + readLength - 1) / (ulong)config.ChunkSize);

            var sql = $@"
                SELECT ChunkIndex, Data 
                FROM [{config.TableName}]
                WHERE SegmentId = @SegmentId AND ChunkIndex BETWEEN @StartChunk AND @EndChunk
                ORDER BY ChunkIndex";

            using (var cmd = new SqlCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("@SegmentId", segmentId);
                cmd.Parameters.AddWithValue("@StartChunk", startChunk);
                cmd.Parameters.AddWithValue("@EndChunk", endChunk);
                cmd.CommandTimeout = 300;

                using (var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    uint totalBytesRead = 0;

                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        int chunkIndex = reader.GetInt32(0);
                        byte[] chunkData = (byte[])reader.GetValue(1);

                        // Calculate offsets for this chunk
                        ulong chunkStartAddress = (ulong)chunkIndex * (ulong)config.ChunkSize;
                        ulong chunkEndAddress = chunkStartAddress + (ulong)chunkData.Length;

                        // Determine which portion of this chunk we need
                        ulong readStartInChunk = sourceAddress > chunkStartAddress ? sourceAddress - chunkStartAddress : 0;
                        ulong readEndInChunk = Math.Min((ulong)chunkData.Length, sourceAddress + readLength - chunkStartAddress);
                        
                        if (readEndInChunk > readStartInChunk)
                        {
                            uint bytesToCopy = (uint)(readEndInChunk - readStartInChunk);
                            Marshal.Copy(chunkData, (int)readStartInChunk, IntPtr.Add(destinationAddress, (int)totalBytesRead), (int)bytesToCopy);
                            totalBytesRead += bytesToCopy;
                        }
                    }

                    // If we didn't read enough data, the segment might not exist or be sparse
                    if (totalBytesRead < readLength)
                    {
                        // Zero-fill the remaining bytes
                        unsafe
                        {
                            byte* destPtr = (byte*)destinationAddress;
                            for (uint i = totalBytesRead; i < readLength; i++)
                            {
                                destPtr[i] = 0;
                            }
                        }
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Write data to SQL Server asynchronously
        /// </summary>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            if (disposed)
            {
                callback(uint.MaxValue, 0, context);
                return;
            }

            var opId = Interlocked.Increment(ref operationSequence);
            Interlocked.Increment(ref pendingCount);

            var operation = new PendingOperation
            {
                OperationId = opId,
                OperationType = "Write",
                Callback = callback,
                Context = context,
                StartTime = DateTime.UtcNow
            };
            pendingOperations[opId] = operation;

            Task.Run(async () =>
            {
                try
                {
                    await ExecuteWithRetryAsync(async () =>
                    {
                        var conn = await connectionPool.GetAsync(disposeCts.Token).ConfigureAwait(false);
                        try
                        {
                            return await WriteChunkedDataAsync(conn, sourceAddress, segmentId, destinationAddress, numBytesToWrite).ConfigureAwait(false);
                        }
                        finally
                        {
                            connectionPool.Return(conn);
                        }
                    }, disposeCts.Token).ConfigureAwait(false);

                    pendingOperations.TryRemove(opId, out _);
                    callback(0, numBytesToWrite, context);
                }
                catch (Exception ex)
                {
                    pendingOperations.TryRemove(opId, out _);
                    callback(GetErrorCode(ex), 0, context);
                }
                finally
                {
                    Interlocked.Decrement(ref pendingCount);
                }
            });
        }

        /// <summary>
        /// Writes chunked data to SQL Server
        /// </summary>
        private async Task<bool> WriteChunkedDataAsync(SqlConnection conn, IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite)
        {
            int startChunk = (int)(destinationAddress / (ulong)config.ChunkSize);
            int endChunk = (int)((destinationAddress + numBytesToWrite - 1) / (ulong)config.ChunkSize);

            uint totalBytesWritten = 0;

            // Write each chunk
            for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++)
            {
                ulong chunkStartAddress = (ulong)chunkIndex * (ulong)config.ChunkSize;
                ulong chunkEndAddress = chunkStartAddress + (ulong)config.ChunkSize;

                // Determine which portion of this chunk we're writing
                ulong writeStartInChunk = destinationAddress > chunkStartAddress ? destinationAddress - chunkStartAddress : 0;
                ulong writeEndInChunk = Math.Min((ulong)config.ChunkSize, destinationAddress + numBytesToWrite - chunkStartAddress);
                
                uint bytesToWrite = (uint)(writeEndInChunk - writeStartInChunk);
                
                // Copy bytes from source
                byte[] chunkData = new byte[bytesToWrite];
                Marshal.Copy(IntPtr.Add(sourceAddress, (int)totalBytesWritten), chunkData, 0, (int)bytesToWrite);

                // Merge with existing chunk data if this is a partial chunk write
                if (writeStartInChunk > 0 || writeEndInChunk < (ulong)config.ChunkSize)
                {
                    chunkData = await MergeWithExistingChunkAsync(conn, segmentId, chunkIndex, (int)writeStartInChunk, chunkData).ConfigureAwait(false);
                }

                // Write chunk to database using MERGE
                var mergeSql = $@"
                    MERGE [{config.TableName}] AS target
                    USING (SELECT @SegmentId AS SegmentId, @ChunkIndex AS ChunkIndex, @Data AS Data) AS source
                    ON target.SegmentId = source.SegmentId AND target.ChunkIndex = source.ChunkIndex
                    WHEN MATCHED THEN
                        UPDATE SET Data = source.Data
                    WHEN NOT MATCHED THEN
                        INSERT (SegmentId, ChunkIndex, Data) VALUES (source.SegmentId, source.ChunkIndex, source.Data);";

                using (var cmd = new SqlCommand(mergeSql, conn))
                {
                    cmd.Parameters.AddWithValue("@SegmentId", segmentId);
                    cmd.Parameters.AddWithValue("@ChunkIndex", chunkIndex);
                    cmd.Parameters.Add("@Data", SqlDbType.VarBinary, -1).Value = chunkData;
                    cmd.CommandTimeout = 300;

                    await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

                totalBytesWritten += bytesToWrite;
            }

            // Update segment metadata
            segmentMetadata.GetOrAdd(segmentId, _ => new SegmentMetadata()).ChunkCount = Math.Max(
                segmentMetadata.GetOrAdd(segmentId, _ => new SegmentMetadata()).ChunkCount,
                endChunk + 1);
            segmentMetadata.GetOrAdd(segmentId, _ => new SegmentMetadata()).TotalSize = Math.Max(
                segmentMetadata.GetOrAdd(segmentId, _ => new SegmentMetadata()).TotalSize,
                (long)(destinationAddress + numBytesToWrite));

            return true;
        }

        /// <summary>
        /// Merges new data with existing chunk data for partial chunk writes
        /// </summary>
        private async Task<byte[]> MergeWithExistingChunkAsync(SqlConnection conn, int segmentId, int chunkIndex, int writeOffset, byte[] newData)
        {
            var selectSql = $@"
                SELECT Data FROM [{config.TableName}]
                WHERE SegmentId = @SegmentId AND ChunkIndex = @ChunkIndex";

            using (var cmd = new SqlCommand(selectSql, conn))
            {
                cmd.Parameters.AddWithValue("@SegmentId", segmentId);
                cmd.Parameters.AddWithValue("@ChunkIndex", chunkIndex);
                cmd.CommandTimeout = 60;

                var existingData = await cmd.ExecuteScalarAsync().ConfigureAwait(false) as byte[];
                
                if (existingData == null)
                {
                    // No existing data, create new chunk with proper size
                    var mergedData = new byte[Math.Max(config.ChunkSize, writeOffset + newData.Length)];
                    Array.Copy(newData, 0, mergedData, writeOffset, newData.Length);
                    return mergedData;
                }
                else
                {
                    // Merge with existing data
                    int requiredSize = Math.Max(existingData.Length, writeOffset + newData.Length);
                    var mergedData = new byte[requiredSize];
                    Array.Copy(existingData, 0, mergedData, 0, existingData.Length);
                    Array.Copy(newData, 0, mergedData, writeOffset, newData.Length);
                    return mergedData;
                }
            }
        }

        /// <summary>
        /// Remove a segment from SQL Server
        /// </summary>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            if (disposed)
            {
                callback?.Invoke(result);
                return;
            }

            var opId = Interlocked.Increment(ref operationSequence);
            Interlocked.Increment(ref pendingCount);

            var operation = new PendingOperation
            {
                OperationId = opId,
                OperationType = "Delete",
                Callback = null,
                Context = null,
                StartTime = DateTime.UtcNow
            };
            pendingOperations[opId] = operation;

            Task.Run(async () =>
            {
                try
                {
                    await ExecuteWithRetryAsync(async () =>
                    {
                        var conn = await connectionPool.GetAsync(disposeCts.Token).ConfigureAwait(false);
                        try
                        {
                            var deleteSql = $"DELETE FROM [{config.TableName}] WHERE SegmentId = @SegmentId";
                            
                            using (var cmd = new SqlCommand(deleteSql, conn))
                            {
                                cmd.Parameters.AddWithValue("@SegmentId", segment);
                                cmd.CommandTimeout = 60;
                                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                            }
                            return true;
                        }
                        finally
                        {
                            connectionPool.Return(conn);
                        }
                    }, disposeCts.Token).ConfigureAwait(false);

                    segmentMetadata.TryRemove(segment, out _);
                    pendingOperations.TryRemove(opId, out _);
                }
                catch (Exception)
                {
                    pendingOperations.TryRemove(opId, out _);
                    // Swallow exception, segment removal is best-effort
                }
                finally
                {
                    Interlocked.Decrement(ref pendingCount);
                    callback?.Invoke(result);
                }
            });
        }

        /// <summary>
        /// Check if throttling is needed
        /// </summary>
        public override bool Throttle()
        {
            return pendingCount > ThrottleLimit;
        }

        /// <summary>
        /// Get the file size of a segment
        /// </summary>
        public override long GetFileSize(int segment)
        {
            if (segmentMetadata.TryGetValue(segment, out var metadata))
            {
                return metadata.TotalSize;
            }

            // Query database for actual size
            try
            {
                var conn = connectionPool.Get();
                try
                {
                    var sizeSql = $@"
                        SELECT ISNULL(SUM(DATALENGTH(Data)), 0)
                        FROM [{config.TableName}]
                        WHERE SegmentId = @SegmentId";

                    using (var cmd = new SqlCommand(sizeSql, conn))
                    {
                        cmd.Parameters.AddWithValue("@SegmentId", segment);
                        cmd.CommandTimeout = 60;
                        var size = (long)cmd.ExecuteScalar();
                        
                        if (segmentMetadata.TryGetValue(segment, out metadata))
                        {
                            metadata.TotalSize = size;
                        }
                        
                        return size;
                    }
                }
                finally
                {
                    connectionPool.Return(conn);
                }
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>
        /// Execute an operation with retry logic for transient failures
        /// </summary>
        private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
        {
            int attempt = 0;
            while (true)
            {
                try
                {
                    return await operation().ConfigureAwait(false);
                }
                catch (SqlException ex) when (IsTransientError(ex) && attempt < config.MaxRetries)
                {
                    attempt++;
                    var delay = TimeSpan.FromMilliseconds(config.RetryDelayMs * Math.Pow(2, attempt - 1));
                    Debug.WriteLine($"Transient SQL error (attempt {attempt}/{config.MaxRetries}): {ex.Message}. Retrying in {delay.TotalMilliseconds}ms...");
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
                catch (TaskCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException("Operation was cancelled", cancellationToken);
                }
            }
        }

        /// <summary>
        /// Determines if a SQL exception is transient and should be retried
        /// </summary>
        private bool IsTransientError(SqlException ex)
        {
            // Common transient error codes
            // -2: Timeout
            // 1205: Deadlock
            // 4060: Cannot open database
            // 40197: Service error
            // 40501: Service busy
            // 40613: Database unavailable
            // 49918: Cannot process request
            // 49919: Cannot process create/update request
            // 49920: Cannot process request (too many operations)
            foreach (SqlError error in ex.Errors)
            {
                switch (error.Number)
                {
                    case -2:    // Timeout
                    case 1205:  // Deadlock
                    case 4060:  // Cannot open database
                    case 40197: // Service error
                    case 40501: // Service busy
                    case 40613: // Database unavailable
                    case 49918: // Cannot process request
                    case 49919: // Cannot process create/update
                    case 49920: // Too many operations
                        return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Detects hung operations based on timeout threshold
        /// </summary>
        private void DetectHangOperations(object state)
        {
            if (disposed) return;

            var now = DateTime.UtcNow;
            var threshold = TimeSpan.FromMilliseconds(config.OperationTimeoutMs);

            foreach (var kvp in pendingOperations)
            {
                var op = kvp.Value;
                var elapsed = now - op.StartTime;
                
                if (elapsed > threshold)
                {
                    Debug.WriteLine($"[SqlServerStorageDevice] HANG DETECTED: Operation {op.OperationId} ({op.OperationType}) has been pending for {elapsed.TotalSeconds:F1}s");
                }
            }
        }

        /// <summary>
        /// Maps exceptions to error codes
        /// </summary>
        private uint GetErrorCode(Exception ex)
        {
            if (ex is SqlException sqlEx)
            {
                return (uint)(sqlEx.Number & 0xFFFF);
            }
            else if (ex is IOException ioEx)
            {
                return (uint)(ioEx.HResult & 0xFFFF);
            }
            return uint.MaxValue;
        }

        /// <summary>
        /// Dispose the device and clean up resources
        /// </summary>
        public override void Dispose()
        {
            if (disposed) return;
            disposed = true;

            // Signal cancellation
            disposeCts.Cancel();

            // Stop hang detection
            hangDetectionTimer?.Dispose();

            // Delete table if configured
            if (config.DeleteOnClose)
            {
                try
                {
                    var conn = connectionPool.Get();
                    try
                    {
                        var dropSql = $"DROP TABLE IF EXISTS [{config.TableName}]";
                        using (var cmd = new SqlCommand(dropSql, conn))
                        {
                            cmd.CommandTimeout = 60;
                            cmd.ExecuteNonQuery();
                        }
                    }
                    finally
                    {
                        connectionPool.Return(conn);
                    }
                }
                catch
                {
                    // Best effort
                }
            }

            // Dispose connection pool
            connectionPool?.Dispose();

            // Clear tracking structures
            segmentMetadata.Clear();
            pendingOperations.Clear();

            disposeCts.Dispose();
        }
    }
}
