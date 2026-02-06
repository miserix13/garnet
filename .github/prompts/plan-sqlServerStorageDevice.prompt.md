# Plan: SQL Server Storage Device with Chunked Blobs

This implements a production-grade storage device that persists Tsavorite segments to SQL Server, splitting large segments into chunks to overcome the 2GB VARBINARY(MAX) limit. The device will include connection pooling, retry logic, hang detection, and robust error handling following patterns from AzureStorageDevice.

**Key Decisions:**
- **Chunked storage:** Segments split into configurable chunk size (default 256MB) stored as separate rows, enabling unlimited segment sizes
- **Core integration:** Lives in [libs/storage/Tsavorite/cs/src/core/Device/](libs/storage/Tsavorite/cs/src/core/Device/) alongside LocalStorageDevice
- **Configuration class:** `SqlServerStorageConfig` encapsulates connection string, table name, chunk size, pool size, retry policy
- **Advanced features:** Production-ready with transient error retry, connection health checks, operation timeout detection
- **Dependency:** Adds `Microsoft.Data.SqlClient` package reference to core project

**Steps**

1. **Create SqlServerStorageConfig.cs** in [libs/storage/Tsavorite/cs/src/core/Device/](libs/storage/Tsavorite/cs/src/core/Device/)
   - Properties: `ConnectionString`, `TableName` (default "TsavoriteSegments"), `ChunkSize` (default 256MB), `ConnectionPoolSize` (default 8), `MaxRetries` (default 3), `RetryDelayMs`, `OperationTimeoutMs` (hang detection threshold)
   - Validation method to check connection string validity and chunk size constraints
   - Constructor accepting connection string with sensible defaults

2. **Create SqlServerStorageDevice.cs** inheriting from `StorageDeviceBase`
   - Constructor: `SqlServerStorageDevice(SqlServerStorageConfig config, uint sectorSize = 512, long capacity = Devices.CAPACITY_UNSPECIFIED)`
   - Store config in readonly field, pass `config.TableName` as filename to base constructor
   - Implement `Initialize()` override to call base, then initialize connection pool and ensure schema exists

3. **Implement connection pooling infrastructure**
   - Add field `AsyncPool<SqlConnection> connectionPool` initialized with `config.ConnectionPoolSize`
   - Factory lambda opens connections from `config.ConnectionString`
   - Add `SafeConcurrentDictionary<int, SegmentMetadata>` tracking segment existence and chunk count
   - Add `ConcurrentDictionary<long, PendingOperation>` for operation tracking with sequence ID generator
   - Define internal `PendingOperation` struct: operation ID, type, callback, context, timestamp for hang detection
   - Add `int pendingCount` field with Interlocked operations for throttling

4. **Create database schema initialization**
   - Method `EnsureSchemaAsync()` called during Initialize
   - Creates table if not exists: `CREATE TABLE [{TableName}] (SegmentId INT, ChunkIndex INT, Data VARBINARY(MAX), PRIMARY KEY (SegmentId, ChunkIndex))`
   - Consider adding index on SegmentId for faster lookups
   - Use retry wrapper for transient failures during schema operations

5. **Implement ReadAsync() with chunked logic**
   - Calculate which chunks span the read range: start chunk = sourceAddress / chunkSize, end chunk = (sourceAddress + readLength - 1) / chunkSize
   - Acquire connection from pool using `await connectionPool.GetAsync()`
   - Execute SQL query: `SELECT ChunkIndex, Data FROM [{TableName}] WHERE SegmentId = @segId AND ChunkIndex BETWEEN @start AND @end ORDER BY ChunkIndex`
   - Copy bytes from returned chunks to destination IntPtr at correct offsets using `Marshal.Copy` or unsafe pointers
   - Handle partial chunks (read may start/end mid-chunk)
   - Wrap in retry logic for transient SQL errors (deadlocks, timeouts)
   - Track operation in pendingOperations dictionary, invoke callback with (errorCode, bytesRead, context), return connection to pool

6. **Implement WriteAsync() with chunked logic**
   - Split write into chunks: calculate chunk range similar to ReadAsync
   - For each chunk, extract bytes from source IntPtr using UnmanagedMemoryStream or unsafe code
   - Use SqlCommand with `MERGE` or `INSERT/UPDATE` logic: `MERGE [{TableName}] USING (VALUES (@segId, @chunkIdx, @data)) AS src ON SegmentId=@segId AND ChunkIndex=@chunkIdx WHEN MATCHED THEN UPDATE SET Data=@data WHEN NOT MATCHED THEN INSERT VALUES (@segId, @chunkIdx, @data)`
   - Consider SqlTransaction wrapping all chunks for atomic writes (configurable?)
   - Track segment metadata: update chunk count in SegmentMetadata dictionary
   - Wrap in retry logic, track operation, invoke callback on completion

7. **Implement RemoveSegmentAsync()**
   - Execute `DELETE FROM [{TableName}] WHERE SegmentId = @segId`
   - Wrap in retry logic
   - Remove from SegmentMetadata dictionary
   - Invoke AsyncCallback with result parameter

8. **Implement Throttle() override**
   - Return `pendingCount > ThrottleLimit` to respect configured concurrency limit
   - Update pendingCount with `Interlocked.Increment/Decrement` in all async operations

9. **Add retry logic helper**
   - Method `ExecuteWithRetryAsync<T>(Func<Task<T>> operation, CancellationToken ct)`
   - Catch `SqlException`, check for transient error codes (timeout=-2, deadlock=1205, connection failures 4060, 40197, 40501, 40613)
   - Exponential backoff: wait `config.RetryDelayMs * Math.Pow(2, attemptNumber)` up to `config.MaxRetries`
   - Non-transient errors fail immediately
   - Log retry attempts (consider adding ILogger support)

10. **Implement hang detection**
    - Add `System.Threading.Timer hangDetectionTimer` initialized in Initialize, runs every 30 seconds
    - Callback scans `pendingOperations`, identifies operations older than `config.OperationTimeoutMs`
    - Log warnings with operation ID, type, elapsed time
    - Optionally cancel hung operations (requires CancellationToken threading through operations)

11. **Implement Dispose()**
    - Stop hang detection timer
    - Set disposed flag to reject new operations
    - Cancel all pending operations gracefully
    - Dispose connection pool (calls Dispose on all pooled connections)
    - Clear SegmentMetadata and pendingOperations dictionaries
    - Call base.Dispose()

12. **Add optional advanced features**
    - Override `GetFileSize(int segment)`: query `SELECT SUM(DATALENGTH(Data)) FROM [{TableName}] WHERE SegmentId = @segId`
    - Implement `TruncateUntilAddress()`: delete segments below threshold
    - Add connection health checks: before returning from pool, validate `connection.State == Open`, reconnect if broken
    - Consider batch write optimization: queue small writes, flush periodically

13. **Update Devices.cs factory** in [libs/storage/Tsavorite/cs/src/core/Device/Devices.cs](libs/storage/Tsavorite/cs/src/core/Device/Devices.cs)
    - Add `DeviceType.SqlServer` enum value if using factory pattern
    - Add factory method `CreateSqlServerDevice(SqlServerStorageConfig config)` returning `IDevice`

14. **Add package reference**
    - Edit [libs/storage/Tsavorite/cs/src/core/Tsavorite.core.csproj](libs/storage/Tsavorite/cs/src/core/Tsavorite.core.csproj)
    - Add `<PackageReference Include="Microsoft.Data.SqlClient" Version="5.x.x" />`

**Verification**

1. **Unit tests** in [test/Garnet.test/](test/Garnet.test/):
   - Test basic read/write/delete operations
   - Test chunked writes spanning multiple chunks
   - Test partial chunk reads (offset within chunk)
   - Test concurrent operations (verify thread safety)
   - Test retry logic with simulated transient failures
   - Test connection pool exhaustion and recovery

2. **Integration test:**
   - Run against real SQL Server instance (LocalDB or container)
   - Initialize Tsavorite log with SqlServerStorageDevice
   - Perform typical workload (inserts, reads, updates, checkpoints)
   - Verify data persistence across restarts
   - Monitor connection pool metrics
   - Induce transient failures (kill connections) and verify recovery

3. **Performance benchmark:**
   - Compare throughput vs LocalStorageDevice using [benchmark/Device.benchmark/](benchmark/Device.benchmark/)
   - Measure latency impact of chunking for large segments
   - Test connection pool tuning (vary pool size)

4. **Commands to validate:**
   ```bash
   dotnet build libs/storage/Tsavorite/cs/src/core/Tsavorite.core.csproj
   dotnet test test/Garnet.test/Garnet.test.csproj --filter Category=SqlServerDevice
   ```

**Decisions**
- **Chunk size 256MB:** Balances SQL Server buffer pool pressure vs. network round-trips; configurable for tuning
- **MERGE over INSERT/UPDATE:** Simpler upsert logic, SQL Server optimized since 2008
- **Shared connection pool:** Read and write operations share pool (unlike ManagedLocalStorageDevice); simplifies management
- **Synchronous schema init:** Initialize() creates table synchronously before any I/O; prevents race conditions
- **No distributed transactions:** Each operation in its own implicit transaction for simplicity; multi-chunk writes could use explicit transactions if atomicity needed

---

This plan delivers a production-ready SQL Server storage device with enterprise-grade reliability, ready to handle workloads requiring durable remote storage with SQL Server's management, backup, and HA capabilities.
