using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using Dapper;
using NLog;
using System.Text.RegularExpressions;

namespace HSBulkCopy
{
    abstract class CopyInfo
    {
        public string TableName;
        public int PartitionNumber;
        public abstract string GetPredicate();
    }

    class NoPartitionsCopyInfo : CopyInfo
    {
        public override string GetPredicate()
        {
            return String.Empty;
        }
    }

    class PhysicalPartitionCopyInfo : CopyInfo
    {
        public string PartitionFunction;
        public string PartitionColumn;

        public override string GetPredicate()
        {
            return $"$partition.{PartitionFunction}({PartitionColumn}) = {PartitionNumber}";
        }
    }

    class LogicalPartitionCopyInfo : CopyInfo
    {
        public int LogicalPartitionsCount;
        public override string GetPredicate()
        {
            if (LogicalPartitionsCount > 1)
                return $"ABS(CAST(%%PhysLoc%% AS BIGINT)) % {LogicalPartitionsCount} = {PartitionNumber - 1}";
            else
                return String.Empty;
        }
    }

    class SmartBulkCopy
    {
        private readonly ILogger _logger;
        private readonly SmartBulkCopyConfiguration _config;
        private readonly Stopwatch _stopwatch = new Stopwatch();
        private readonly ConcurrentQueue<CopyInfo> _queue = new ConcurrentQueue<CopyInfo>();
        private readonly List<string> _tablesToCopy = new List<string>();
        private readonly ConcurrentDictionary<string, string> _activeTasks = new ConcurrentDictionary<string, string>();
        private long _runningTasks = 0;

        public SmartBulkCopy(SmartBulkCopyConfiguration config, ILogger logger)
        {
            _logger = logger;
            _config = config;

            var v = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString();

            _logger.Info($"SmartBulkCopy engine v. {v}");
        }

        public async Task<int> Copy()
        {
            return await Copy(_config.TablesToCopy);
        }

        public async Task<int> Copy(List<String> tablesToCopy)
        {
            _logger.Info("Starting smart bulk copy process...");

            _logger.Info($"Using up to {_config.MaxParallelTasks} parallel tasks to copy data between databases.");
            _logger.Info($"Batch Size is set to: {_config.BatchSize}.");

            if (_config.TruncateTables)
                _logger.Info("Destination tables will be truncated.");

            _logger.Info("Testing connections...");

            var t1 = TestConnection(_config.SourceConnectionString);
            var t2 = TestConnection(_config.DestinationConnectionString);

            await Task.WhenAll(t1, t2);

            if (await t1 != true || await t2 != true) return 1;

            _logger.Info("Checking that source is a snapshot...");
            var isSourceSnapshot = CheckDatabaseSnapshot();

            if (!isSourceSnapshot)
            {
                if (_config.CheckUsingSnapshot)
                {
                    _logger.Error("Source database must be a database snapshot");
                    return 1;
                }
                else
                {
                    _logger.Warn("WARNING! Source database is NOT a database snapshot AND database snapshot check has been turned off.");
                    _logger.Warn("WARNING! Make sure data in source database is not changed during bulk copy process to avoid inconsistencies.");
                }
            }

            _logger.Info("Initializing copy process...");

            var conn = new SqlConnection(_config.SourceConnectionString);
            var tasks = new List<Task>();

            var internalTablesToCopy = new List<String>();            
            foreach(var t in tablesToCopy.Distinct())
            {
                if (t.Contains("*")) 
                {
                    _logger.Info("Getting list of tables to copy...");                    
                    var tables = conn.Query(@"
                        select 
                            [Name] = QUOTENAME(s.[name]) + '.' + QUOTENAME(t.[name]) 
                        from 
                            sys.tables t 
                        inner join 
                            sys.schemas s on t.[schema_id] = s.[schema_id] 
                        inner join 
                            sys.objects o on t.[object_id] = o.[object_id] 
                        where
                            o.is_ms_shipped = 0
                    ");
                    var regExPattern = t.Replace(".", "[.]").Replace("*", ".*");
                    foreach (var tb in tables)
                    {
                        bool matches = Regex.IsMatch(tb.Name.Replace("[", "").Replace("]", ""), regExPattern); // TODO: Improve wildcard matching
                        if (matches) {
                            _logger.Info($"Adding {tb.Name}...");
                            internalTablesToCopy.Add(tb.Name);
                        }
                    }
                } else {
                    _logger.Info($"Adding {t}...");
                    internalTablesToCopy.Add(t);
                }
            }

            _tablesToCopy.AddRange(internalTablesToCopy);

            _logger.Info("Analyzing tables...");
            var copyInfo = new List<CopyInfo>();
            foreach (var t in internalTablesToCopy)
            {
                // Check it tables exists
                if (!await CheckTableExistence(_config.SourceConnectionString, t))
                {
                    _logger.Error($"Table {t} does not exists on source.");
                    return 1;
                }
                if (!await CheckTableExistence(_config.DestinationConnectionString, t))
                {
                    _logger.Error($"Table {t} does not exists on destination.");
                    return 1;
                }

                // Check if table is big enough to use partitions
                var isBigEnough = CheckTableSize(t);

                if (isBigEnough)
                {
                    // Check if table is partitioned
                    var isPartitioned = CheckIfSourceTableIsPartitioned(t);

                    // Create the Work Info data based on partitio lind
                    if (isPartitioned)
                    {
                        copyInfo.AddRange(CreatePhysicalPartitionedTableCopyInfo(t));
                    }
                    else
                    {
                        copyInfo.AddRange(CreateLogicalPartitionedTableCopyInfo(t));
                    }
                }
                else
                {
                    _logger.Info($"Table {t} is small, partitioned copy will not be used.");
                    copyInfo.Add(new NoPartitionsCopyInfo
                    {
                        TableName = t,
                        PartitionNumber = 1
                    });
                }
            }

            _logger.Info("Enqueing work...");
            copyInfo.ForEach(ci => _queue.Enqueue(ci));
            _logger.Info($"{_queue.Count} items enqueued.");

            if (_config.TruncateTables)
            {
                _logger.Info("Truncating destination tables...");
                internalTablesToCopy.ForEach(t => TruncateDestinationTable(t));
            }

            _logger.Info($"Copying using {_config.MaxParallelTasks} parallel tasks.");
            foreach (var i in Enumerable.Range(1, _config.MaxParallelTasks))
            {
                tasks.Add(new Task(() => BulkCopy(i)));
            }

            _logger.Info($"Starting monitor...");
            var monitorTask = Task.Run(() => MonitorCopyProcess());

            _logger.Info($"Start copying...");
            _stopwatch.Start();
            tasks.ForEach(t => t.Start());
            await Task.WhenAll(tasks.ToArray());
            _stopwatch.Stop();
            _logger.Info($"Done copying.");

            _logger.Info($"Waiting for monitor to shut down...");
            monitorTask.Wait();

            _logger.Info("Checking source and destination row counts...");
            bool rowsChecked = await CheckResults();
            int result = 0 ;

            if (!rowsChecked)
            {
                _logger.Warn("WARNING! Source and Destination table have a different number of rows!");
                result = 2;
            }

            _logger.Info("Done in {0:#.00} secs.", (double)_stopwatch.ElapsedMilliseconds / 1000.0);

            return result;
        }

        private bool CheckTableSize(string tableName)
        {
            string sql = @"
                select 
                    sum(row_count) as row_count 
                from 
                    sys.dm_db_partition_stats 
                where 
                    [object_id] = object_id(@tableName) 
                and 
                    index_id in (0, 1) 
                group by 
                    [object_id]
            ";

            _logger.Debug($"Executing: {sql}");

            var conn = new SqlConnection(_config.SourceConnectionString);
            var rowCount = conn.ExecuteScalar<int>(sql, new { @tableName = tableName });
            return (rowCount > _config.BatchSize);
        }

        private bool CheckDatabaseSnapshot()
        {
            var conn = new SqlConnection(_config.SourceConnectionString);
            var snapshotName = conn.ExecuteScalar("select [name] from sys.databases where source_database_id is not null and database_id = db_id()");
            return (snapshotName != null);
        }

        private async Task<bool> CheckResults()
        {
            var connSource = new SqlConnection(_config.SourceConnectionString);
            var connDest = new SqlConnection(_config.DestinationConnectionString);
            bool result = true;
            string sql = @"
                select 
                    sum(row_count) as row_count 
                from 
                    sys.dm_db_partition_stats 
                where 
                    [object_id] = object_id(@tableName) 
                and 
                    index_id in (0, 1) 
                group by 
                    [object_id]
            ";

            foreach (var t in _tablesToCopy)
            {
                _logger.Debug($"Executing: {sql}");

                var ts = connSource.ExecuteScalarAsync<int>(sql, new { @tableName = t });
                var td = connDest.ExecuteScalarAsync<int>(sql, new { @tableName = t });

                await Task.WhenAll(ts, td);

                int sourceRows = await ts;
                int destRows = await td;

                if (sourceRows == destRows)
                {
                    _logger.Info($"Table {t} has {sourceRows} rows both in source and destination.");
                }
                else
                {
                    _logger.Error($"Table {t} has {sourceRows} rows in source and {destRows} rows in destination!");
                    result = false;
                }
            }

            return result;
        }

        private bool CheckIfSourceTableIsPartitioned(string tableName)
        {
            var conn = new SqlConnection(_config.SourceConnectionString);

            var isPartitioned = (int)conn.ExecuteScalar($@"
                    select 
                        IsPartitioned = case when count(*) > 1 then 1 else 0 end 
                    from 
                        sys.dm_db_partition_stats 
                    where 
                        [object_id] = object_id(@tableName) 
                    and 
                        index_id in (0,1)
                    ", new { @tableName = tableName });

            return (isPartitioned == 1);
        }

        private void TruncateDestinationTable(string tableName)
        {
            _logger.Info($"Truncating '{tableName}'...");
            var destinationConnection = new SqlConnection(_config.DestinationConnectionString);
            destinationConnection.ExecuteScalar($"TRUNCATE TABLE {tableName}");
        }

        private List<CopyInfo> CreatePhysicalPartitionedTableCopyInfo(string tableName)
        {
            var copyInfo = new List<CopyInfo>();

            var conn = new SqlConnection(_config.SourceConnectionString);

            var sql1 = $@"
                    select 
                        partitions = count(*) 
                    from 
                        sys.dm_db_partition_stats 
                    where 
                        [object_id] = object_id(@tableName) 
                    and
                        index_id in (0,1)
                    ";

            _logger.Debug($"Executing: {sql1}");

            var partitionCount = (int)conn.ExecuteScalar(sql1, new { @tableName = tableName });

            _logger.Info($"Table {tableName} is partitioned. Bulk copy will be parallelized using {partitionCount} partition(s).");

            var sql2 = $@"
                select 
                    pf.[name] as PartitionFunction,
                    c.[name] as PartitionColumn,
                    pf.[fanout] as PartitionCount
                from 
                    sys.indexes i 
                inner join
                    sys.partition_schemes ps on i.data_space_id = ps.data_space_id
                inner join
                    sys.partition_functions pf on ps.function_id = pf.function_id
                inner join
                    sys.index_columns ic on i.[object_id] = ic.[object_id] and i.index_id = ic.index_id
                inner join
                    sys.columns c on c.[object_id] = i.[object_id] and c.column_id = ic.column_id
                where 
                    i.[object_id] = object_id(@tableName) 
                and 
                    i.index_id in (0,1)
                and
                    ic.partition_ordinal = 1
                ";

            var partitionInfo = conn.QuerySingle(sql2, new { @tableName = tableName });

            _logger.Debug($"Executing: {sql2}");

            foreach (var n in Enumerable.Range(1, partitionCount))
            {
                var cp = new PhysicalPartitionCopyInfo();
                cp.PartitionNumber = n;
                cp.TableName = tableName;
                cp.PartitionColumn = partitionInfo.PartitionColumn;
                cp.PartitionFunction = partitionInfo.PartitionFunction;

                copyInfo.Add(cp);
            }

            return copyInfo;
        }

        private List<CopyInfo> CreateLogicalPartitionedTableCopyInfo(string tableName)
        {
            _logger.Info($"Table {tableName} is NOT partitioned. Bulk copy will be parallelized using {_config.LogicalPartitions} logical partitions.");

            var copyInfo = new List<CopyInfo>();

            foreach (var n in Enumerable.Range(1, _config.LogicalPartitions))
            {
                var cp = new LogicalPartitionCopyInfo();
                cp.PartitionNumber = n;
                cp.TableName = tableName;
                cp.LogicalPartitionsCount = _config.LogicalPartitions;

                copyInfo.Add(cp);
            }

            return copyInfo;
        }

        private void BulkCopy(int taskId)
        {
            CopyInfo copyInfo;
            _logger.Info($"Task {taskId}: Started...");

            Interlocked.Add(ref _runningTasks, 1);

            try
            {

                while (_queue.TryDequeue(out copyInfo))
                {
                    if (copyInfo is NoPartitionsCopyInfo) 
                        _logger.Info($"Task {taskId}: Processing table {copyInfo.TableName}");
                    else 
                        _logger.Info($"Task {taskId}: Processing table {copyInfo.TableName} partition {copyInfo.PartitionNumber}...");                                     

                    _activeTasks.AddOrUpdate(taskId.ToString(), copyInfo.TableName, (_1, _2) => { return copyInfo.TableName; });
                    
                    var sourceConnection = new SqlConnection(_config.SourceConnectionString);
                    var whereClause = string.Empty;
                    var predicate = copyInfo.GetPredicate();
                    if (!string.IsNullOrEmpty(predicate)) {
                        whereClause = $" WHERE {predicate}";
                    };
                    var sql = $"SELECT * FROM {copyInfo.TableName}{whereClause}";

                    _logger.Debug($"Task {taskId}: Executing: {sql}");
                    var sourceReader = sourceConnection.ExecuteReader(sql);

                    using (var bulkCopy = new SqlBulkCopy(_config.DestinationConnectionString + $";Application Name=smartbulkcopy{taskId}", SqlBulkCopyOptions.TableLock))
                    {
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.BatchSize = _config.BatchSize;
                        bulkCopy.DestinationTableName = copyInfo.TableName;

                        try
                        {
                            bulkCopy.WriteToServer(sourceReader);
                        }
                        finally
                        {
                            sourceReader.Close();
                        }
                    }

                    if (copyInfo is NoPartitionsCopyInfo)                         
                        _logger.Info($"Task {taskId}: Table {copyInfo.TableName} copied.");
                    else 
                        _logger.Info($"Task {taskId}: Table {copyInfo.TableName}, partition {copyInfo.PartitionNumber} copied.");
                }

                _logger.Info($"Task {taskId}: Completed successfully.");                
            }
            catch (Exception ex)
            {
                _logger.Error($"Task {taskId}: {ex.Message}");
                var ie = ex.InnerException;
                while (ie != null)
                {
                    _logger.Error($"Task {taskId}: {ex.Message}");
                    ie = ie.InnerException;
                }
                _logger.Error($"Task {taskId}: Completed with errors.");                
            } 
            finally 
            {
                string dummy = string.Empty;
                _activeTasks.Remove(taskId.ToString(), out dummy);
                Interlocked.Add(ref _runningTasks, -1);
            }            
        }

        private void MonitorCopyProcess()
        {
            var conn = new SqlConnection(_config.DestinationConnectionString + ";Application Name=hsbulk_log_monitor");
            var instance_name = (string)(conn.ExecuteScalar($"select instance_name from sys.dm_os_performance_counters where counter_name = 'Log Bytes Flushed/sec' and instance_name like '%-%-%-%-%'"));

            string query = $@"
                declare @v1 bigint, @v2 bigint
                select @v1 = cntr_value from sys.dm_os_performance_counters 
                where counter_name = 'Log Bytes Flushed/sec' and instance_name = '{instance_name}';
                waitfor delay '00:00:05';
                select @v2 = cntr_value from sys.dm_os_performance_counters 
                where counter_name = 'Log Bytes Flushed/sec' and instance_name = '{instance_name}';
                select log_flush_mb_sec =  ((@v2-@v1) / 5.) / 1024. / 1024.
            ";

            while (true)
            {
                var runningTasks = Interlocked.Read(ref _runningTasks);
                if (_queue.Count == 0 && runningTasks == 0) break;

                var log_flush = (decimal)(conn.ExecuteScalar(query));
                var copyingTables = String.Join(',', _activeTasks.Values.Distinct().ToArray());
                if (copyingTables == "") copyingTables = "None";
                _logger.Info($"Log Flush Speed: {log_flush:00.00} MB/Sec, {runningTasks} Running Tasks, Tables being copied: {copyingTables}.");
            }
        }

        async Task<bool> TestConnection(string connectionString)
        {
            var builder = new SqlConnectionStringBuilder(connectionString);

            _logger.Info($"Testing connection to: {builder.DataSource}...");

            var conn = new SqlConnection(connectionString);
            bool result = false;

            try
            {
                await conn.OpenAsync();
                result = true;
                _logger.Info($"Connection to {builder.DataSource} succeeded.");
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error while opening connection.");
            }
            finally
            {
                conn.Close();
            }

            return result;
        }

        private async Task<bool> CheckTableExistence(string connectionString, string tableName)
        {
            bool result = false;
            var conn = new SqlConnection(connectionString);
            try
            {
                await conn.QuerySingleAsync(@"select 
                        [FullName] = QUOTENAME(s.[name]) + '.' + QUOTENAME(t.[name]) 
                    from 
                        sys.tables t 
                    inner join 
                        sys.schemas s on t.[schema_id] = s.[schema_id]
                    where
                        s.[name] = PARSENAME(@tableName, 2)
                    and
                        t.[name] = PARSENAME(@tableName, 1)", new { @tableName = tableName });
                result = true;
            }
            catch (InvalidOperationException)
            {
                result = false;
            }
            finally
            {
                conn.Close();
            }

            return result;
        }
    }
}