using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using Dapper;
using NLog;
using System.Text.RegularExpressions;

namespace SmartBulkCopy
{
    class SmartBulkCopy
    {
        // Added all error code listed here: "https://docs.microsoft.com/en-us/azure/sql-database/sql-database-develop-error-messages"
        // Added Error Code 0 to automatically handle killed connections
        // Added Error Code 4891 to automatically handle "Insert bulk failed due to a schema change of the target table" error
        // Added Error Code 10054 to handle "The network path was not found" error that could happen if connection is severed (e.g.: cable unplugged)
        // Added Error Code 53 to handle "No such host is known" error that could happen if connection is severed (e.g.: cable unplugged)
        // Added Error Code 11001 to handle transient network errors
        // Added Error Code 10065 to handle transient network errors
        // Added Error Code 10060 to handle transient network errors
        // Added Error Code 121 to handle transient network errors
        // Added Error Code 258 to handle transient login erros

        private readonly List<int> _transientErrors = new List<int>() { 0, 53, 121, 258, 4891, 10054, 4060, 40197, 40501, 40613, 49918, 49919, 49920, 10054, 11001, 10065, 10060, 10051};
        private int _maxAttempts = 5;
        private int _delay = 10; // seconds
        private readonly ILogger _logger;
        private readonly SmartBulkCopyConfiguration _config;
        private readonly Stopwatch _stopwatch = new Stopwatch();
        private readonly ConcurrentQueue<CopyInfo> _queue = new ConcurrentQueue<CopyInfo>();
        private readonly List<string> _tablesToCopy = new List<string>();
        private readonly ConcurrentDictionary<string, string> _activeTasks = new ConcurrentDictionary<string, string>();
        private long _runningTasks = 0;
        private long _erroredTasks = 0;

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
            _logger.Info($"Logical Partition Strategy: {_config.LogicalPartitioningStrategy}");
            if (_config.LogicalPartitioningStrategy != LogicalPartitioningStrategy.Auto)
            {
                _logger.Info($"Logical Partitions: {_config.LogicalPartitions}");
            }

            if (_config.TruncateTables)
                _logger.Info("Destination tables will be truncated.");

            _logger.Info("Testing connections...");

            var t1 = TestConnection(_config.SourceConnectionString);
            var t2 = TestConnection(_config.DestinationConnectionString);

            _maxAttempts = _config.RetryMaxAttempt;
            _delay = _config.RetryDelayIncrement;

            _logger.Info($"Connection retry logic: max {_maxAttempts} times, with {_delay} seconds increment for each attempt.");

            await Task.WhenAll(t1, t2);

            if (await t1 != true || await t2 != true) return 1;

            if (_config.SafeCheck != SafeCheck.None)
            {
                _logger.Info("Executing security checks...");

                if (_config.SafeCheck == SafeCheck.Snapshot && !CheckDatabaseSnapshot())
                {
                    _logger.Error("Source database must be a database snapshot");
                    return 1;
                }
                if (_config.SafeCheck == SafeCheck.ReadOnly && !CheckDatabaseReadonly())
                {
                    _logger.Error("Source database must set to readonly");
                    return 1;
                }

                _logger.Info("Security check passed: source database is immutable.");
            }
            else
            {
                _logger.Warn("WARNING! Source safety checks disabled.");
                _logger.Warn("WARNING! It is recommended to enable 'safe-check' option by setting it to 'readonly' or 'snapshot'.");
                _logger.Warn("WARNING! Make sure data in source database is not changed during bulk copy process to avoid inconsistencies.");
            }

            _logger.Info("Initializing copy process...");

            var internalTablesToCopy = GetTablesToCopy(tablesToCopy.Distinct());
            _tablesToCopy.AddRange(internalTablesToCopy);

            _logger.Info("Analyzing tables...");
            var copyInfo = new List<CopyInfo>();
            foreach (var t in internalTablesToCopy)
            {
                // By default try to use partitioned load
                bool usePartitioning = true;

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

                // Check if there is a compatibile clustered index on the target table
                // so that ordered bulk load could be used
                var ciSource = await GetTableClusteredIndex(_config.SourceConnectionString, t);
                var ciDestination = await GetTableClusteredIndex(_config.DestinationConnectionString, t);
                bool canUseOrderHint = false;
                if (ciSource.GetOrderBy() == ciDestination.GetOrderBy())
                {
                   _logger.Info($"Table {t} has compatible clustered index on both source and target database. ORDER hint can be used."); 
                   canUseOrderHint = true;                   
                }
 
                // Check if table is partitioned
                var isSourcePartitioned = await CheckIfTableIsPartitioned(_config.SourceConnectionString, t);

                // If table is not partitined and has a clustered index, 
                // logical partitioning CANNOT be used
                if (canUseOrderHint == true && isSourcePartitioned == false)
                {
                   _logger.Info($"Table {t} has a clustered index but no physical partitions. Forcing 1 logical partition and ordered load.") ;
                   usePartitioning = false;
                }

                // Use partitions if that make sense
                var partitionType = "Unknown";
                if (usePartitioning == true)
                {                    
                    var tableSize = GetTableSize(t);

                    // Check if table is big enough to use partitions
                    if (tableSize.RowCount > _config.BatchSize || tableSize.SizeInGB > 1)
                    {
                        // Create the Work Info data based on partition type
                        if (isSourcePartitioned)
                        {
                            var cis = CreatePhysicalPartitionedTableCopyInfo(t);
                            if (canUseOrderHint) cis.ForEach(ci => ci.ClusteredIndex = ciSource);
                            copyInfo.AddRange(cis);
                            partitionType = "Physical";

                        }
                        else
                        {
                            var cis = CreateLogicalPartitionedTableCopyInfo(t, tableSize);                        
                            copyInfo.AddRange(cis);
                            partitionType = "Logical";
                        } 
                    }
                    else
                    {
                        _logger.Info($"Table {t} is small, partitioned copy will not be used.");      
                        usePartitioning = false;                           
                    }     
                } 

                // Otherwise just copy the table, possibility using 
                // and ordered bulk copy                                
                if (usePartitioning == false) {
                    var columns = GetColumnsForBulkCopy(t);
                    var ci = new NoPartitionsCopyInfo() { TableName = t };
                    ci.Columns.AddRange(columns);
                    if (canUseOrderHint) ci.ClusteredIndex = ciSource;
                    copyInfo.Add(ci);      
                    partitionType = "None";
                }

                 _logger.Info($"Table {t} analysis result: usePartioning={usePartitioning}, partitionType={partitionType}, canUseOrderHint={canUseOrderHint}");   
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
            var tasks = new List<Task>();
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
            if (Interlocked.Read(ref _erroredTasks) == 0)
            {
                _logger.Info($"Done copying.");
                _logger.Info($"Waiting for monitor to shut down...");
                monitorTask.Wait();
            } else {
                // TODO: cancel monitor execution
            }

            int result = 0;
            
            if (Interlocked.Read(ref _erroredTasks) == 0)
            {
                _logger.Info("Checking source and destination row counts...");
                bool rowsChecked = await CheckResults();                

                if (!rowsChecked)
                {
                    _logger.Warn("WARNING! Source and Destination table have a different number of rows!");
                    result = 2;
                }
                else
                {
                    _logger.Info("All tables copied correctly.");
                }

                _logger.Info("Done in {0:#.00} secs.", (double)_stopwatch.ElapsedMilliseconds / 1000.0);
            } else {                
                _logger.Warn("Completed with errors.");
                result = 3;            
            }

            return result;
        }

        private async Task<ClusteredIndexInfo> GetTableClusteredIndex(string connectionString, string tableName)
        {
            var sqsb = new SqlConnectionStringBuilder(connectionString);

            var result = new ClusteredIndexInfo(); 

            string sql = @"
                select
                    c.name as ColumnName,
                    ic.key_ordinal as OrdinalPosition,
                    ic.is_descending_key as IsDescending,
                    ic.partition_ordinal
                from
                    sys.indexes i
                inner join
                    sys.index_columns ic on ic.index_id = i.index_id and ic.[object_id] = i.[object_id] 
                inner join
                    sys.columns c on ic.column_id = c.column_id and ic.[object_id] = c.[object_id]
                where
                    i.[object_id] = object_id(@tableName) 
                and
                    i.[type] = 1
                order by
                    ic.key_ordinal,
                    ic.partition_ordinal
            ";
            
            _logger.Debug($"Executing: {sql}, @tableName = {tableName}, @server = {sqsb.DataSource}");

            var conn = new SqlConnection(connectionString);
            var qr = await conn.QueryAsync<IndexColumn>(sql, new { @tableName = tableName });            
    
            result.IndexColumns.AddRange(qr.ToList());
            
            if (result.IndexColumns.Count > 0)
            {
                _logger.Debug($"Detected Clustered Index on {tableName}@{sqsb.DataSource}: {result.GetOrderBy()}");
            }

            return result;
        }

        private TableSize GetTableSize(string tableName)
        {
            string sql = @"
                select 
                    sum(row_count) as row_count,
                    (sum(used_page_count) * 8) / 1024. / 1024 as size_gb
                from 
                    sys.dm_db_partition_stats 
                where 
                    [object_id] = object_id(@tableName) 
                and 
                    index_id in (0, 1) 
                group by 
                    [object_id]
            ";

            _logger.Debug($"Executing: {sql}, @tableName = {tableName}");

            var conn = new SqlConnection(_config.SourceConnectionString);
            var qr = conn.QuerySingle(sql, new { @tableName = tableName });            
            return new TableSize() { RowCount = (long)(qr.row_count), SizeInGB = (int)(qr.size_gb) };
        }

        private bool CheckDatabaseSnapshot()
        {
            var conn = new SqlConnection(_config.SourceConnectionString);
            var snapshotName = conn.ExecuteScalar("select [name] from sys.databases where source_database_id is not null and database_id = db_id()");
            return (snapshotName != null);
        }

        private bool CheckDatabaseReadonly()
        {
            var conn = new SqlConnection(_config.SourceConnectionString);
            var isReadOnly = conn.ExecuteScalar<int>("SELECT [is_read_only] FROM sys.databases WHERE [database_id] = DB_ID()");
            return (isReadOnly == 1);
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
                _logger.Debug($"Executing: {sql}, @tableName = {t}");

                var ts = connSource.ExecuteScalarAsync<long>(sql, new { @tableName = t });
                var td = connDest.ExecuteScalarAsync<long>(sql, new { @tableName = t });

                await Task.WhenAll(ts, td);

                var sourceRows = await ts;
                var destRows = await td;

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

        private async Task<bool> CheckIfTableIsPartitioned(string connectionString, string tableName)
        {
             var sqsb = new SqlConnectionStringBuilder(connectionString);

            var conn = new SqlConnection(connectionString);

            string sql = @"
                select 
                    IsPartitioned = case when count(*) > 1 then 1 else 0 end 
                from 
                    sys.dm_db_partition_stats 
                where 
                    [object_id] = object_id(@tableName) 
                and 
                    index_id in (0,1)
            ";
            
            _logger.Debug($"Executing: {sql}, @tableName = {tableName}, @server = {sqsb.DataSource}");

            var isPartitioned = await conn.ExecuteScalarAsync<int>(sql, new { @tableName = tableName });

            return (isPartitioned == 1);
        }

        private void TruncateDestinationTable(string tableName)
        {
            _logger.Info($"Truncating '{tableName}'...");
            var destinationConnection = new SqlConnection(_config.DestinationConnectionString);
            destinationConnection.ExecuteScalar($"TRUNCATE TABLE {tableName}");
        }

        private List<String> GetColumnsForBulkCopy(string tableName)
        {
            _logger.Debug($"Creating column list for {tableName}...");
            var conn = new SqlConnection(_config.SourceConnectionString);

            var sql = $@"
                    select 
                        [name] 
                    from 
                        sys.columns 
                    where 
                        [object_id] = object_id(@tableName) 
                    and
                        [is_computed] = 0 
                    and 
                        [is_column_set] = 0
                    ";

            _logger.Debug($"Executing: {sql}, @tableName = {tableName}");

            var columns = conn.Query<string>(sql, new { @tableName = tableName });

            return columns.ToList();
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

            _logger.Debug($"Executing: {sql1}, @tableName = {tableName}");

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

            _logger.Debug($"Executing: {sql2}, @tableName = {tableName}");

            var partitionInfo = conn.QuerySingle(sql2, new { @tableName = tableName });

            var columns = GetColumnsForBulkCopy(tableName);           

            foreach (var n in Enumerable.Range(1, partitionCount))
            {
                var cp = new PhysicalPartitionCopyInfo();
                cp.PartitionNumber = n;
                cp.TableName = tableName;
                cp.PartitionColumn = partitionInfo.PartitionColumn;
                cp.PartitionFunction = partitionInfo.PartitionFunction;
                cp.Columns.AddRange(columns);                

                copyInfo.Add(cp);
            }

            return copyInfo;
        }

        private List<CopyInfo> CreateLogicalPartitionedTableCopyInfo(string tableName, TableSize tableSize)
        {
            var copyInfo = new List<CopyInfo>();

            var columns = GetColumnsForBulkCopy(tableName);

            long partitionCount = 1;

            _logger.Debug($"Table {tableName}: RowCount={tableSize.RowCount}, SizeInGB={tableSize.SizeInGB}");

            switch(_config.LogicalPartitioningStrategy)
            {
                case LogicalPartitioningStrategy.Auto:                    
                    // One partition per GB
                    partitionCount = tableSize.SizeInGB;            

                    // If table is small in size but has a lot of rows
                    if (tableSize.SizeInGB < 1 && tableSize.RowCount > _config.BatchSize)
                    {
                        partitionCount = tableSize.RowCount / (_config.BatchSize * 10);
                    }
                                    
                    var maxPartitions = _config.MaxParallelTasks * 3;
                    if (partitionCount < 3) partitionCount = 3;
                    if (partitionCount > maxPartitions) partitionCount = maxPartitions;                    
                    break;
                case LogicalPartitioningStrategy.Size:
                    partitionCount = tableSize.SizeInGB / _config.LogicalPartitions;
                    break;
                case LogicalPartitioningStrategy.Count:
                    partitionCount = _config.LogicalPartitions;
                    break;
            } 

            if (partitionCount % 2 == 0) partitionCount += 1; // Make sure number is odd.

            var ps = (double)tableSize.SizeInGB / (double)partitionCount;
            var pc = (double)tableSize.RowCount / (double)partitionCount;
            _logger.Info($"Table {tableName} is not partitioned. Bulk copy will be parallelized using {partitionCount} logical partitions (Size: {ps:0.00} GB, Rows: {pc:0.00}).");            

            foreach (var n in Enumerable.Range(1, (int)partitionCount))
            {
                var cp = new LogicalPartitionCopyInfo();
                cp.PartitionNumber = n;
                cp.TableName = tableName;
                cp.LogicalPartitionsCount = (int)partitionCount;
                cp.Columns.AddRange(columns);

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
                        _logger.Info($"Task {taskId}: Bulk copying table {copyInfo.TableName}...");
                    else
                        _logger.Info($"Task {taskId}: Bulk copying table {copyInfo.TableName} partition {copyInfo.PartitionNumber}...");

                    _activeTasks.AddOrUpdate(taskId.ToString(), copyInfo.TableName, (_1, _2) => { return copyInfo.TableName; });
                    _logger.Debug($"Task {taskId}: Added to ActiveTasks");

                    var sourceConnection = new SqlConnection(_config.SourceConnectionString);
                    var whereClause = string.Empty;
                    var predicate = copyInfo.GetPredicate();                    
                    if (!string.IsNullOrEmpty(predicate))
                    {
                        whereClause = $" WHERE {predicate}";
                    };                    
                    var orderBy = copyInfo.GetOrderBy();
                    if (!string.IsNullOrEmpty(orderBy))
                    {
                        orderBy = $" ORDER BY {orderBy}";
                    };
                    var sql = $"SELECT {copyInfo.GetSelectList()} FROM {copyInfo.TableName}{whereClause}{orderBy}";

                    var options = SqlBulkCopyOptions.KeepIdentity |
                                SqlBulkCopyOptions.KeepNulls;         

                    // Tablock should be used only if using logical partitioning      
                    // and NO indexes are detected/used
                    if (copyInfo.ClusteredIndex.IndexColumns.Count == 0) 
                    {
                        options |= SqlBulkCopyOptions.TableLock;
                        _logger.Debug($"Task {taskId}: Using TABLOCK");
                    }

                    int attempts = 0;                    
                    int waitTime = attempts * _delay;

                    while (attempts < _maxAttempts)
                    {
                        attempts += 1;

                        if (attempts > 1)
                        {
                            _logger.Info($"Task {taskId}: Attempt {attempts} out of {_maxAttempts}.");

                            if (copyInfo is NoPartitionsCopyInfo)
                                _logger.Info($"Task {taskId}: Bulk copying table {copyInfo.TableName} (OrderBy: {copyInfo.GetOrderBy()})...");
                            else
                                _logger.Info($"Task {taskId}: Bulk copying table {copyInfo.TableName} partition {copyInfo.PartitionNumber} (OrderBy: {copyInfo.GetOrderBy()})...");
                        }

                        SqlConnection taskConn = null;
                        SqlTransaction taskTran = null;

                        try
                        {
                            _logger.Debug($"Task {taskId}: Executing: {sql}");      
                            sourceConnection.Open();                      
                            var sourceReader = sourceConnection.ExecuteReader(sql, commandTimeout: 0);

                            var sbc = new SqlConnectionStringBuilder(_config.DestinationConnectionString);
                            sbc.ApplicationName = $"smartbulkcopy{taskId}";
                            
                            // TODO
                            // Depending on if connecting to On-Prem/VM or Azure SQL
                            // ConnectionTimeout should be set automatically to 300 or 90

                            taskConn = new SqlConnection(sbc.ToString());                            
                            taskConn.Open();
                            taskTran = taskConn.BeginTransaction();

                            using (var bulkCopy = new SqlBulkCopy(taskConn, options, taskTran))
                            {
                                bulkCopy.BulkCopyTimeout = 0;                                
                                bulkCopy.DestinationTableName = copyInfo.TableName;
                                foreach (string c in copyInfo.Columns)
                                {
                                    bulkCopy.ColumnMappings.Add(c, c);
                                }
                                foreach (var ii in copyInfo.ClusteredIndex.IndexColumns) {
                                    bulkCopy.ColumnOrderHints.Add(ii.ColumnName, ii.IsDescending ? SortOrder.Descending : SortOrder.Ascending);
                                }                           
                                bulkCopy.BatchSize = _config.BatchSize;
                                bulkCopy.WriteToServer(sourceReader);
                                attempts = int.MaxValue;
                                taskTran.Commit();
                            }
                        }
                        catch (SqlException se)
                        {
                            if (_transientErrors.Contains(se.Number))
                            {
                                if (taskTran?.Connection != null)
                                    taskTran.Rollback();

                                waitTime = attempts * _delay;

                                _logger.Warn($"Task {taskId}: Transient error while copying data. Waiting {waitTime} seconds and then trying again...");
                                _logger.Warn($"Task {taskId}: [{se.Number}] {se.Message}");

                                Task.Delay(waitTime * 1000).Wait();
                            }
                            else
                            {
                                throw;
                            }
                        }
                        finally {
                            if (taskConn != null) 
                                taskConn.Close();
                            if (sourceConnection != null)
                                sourceConnection.Close();
                        }
                    }

                    if (attempts == int.MaxValue)
                    {
                        if (copyInfo is NoPartitionsCopyInfo)
                            _logger.Info($"Task {taskId}: Table {copyInfo.TableName} copied.");
                        else
                            _logger.Info($"Task {taskId}: Table {copyInfo.TableName}, partition {copyInfo.PartitionNumber} copied.");
                    }
                    else
                    {
                        _logger.Error($"Task {taskId}: Table {copyInfo.TableName} copy failed.");
                    }
                }

                _logger.Info($"Task {taskId}: No more items in queue, exiting.");
            }
            catch (Exception ex)
            {
                Interlocked.Add(ref _erroredTasks, 1);
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
                _logger.Debug($"Task {taskId}: Removed from ActiveTasks");
                Interlocked.Add(ref _runningTasks, -1);
            }
        }

        private void MonitorCopyProcess()
        {                                
            int attempts = 0;            
            int waitTime = attempts * _delay;                
                
            while (true)
            {
                try
                {       
                    attempts += 1;

                    var errors = Interlocked.Read(ref _erroredTasks);
                    if (errors != 0)
                    {
                        _logger.Warn("Monitor: Shutting down monitor due to unhandled errors in running tasks.");
                        break;
                    }
                    
                    if (attempts > _maxAttempts)
                    {
                        _logger.Warn("Monitor: Unable to connect query destination database. Terminating monitor.");
                        break;
                    }

                    if (attempts > 1)
                    {
                        _logger.Info($"Monitor: Attempt {attempts} out of {_maxAttempts}.");
                    }

                    // This needs to be in the loop 'cause instance name will change if database Service Level Objective is changed                    
                    var csb = new SqlConnectionStringBuilder(_config.DestinationConnectionString);
                    csb.ApplicationName = "smartbulkcopy_log_monitor";
                    csb.ConnectTimeout = ((attempts + 1) * _delay);
                    if (csb.ConnectTimeout < 90) csb.ConnectTimeout = 90;
                    var conn = new SqlConnection(csb.ToString());

                    var instance_name = (string)(conn.ExecuteScalar(@"
                        DECLARE @instanceName SYSNAME;
                        BEGIN TRY
                            EXECUTE sp_executesql N'SELECT TOP (1) @in=[physical_database_name] FROM sys.[databases] WHERE [database_id] = DB_ID();', N'@in SYSNAME OUTPUT', @in=@instanceName OUTPUT
                        END TRY
                        BEGIN CATCH
                            SET @instanceName = DB_NAME(DB_ID())
                        END CATCH   
                        SELECT
                            instance_name 
                        FROM
                            sys.dm_os_performance_counters 
                        WHERE 
                            counter_name = 'Log Bytes Flushed/sec' AND instance_name = @instanceName
                    "));

                    string query = $@"
                        declare @v1 bigint, @v2 bigint
                        select @v1 = cntr_value from sys.dm_os_performance_counters 
                        where counter_name = 'Log Bytes Flushed/sec' and instance_name = '{instance_name}';
                        waitfor delay '00:00:05';
                        select @v2 = cntr_value from sys.dm_os_performance_counters 
                        where counter_name = 'Log Bytes Flushed/sec' and instance_name = '{instance_name}';
                        select log_flush_mb_sec =  ((@v2-@v1) / 5.) / 1024. / 1024.
                    ";

                    var runningTasks = Interlocked.Read(ref _runningTasks);
                    if (_queue.Count == 0 && runningTasks == 0) break;

                    var log_flush = Convert.ToDecimal(conn.ExecuteScalar(query) ?? 0);
                    var copyingTables = String.Join(',', _activeTasks.Values.Distinct().ToArray());
                    if (copyingTables == "") copyingTables = "None";
                    _logger.Info($"Log Flush Speed: {log_flush:00.00} MB/Sec, {runningTasks} Running Tasks, Queue Size {_queue.Count}, Tables being copied: {copyingTables}.");     

                    attempts = 0;           
                } 
                catch (SqlException se)
                {
                    if (_transientErrors.Contains(se.Number))
                    {                                    
                        waitTime = attempts * _delay;

                        _logger.Warn($"Monitor: Transient error while copying data. Waiting {waitTime} seconds and then trying again...");
                        _logger.Warn($"Monitor: [{se.Number}] {se.Message}");

                        Task.Delay(waitTime * 1000).Wait();
                    } else {
                        _logger.Error($"Monitor: [{se.Number}] {se.Message}");
                        throw;
                    }
                }           
                catch (Exception e)
                {
                    _logger.Error($"Monitor: {e.Message}");
                    throw;
                }                 
            }
        }

        async Task<bool> TestConnection(string connectionString)
        {
            var builder = new SqlConnectionStringBuilder(connectionString);

            _logger.Info($"Testing connection to: {builder.DataSource}, database {builder.InitialCatalog}...");

            var conn = new SqlConnection(connectionString);
            bool result = false;

            try
            {
                await conn.OpenAsync();
                result = true;
                _logger.Info($"Connection to {builder.DataSource} succeeded.");

                var sku = conn.ExecuteScalar<string>(@"
                    BEGIN TRY
	                    EXEC('SELECT [service_objective] FROM sys.[database_service_objectives]')
                    END TRY
                    BEGIN CATCH
	                    SELECT 'None' AS [service_objective]
                    END CATCH   
                    ");
                if (sku != "None")
                {
                    _logger.Info($"Database {builder.DataSource}/{builder.InitialCatalog} is a {sku}.");
                }
                else
                {
                    _logger.Info($"Database {builder.DataSource}/{builder.InitialCatalog} is a VM/On-Prem.");
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, $"Error while opening connection on '{builder.DataSource}'.");
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

        private List<String> GetTablesToCopy(IEnumerable<String> sourceList)
        {
            var conn = new SqlConnection(_config.SourceConnectionString);

            var internalTablesToCopy = new List<String>();
            foreach (var t in sourceList)
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
                        and
	                        t.[name] != 'sysdiagrams'
                    ");
                    var regExPattern = t.Replace(".", "[.]").Replace("*", ".*");
                    foreach (var tb in tables)
                    {
                        bool matches = Regex.IsMatch(tb.Name.Replace("[", "").Replace("]", ""), regExPattern); // TODO: Improve wildcard matching
                        if (matches)
                        {
                            _logger.Info($"Adding {tb.Name}...");
                            internalTablesToCopy.Add(tb.Name);
                        }
                    }
                }
                else
                {
                    _logger.Info($"Adding {t}...");
                    internalTablesToCopy.Add(t);
                }
            }

            return internalTablesToCopy;
        }
    
        private class TableSize {
            public long RowCount;
            public int SizeInGB;
        }       
    }    
}