using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Runtime.Loader;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Types;
using Dapper;
using NLog;

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
        // Added Error Code 64 to handle transient network errors
        // Added Error Code 121 to handle transient network errors
        // Added Error Code 258 to handle transient login errors

        private readonly List<int> _transientErrors = new List<int>() { 0, 53, 64, 121, 258, 4891, 10054, 4060, 40197, 40501, 40613, 49918, 49919, 49920, 10054, 11001, 10065, 10060, 10051 };
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
        private CancellationTokenSource ctsMonitor = new CancellationTokenSource();
        private CancellationTokenSource ctsCopy = new CancellationTokenSource();

        private Assembly OnAssemblyResolve(AssemblyLoadContext assemblyLoadContext, AssemblyName assemblyName)
        {
            // SqlServer.Types assembly redirection            
            if (assemblyName.FullName.Contains("Microsoft.SqlServer.Types") && assemblyName.FullName.Contains("PublicKeyToken=89845dcd8080cc91"))
            {
                var a = typeof(SqlHierarchyId).Assembly;
                _logger.Debug($"Rebinding {assemblyName.FullName} to {a.FullName}");
                _logger.Warn($"Detected usage of SqlServer Data Types. Using custom assembly '{a.FullName}' to allow Bulk Load to work.");
                return a;
            }

            return null;
        }

        public SmartBulkCopy(SmartBulkCopyConfiguration config, ILogger logger)
        {
            _logger = logger;
            _config = config;

            AssemblyLoadContext.Default.Resolving += OnAssemblyResolve;
        }

        public async Task<int> Copy()
        {
            return await Copy(_config.TablesToCopy);
        }

        public async Task<int> Copy(List<String> tablesToCopy)
        {
            _logger.Info("Starting smart bulk copy process...");

            _logger.Info($"Setting CommandTimeOut to: {_config.CommandTimeOut} secs");
            Dapper.SqlMapper.Settings.CommandTimeout = _config.CommandTimeOut;

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

            _logger.Info("Bulding list of tables to be copied...");
            var internalTablesToCopy = GetTablesToCopy(tablesToCopy.Distinct());
            _tablesToCopy.AddRange(internalTablesToCopy);
            _logger.Info("...done");

            _logger.Info("Gathering tables info...");
            var ticSource = new TablesInfoCollector(_config.SourceConnectionString, internalTablesToCopy, _logger);
            var ticDestination = new TablesInfoCollector(_config.DestinationConnectionString, internalTablesToCopy, _logger);

            var ti1 = ticSource.CollectTablesInfoAsync();
            var ti2 = ticDestination.CollectTablesInfoAsync();

            await Task.WhenAll(ti1, ti2);

            var tiSource = await ti1;
            var tiDestination = await ti2;
            _logger.Info("...done");

            _logger.Info("Analyzing tables...");
            var ta = new TableAnalyzer(_config, _logger);
            var tar = ta.Analyze(internalTablesToCopy, tiSource, tiDestination);
            if (tar.Outcome != AnalysisOutcome.Success)
            {
                return (int)(tar.Outcome);
            }
            var copyInfo = tar.CopyInfo;
            _logger.Info("...done");

            _logger.Info("Enqueueing work...");
            copyInfo.ForEach(ci => _queue.Enqueue(ci));
            _logger.Info($"{_queue.Count} items enqueued.");

            if (_config.TruncateTables)
            {
                _logger.Info("Disabling system versioned tables, if any...");
                internalTablesToCopy.ForEach(t => DisableSystemVersioning(tiDestination.Find(t2 => t2.TableName == t)));

                _logger.Info("Truncating destination tables...");
                internalTablesToCopy.ForEach(t => TruncateDestinationTable(t));
            }

            _logger.Info($"Copying using up to {_config.MaxParallelTasks} parallel tasks.");
            var tasks = new List<Task>();
            var taskCount = _config.MaxParallelTasks;
            if (taskCount > copyInfo.Count()) taskCount = copyInfo.Count();
            foreach (var i in Enumerable.Range(1, taskCount))
            {
                tasks.Add(new Task(() => BulkCopy(i, ctsCopy.Token)));
            }

            _logger.Info($"Starting monitor...");
            Console.CancelKeyPress += new ConsoleCancelEventHandler(this.ConsoleCancelHandler);
            var monitorTask = Task.Run(() => MonitorCopyProcess(ctsMonitor.Token));

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
            }
            else
            {
                ctsMonitor.Cancel();
                monitorTask.Wait();
            }

            if (_config.TruncateTables)
            {
                _logger.Info("Re-Enabling system versioned tables, if any...");
                internalTablesToCopy.ForEach(t => EnableSystemVersioning(tiDestination.Find(t2 => t2.TableName == t)));
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

                    if (_config.SyncIdentity)
                    {
                        _logger.Info("Synchronizing identity values...");
                        bool identitySynced = await SyncIdentity();           
                        if (!identitySynced)
                        {       
                            _logger.Warn("WARNING! Identity synchronization encountered some errors!");
                            result = 2;  
                        } else 
                        {
                            _logger.Info("Identity synchronization done.");             
                        }                        
                    }
                }

                _logger.Info("Smart Bulk Copy completed in {0:#.00} secs.", (double)_stopwatch.ElapsedMilliseconds / 1000.0);
            }
            else
            {
                _logger.Warn("Completed with errors.");
                result = 3;
            }

            return result;
        }

        private void DisableSystemVersioning(TableInfo tableInfo)
        {
            if (tableInfo.Type == TableType.SystemVersionedTemporal)
            {
                var tableName = tableInfo.TableName;
                _logger.Info($"Disabling system versioning on '{tableName}'...");
                var dc = new SqlConnection(_config.DestinationConnectionString);

                dc.ExecuteScalar($"alter table {tableName} set (system_versioning = off)");
                dc.ExecuteScalar($"alter table {tableName} drop period for system_time");
            }
        }

        private void EnableSystemVersioning(TableInfo tableInfo)
        {
            if (tableInfo.Type == TableType.SystemVersionedTemporal)
            {
                var tableName = tableInfo.TableName;
                _logger.Info($"Re-Enabling system versioning on '{tableName}'...");
                var dc = new SqlConnection(_config.DestinationConnectionString);

                dc.ExecuteScalar($"alter table {tableName} add period for system_time ({tableInfo.HistoryInfo.PeriodStartColumn}, {tableInfo.HistoryInfo.PeriodEndColumn})");
                dc.ExecuteScalar($"alter table {tableName} set (system_versioning = on (history_table = {tableInfo.HistoryInfo.HistoryTable}, history_retention_period = {tableInfo.HistoryInfo.RetentionPeriod}))");
            }
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
            var isReadOnly = conn.ExecuteScalar<int>("select [is_read_only] from sys.databases where [database_id] = DB_ID()");
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

        private async Task<bool> SyncIdentity()
        {
            var connSource = new SqlConnection(_config.SourceConnectionString);
            var connDest = new SqlConnection(_config.DestinationConnectionString);
            bool result = true;

            string sqlCheck = @"
                with cte as
                (
                    select 	
                        objectproperty([object_id], 'TableHasIdentity') as TableHasIdentity,
                        ident_current(schema_name([schema_id]) + '.' + object_name([object_id])) as IdentityCurrent
                    from
                        sys.[tables]
                    where 
                    object_id = object_id(@tableName) 
                )
                select
                    IdentityCurrent
                from
                    cte
                where
                    TableHasIdentity = 1
                ";

            foreach (var t in _tablesToCopy)
            {
                _logger.Debug($"Executing: {sqlCheck}, @tableName = {t}");
                var ic = await connSource.ExecuteScalarAsync<int?>(sqlCheck, new { @tableName = t });
                if (ic.HasValue)
                {
                    string sqlSet = $"dbcc checkident('{t}', reseed, {ic.Value})";
                    _logger.Debug($"Executing: {sqlSet}, @tableName = {t}");
                    connDest.Execute(sqlSet);
                    var ic2 = await connDest.ExecuteScalarAsync<int?>(sqlCheck, new { @tableName = t });
                    if (ic.Value == ic2.Value) 
                    {
                        _logger.Info($"Identity for table {t} set to {ic.Value}");
                    }                        
                    else
                    {
                        _logger.Error($"Unable to sync identity value for {t} to {ic.Value}. Identity value found is {ic2.Value}.");
                        result = false;
                    }                            
                }
            }

            return result;
        }

        private void TruncateDestinationTable(string tableName)
        {
            _logger.Info($"Truncating '{tableName}'...");
            var dc = new SqlConnection(_config.DestinationConnectionString);
            dc.ExecuteScalar($"truncate table {tableName}");
        }

        private void BulkCopy(int taskId, CancellationToken ct)
        {
            CopyInfo copyInfo;
            _logger.Info($"Task {taskId}: Started...");

            Interlocked.Add(ref _runningTasks, 1);

            try
            {
                while (_queue.TryDequeue(out copyInfo))
                {
                    var bulkLoadMessage = $"Task {taskId}: Bulk copying table {copyInfo.TableName}";

                    if (!(copyInfo is NoPartitionsCopyInfo))
                        bulkLoadMessage += $" partition {copyInfo.PartitionNumber}";

                    if ((copyInfo.GetOrderBy().Trim() != string.Empty) && (copyInfo.OrderHintType != OrderHintType.None))
                        bulkLoadMessage += $" (OrderBy: {copyInfo.GetOrderBy()})";

                    _logger.Info(bulkLoadMessage + "...");

                    _activeTasks.AddOrUpdate(taskId.ToString(), copyInfo.TableName, (_1, _2) => { return copyInfo.TableName; });
                    _logger.Debug($"Task {taskId}: Added to ActiveTasks");

                    var sourceConnection = new SqlConnection(_config.SourceConnectionString);
                    var whereClause = string.Empty;
                    var predicate = copyInfo.GetPredicate();
                    if (!string.IsNullOrEmpty(predicate))
                    {
                        whereClause = $" WHERE {predicate}";
                    };
                    var orderBy = "";
                    if (copyInfo.OrderHintType != OrderHintType.None)
                    {
                        orderBy = copyInfo.GetOrderBy();
                        if (!string.IsNullOrEmpty(orderBy))
                        {
                            orderBy = $" ORDER BY {orderBy}";
                        };
                    }
                    var sql = $"SELECT {copyInfo.GetSelectList()} FROM {copyInfo.TableName}{whereClause}{orderBy}";

                    var options = SqlBulkCopyOptions.KeepIdentity |
                                SqlBulkCopyOptions.KeepNulls;

                    // TABLOCK can be used only if target is HEAP
                    if (copyInfo.DestinationTableInfo.PrimaryIndex is Heap)
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
                            _logger.Info(bulkLoadMessage);
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

                                if (copyInfo.OrderHintType == OrderHintType.ClusteredIndex)
                                {
                                    _logger.Debug($"Task {taskId}: Adding OrderHints ({copyInfo.OrderHintType}).");
                                    var oc = copyInfo.SourceTableInfo.PrimaryIndex.GetOrderBy();
                                    foreach (var ii in oc)
                                    {
                                        bulkCopy.ColumnOrderHints.Add(ii.ColumnName, ii.IsDescending ? SortOrder.Descending : SortOrder.Ascending);
                                    }
                                }
                                if (copyInfo.OrderHintType == OrderHintType.PartionKeyOnly)
                                {
                                    _logger.Debug($"Task {taskId}: Adding OrderHints ({copyInfo.OrderHintType}).");
                                    var oc = copyInfo.SourceTableInfo.PrimaryIndex.GetPartitionBy();
                                    foreach (var ii in oc)
                                    {
                                        bulkCopy.ColumnOrderHints.Add(ii.ColumnName, ii.IsDescending ? SortOrder.Descending : SortOrder.Ascending);
                                    }
                                }

                                if (copyInfo.DestinationTableInfo.PrimaryIndex is ColumnStoreClusteredIndex)
                                {
                                    // Make sure Columnstore will have as few rowgroups as possible
                                    if (bulkCopy.BatchSize < 102400) bulkCopy.BatchSize = 102400;
                                    _logger.Debug($"Task {taskId}: Forcing BatchSize to 102400");
                                }
                                else
                                {
                                    bulkCopy.BatchSize = _config.BatchSize;
                                }

                                if (ct.IsCancellationRequested)
                                    ct.ThrowIfCancellationRequested();

                                Task innerTask = null;
                                try
                                {
                                    innerTask = bulkCopy.WriteToServerAsync(sourceReader, ct);
                                    innerTask.Wait();
                                }
                                catch (Exception ex)
                                {
                                    if (innerTask != null)
                                    {
                                        var ine = innerTask.Exception?.InnerException ?? ex;
                                        if (ine is AggregateException) ine = (ine as AggregateException).Flatten();
                                        while (ine != null)
                                        {
                                            // If a SqlException or InvalidOperationException is found in the AggregateExceptions, 
                                            // throw it so that automatic retry can kick-in
                                            if (ine is SqlException) throw (ine as SqlException);
                                            if (ine is InvalidOperationException) throw (ine as InvalidOperationException);
                                            if (ine is TaskCanceledException) throw (ine as TaskCanceledException);
                                            if (!(ine is AggregateException)) _logger.Error($"Task {taskId}@WriteToServerAsync: [{ine.GetType()}] {ine.Message}");
                                            ine = ine.InnerException;
                                        }
                                        throw ex;
                                    }
                                }

                                _logger.Info($"Task {taskId}: Committing changes to {copyInfo.TableName}...");
                                attempts = int.MaxValue;
                                taskTran.Commit();
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            attempts = int.MaxValue;
                            throw;
                        }
                        catch (InvalidOperationException ioe)
                        {
                            waitTime = attempts * _delay;

                            _logger.Warn($"Task {taskId}@Transaction: Transient error while copying data. Waiting {waitTime} seconds and then trying again...");
                            _logger.Warn($"Task {taskId}@Transaction: [InvalidOperationException] {ioe.Message}");

                            if (taskTran?.Connection != null)
                                taskTran.Rollback();

                            Task.Delay(waitTime * 1000).Wait();
                        }
                        catch (SqlException se)
                        {
                            if (_transientErrors.Contains(se.Number))
                            {
                                waitTime = attempts * _delay;

                                _logger.Warn($"Task {taskId}@Transaction: Transient error while copying data. Waiting {waitTime} seconds and then trying again...");
                                _logger.Warn($"Task {taskId}@Transaction: [SqlException:{se.Number}] {se.Message}");

                                if (taskTran?.Connection != null)
                                    taskTran.Rollback();

                                Task.Delay(waitTime * 1000).Wait();
                            }
                            else
                            {
                                throw se;
                            }
                        }
                        finally
                        {
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
                bool canceled = false;
                Interlocked.Add(ref _erroredTasks, 1);
                var ie = ex;
                if (ie is AggregateException) ie = (ie as AggregateException).Flatten();
                while (ie != null)
                {
                    if (ie is TaskCanceledException) canceled = true;
                    if (ie is OperationCanceledException) canceled = true;
                    var se = ie as SqlException;
                    if (se != null)
                    {
                        _logger.Error($"Task {taskId}: [{se.Number}] {se.Message}");
                    }
                    else
                    {
                        if (!(ie is TaskCanceledException) && !(ie is OperationCanceledException))
                            _logger.Error($"Task {taskId}: [{ex.GetType()}] {ex.Message}");
                    }
                    ie = ie.InnerException;
                }

                if (!canceled)
                {
                    _logger.Error($"Task {taskId}: Completed with errors.");
                }
                else
                {
                    _logger.Warn($"Task {taskId}: Execution has been canceled.");
                }
            }
            finally
            {
                string dummy = string.Empty;
                _activeTasks.Remove(taskId.ToString(), out dummy);
                _logger.Debug($"Task {taskId}: Removed from ActiveTasks");
                Interlocked.Add(ref _runningTasks, -1);
            }
        }

        private void MonitorCopyProcess(CancellationToken ct)
        {
            int attempts = 0;
            int waitTime = attempts * _delay;

            while (true)
            {
                try
                {
                    if (ct.IsCancellationRequested)
                    {
                        _logger.Warn("Monitor: Shutting down monitor due to cancellation request.");
                        break;
                    }

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
                    var copyingTables = String.Join(",", _activeTasks.Values.Distinct().ToArray());
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
                    }
                    else
                    {
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

        private List<String> GetTablesToCopy(IEnumerable<String> sourceList)
        {
            var conn = new SqlConnection(_config.SourceConnectionString);

            var internalTablesToCopy = new List<String>();
            foreach (var s in sourceList)
            {
                var mode = "+";
                var t = s;

                // Handle inclusion and exclusion prefixes
                if (s.Substring(0, 2) == "+:" || s.Substring(0, 2) == "-:")
                {
                    mode = s.Substring(0, 1);
                    t = s.Substring(2);
                }

                if (t.Contains("*"))
                {
                    _logger.Debug($"Wildcard found: '{t}'. Getting list of tables to copy...");
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
                            t.is_external = 0
                        and
	                        t.[name] != 'sysdiagrams'
                    ");
                    var regExPattern = t.Replace(".", "[.]").Replace("*", ".*");
                    foreach (var tb in tables)
                    {
                        bool matches = Regex.IsMatch(tb.Name.Replace("[", "").Replace("]", ""), regExPattern); // TODO: Improve wildcard matching
                        if (matches)
                        {
                            if (mode == "+")
                            {
                                _logger.Debug($"Including via wildcard {tb.Name}...");
                                internalTablesToCopy.Add(tb.Name);
                            }
                            else
                            {
                                _logger.Debug($"Excluding via wildcard {tb.Name}...");
                                internalTablesToCopy.Remove(tb.Name);
                            }
                        }
                    }
                }
                else
                {
                    var parts = t.Split('.');
                    var qt = string.Join(".", parts.Select(p =>
                    {
                        string n = "";
                        if (!p.StartsWith("[")) n += "[";
                        n += p;
                        if (!p.EndsWith("]")) n += "]";
                        return n;
                    }).ToArray());

                    if (mode == "+")
                    {
                        _logger.Debug($"Including {qt}...");
                        internalTablesToCopy.Add(qt);
                    }
                    else
                    {
                        _logger.Debug($"Excluding {qt}...");
                        internalTablesToCopy.Remove(qt);
                    }
                }
            }

            internalTablesToCopy.ForEach(t => _logger.Info($"Queueing table {t}..."));

            return internalTablesToCopy;
        }

        private void ConsoleCancelHandler(object sender, ConsoleCancelEventArgs args)
        {
            if (!ctsCopy.IsCancellationRequested)
            {
                _logger.Warn("Cancelling Smart Bulk Copy execution. Asking tasks to cancel (this may take some minutes, please be patient)...");
                _logger.Warn("(CTRL+C again to terminate the process abruptly)");
                ctsCopy.Cancel();
                ctsMonitor.Cancel();
                args.Cancel = true;
            }
            else
            {
                _logger.Warn("WARN: Terminating process immediately.");
                _logger.Warn("WARN: Destination database may be left in an inconsistent state.");
            }
        }
    }
}