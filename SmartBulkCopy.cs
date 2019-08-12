using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using NLog;

namespace HSBulkCopy
{
    enum PartitionType
    {
        Physical,
        Logical
    }

    abstract class CopyInfo
    {
        public string TableName;
        public int PartitionNumber;
        public abstract string GetPredicate();
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
            return $"ABS(CAST(%%PhysLoc%% AS BIGINT)) % {LogicalPartitionsCount} = {PartitionNumber - 1}";
        }
    }

    class SmartBulkCopyConfiguration 
    {
        public readonly string SourceConnectionString;
        
        public readonly string DestinationConnectionString;                       

        public SmartBulkCopyConfiguration(string sourceConnectionString, string destinationConnectionString)
        {
            this.SourceConnectionString = sourceConnectionString;
            this.DestinationConnectionString = destinationConnectionString;            
        }
    }

    class SmartBulkCopy
    {
        private readonly ILogger _logger;
        private readonly SmartBulkCopyConfiguration _config;
        private readonly Stopwatch _stopwatch = new Stopwatch();
        private readonly ConcurrentQueue<CopyInfo> _queue = new ConcurrentQueue<CopyInfo>();        
        private int _maxTasks = 7;
        private int _logicalPartitionCount = 7;

        public SmartBulkCopy(SmartBulkCopyConfiguration config, ILogger logger)
        {
            _logger = logger;
            _config =  config;            
        }

        public async Task<int> Copy()
        {
           var tableList = new List<string>();
           
           return await Copy(tableList);
        }
     
        public async Task<int> Copy(List<String> tablesToCopy)
        {
            _logger.Info("Starting smart bulk copy process...");

            _logger.Info("Testing connections...");

            var t1 = TestConnection(_config.SourceConnectionString);
            var t2 = TestConnection(_config.DestinationConnectionString);

            await Task.WhenAll(t1, t2);
        
            if (await t1 != true || await t2 != true) return 1;
            
            var conn = new SqlConnection(_config.SourceConnectionString);
            var tasks = new List<Task>();

            var internalTablesToCopy = new List<String>();
            internalTablesToCopy.AddRange(tablesToCopy.Distinct());
                                    
            if (internalTablesToCopy.Contains("*")) {                                
                _logger.Info("Getting list of tables to copy...");
                internalTablesToCopy.Remove("*");
                var tables = conn.Query("select [Name] = QUOTENAME(s.[name]) + '.' + QUOTENAME(t.[name]) from sys.tables t inner join sys.schemas s on t.[schema_id] = s.[schema_id]");
                foreach(var t in tables)
                {   
                    _logger.Info($"Adding {t.Name}");
                    internalTablesToCopy.Add(t.Name);                    
                }                
            }

            _logger.Info("Analyzing tables...");
            var copyInfo = new List<CopyInfo>();
            foreach (var t in internalTablesToCopy)
            {
                // TODO: Check it table exists

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

            _logger.Info("Enqueing work...");
            copyInfo.ForEach(ci => _queue.Enqueue(ci));
            _logger.Info($"{_queue.Count} items enqueued.");

            _logger.Info("Truncating destination tables...");
            internalTablesToCopy.ForEach(t => TruncateDestinationTable(t));
            
            _logger.Info($"Copying using {_maxTasks} parallel tasks.");
            foreach (var i in Enumerable.Range(1, _maxTasks))
            {
                tasks.Add(new Task(() => BulkCopy(i)));
            }
            _logger.Info($"Starting monitor...");
            var monitorTask = Task.Run(() => MonitorLogFlush());

            _logger.Info($"Start copying...");
            _stopwatch.Start();
            tasks.ForEach(t => t.Start());
            await Task.WhenAll(tasks.ToArray());
            _stopwatch.Stop();
            _logger.Info($"Done copying.");

            _logger.Info($"Waiting for monitor to shut down...");
            monitorTask.Wait();

            _logger.Info("Done in {0:#.00} secs", (double)_stopwatch.ElapsedMilliseconds / 1000.0);

            return 0;
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
                        [object_id] = object_id('{tableName}') 
                    and 
                        index_id in (0,1)
                    ");

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
                        [object_id] = object_id('{tableName}') 
                    and
                        index_id in (0,1)
                    ";

            _logger.Debug($"Executing: {sql1}");

            var partitionCount = (int)conn.ExecuteScalar(sql1);

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
                    i.[object_id] = object_id('{tableName}') 
                and 
                    i.index_id in (0,1)
                and
                    ic.partition_ordinal = 1
                ";

            var partitionInfo = conn.QuerySingle(sql2);

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
            _logger.Info($"Table {tableName} is NOT partitioned. Bulk copy will be parallelized using {_logicalPartitionCount} logical partitions.");

            var copyInfo = new List<CopyInfo>();

            foreach (var n in Enumerable.Range(1, _logicalPartitionCount))
            {
                var cp = new LogicalPartitionCopyInfo();
                cp.PartitionNumber = n;
                cp.TableName = tableName;
                cp.LogicalPartitionsCount = _logicalPartitionCount;

                copyInfo.Add(cp);
            }

            return copyInfo;

        }

        private void BulkCopy(int taskId)
        {
            CopyInfo copyInfo;
            _logger.Info($"Task {taskId}: Started...");

            while (_queue.TryDequeue(out copyInfo))
            {
                _logger.Info($"Task {taskId}: Processing table {copyInfo.TableName} partition {copyInfo.PartitionNumber}...");
                var sourceConnection = new SqlConnection(_config.SourceConnectionString);
                var sql = $"SELECT * FROM {copyInfo.TableName} WHERE {copyInfo.GetPredicate()}";
                _logger.Debug($"Task {taskId}: Executing: {sql}");
                var sourceReader = sourceConnection.ExecuteReader(sql);

                using (var bulkCopy = new SqlBulkCopy(_config.DestinationConnectionString + $";Application Name=hsbulkcopy{taskId}", SqlBulkCopyOptions.TableLock))
                {
                    bulkCopy.BulkCopyTimeout = 0;
                    bulkCopy.BatchSize = 100000;
                    bulkCopy.DestinationTableName = copyInfo.TableName;

                    try
                    {
                        bulkCopy.WriteToServer(sourceReader);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        var ie = ex.InnerException;
                        while (ie != null)
                        {
                            Console.WriteLine(ex.Message);
                            ie = ie.InnerException;
                        }
                    }
                    finally
                    {
                        sourceReader.Close();                        
                    }
                }
                _logger.Info($"Task {taskId}: Table {copyInfo.TableName}, partition {copyInfo.PartitionNumber} copied.");
            }

            _logger.Info($"Task {taskId}: Done.");
        }
    
        private void MonitorLogFlush()
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
                var log_flush = (decimal)(conn.ExecuteScalar(query));
                _logger.Info($"Log Flush Speed: {log_flush:00.00} MB/Sec");

                Task.Delay(5000);

                if (_queue.Count == 0) break;
            }
        }

        async Task<bool> TestConnection(string connectionString)
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
           
            _logger.Debug($"Testing connection to: {builder.DataSource}...");

            var conn = new SqlConnection(connectionString);
            bool result = false;

            try {
                await conn.OpenAsync();
                result = true;
                _logger.Debug($"Connection to {builder.DataSource} succeeded.");
            } 
            catch (Exception ex)
            {
                _logger.Info(ex, "Error while opening connection.");
            } finally {
                conn.Close();
            }
            
            return result;    
        }
    }
}