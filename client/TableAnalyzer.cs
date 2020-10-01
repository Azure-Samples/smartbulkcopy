using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using NLog;
using System.Data;
using Microsoft.Data.SqlClient;
using Dapper;

namespace SmartBulkCopy
{
    public enum AnalysisOutcome
    {
        Success = 0,
        AnalysisNotCompleted,
        SourceTableDoNotExist,
        DestinationTableDoNotExists,
        SecondaryIndexFoundOnDestination,
        ForeignKeysFoundOnDestination,
        DestinationIsTemporalTable
    }

    public class AnalysisResult
    {
        public List<CopyInfo> CopyInfo = new List<CopyInfo>();
        public AnalysisOutcome Outcome = AnalysisOutcome.AnalysisNotCompleted;

    }

    public class TableAnalyzer
    {
        private readonly ILogger _logger;
        private readonly SmartBulkCopyConfiguration _config;

        public TableAnalyzer(SmartBulkCopyConfiguration config, ILogger logger)
        {
            this._config = config;
            this._logger = logger;
        }

        public AnalysisResult Analyze(List<String> tablesToCopy, List<TableInfo> tiSource, List<TableInfo> tiDestination)
        {
            var result = new AnalysisResult();
            
            foreach (var t in tablesToCopy)
            {
                bool usePartitioning = false;
                OrderHintType orderHintType = OrderHintType.None;

                var sourceTable = tiSource.Find(p => p.TableName == t);
                var destinationTable = tiDestination.Find(p => p.TableName == t);

                // Check it tables exists
                if (sourceTable.Exists == false)
                {
                    _logger.Error($"Table {t} does not exists on source.");
                    result.Outcome = AnalysisOutcome.SourceTableDoNotExist;
                    return result;
                }
                if (destinationTable.Exists == false)
                {
                    _logger.Error($"Table {t} does not exists on destination.");
                    result.Outcome = AnalysisOutcome.DestinationTableDoNotExists;
                    return result;
                }

                // Check for Secondary Indexes
                if (destinationTable.SecondaryIndexes > 0)
                {
                    _logger.Info($"{t} destination table has {destinationTable.SecondaryIndexes} Secondary Indexes...");
                    if (_config.StopIf.HasFlag(StopIf.SecondaryIndex))
                    {
                        _logger.Error("Stopping as Secondary Indexes have been detected on destination table.");
                        result.Outcome = AnalysisOutcome.SecondaryIndexFoundOnDestination;
                        return result;
                    }
                    else
                    {
                        _logger.Warn($"WARNING! Secondary Indexes detected on {t}. Data transfer performance will be severely affected!");
                    }
                }

                // Check for Foreign Keys
                if (destinationTable.ForeignKeys > 0)
                {
                    _logger.Error($"{t} destination table has {destinationTable.ForeignKeys} Foreign Keys...");
                    _logger.Error("Stopping as Foreign Keys must be dropped in the destination table in order for Smart Bulk Copy to work.");
                    result.Outcome = AnalysisOutcome.ForeignKeysFoundOnDestination;
                    return result;
                }

                // Check if dealing with a Temporal Table
                if (destinationTable.Type != TableType.Regular)
                {
                    _logger.Info($"{t} destination table is {destinationTable.Type}...");
                    if (_config.StopIf.HasFlag(StopIf.TemporalTable))
                    {
                        _logger.Error($"Stopping a destination table {t} is a Temporal Table. Disable Temporal Tables on the destination.");
                        result.Outcome = AnalysisOutcome.DestinationIsTemporalTable;
                        return result;
                    }
                    else
                    {
                        _logger.Warn($"WARNING! Destination table {t} is a Temporal Table. System Versioning will be automatically disabled and re-enabled to allow bulk load.");
                        _logger.Debug($"Temporal Table {t} uses {destinationTable.HistoryInfo.HistoryTable} for History. Period is {destinationTable.HistoryInfo.PeriodStartColumn} to {destinationTable.HistoryInfo.PeriodEndColumn}. Retention is {destinationTable.HistoryInfo.RetentionPeriod}");
                    }
                }

                // Check if partitioned load is possible
                if (sourceTable.PrimaryIndex.IsPartitioned && destinationTable.PrimaryIndex is Heap)
                {
                    _logger.Info($"{t} -> Source is partitioned and destination is heap. Parallel load available.");
                    _logger.Info($"{t} -> Partition By: {sourceTable.PrimaryIndex.GetPartitionByString()}");
                    usePartitioning = true;
                }
                else if (sourceTable.PrimaryIndex is Heap && destinationTable.PrimaryIndex is Heap)
                {
                    _logger.Info($"{t} -> Source and destination are not partitioned and both are heaps. Parallel load available.");
                    usePartitioning = true;
                }
                else if (sourceTable.PrimaryIndex.IsPartitioned == false && destinationTable.PrimaryIndex is Heap)
                {
                    _logger.Info($"{t} -> Source is not partitioned but destination is an heap. Parallel load available.");
                    usePartitioning = true;
                }
                else if (
                      (sourceTable.PrimaryIndex.IsPartitioned && destinationTable.PrimaryIndex.IsPartitioned) &&
                      (sourceTable.PrimaryIndex.GetPartitionByString() == destinationTable.PrimaryIndex.GetPartitionByString()) &&
                      (sourceTable.PrimaryIndex.GetOrderByString() == destinationTable.PrimaryIndex.GetOrderByString())
                  )
                {
                    _logger.Info($"{t} -> Source and destination tables have compatible partitioning logic. Parallel load available.");
                    _logger.Info($"{t} -> Partition By: {sourceTable.PrimaryIndex.GetPartitionByString()}");
                    if (sourceTable.PrimaryIndex.GetOrderByString() != string.Empty) _logger.Info($"{t} -> Order By: {sourceTable.PrimaryIndex.GetOrderByString()}");
                    usePartitioning = true;
                }
                else if (destinationTable.PrimaryIndex is ColumnStoreClusteredIndex)
                {
                    _logger.Info($"{t} -> Destination is a ColumnStore. Parallel load available.");
                    usePartitioning = true;
                }
                else
                {
                    _logger.Info($"{t} -> Source and destination tables do not support parallel loading.");
                    usePartitioning = false;
                }

                // Check if ORDER hint can be used to avoid sorting data on the destination
                if (sourceTable.PrimaryIndex is RowStoreClusteredIndex && destinationTable.PrimaryIndex is RowStoreClusteredIndex)
                {
                    if (sourceTable.PrimaryIndex.GetOrderByString() == destinationTable.PrimaryIndex.GetOrderByString())
                    {
                        _logger.Info($"{t} -> Source and destination clustered rowstore index have same ordering. Enabling ORDER hint.");
                        orderHintType = OrderHintType.ClusteredIndex;

                    }
                }
                if (sourceTable.PrimaryIndex is Heap && destinationTable.PrimaryIndex is Heap)
                {
                    if (sourceTable.PrimaryIndex.IsPartitioned && destinationTable.PrimaryIndex.IsPartitioned)
                    {
                        _logger.Info($"{t} -> Source and destination are partitioned but not RowStores. Enabling ORDER hint on partition column.");
                        orderHintType = OrderHintType.PartionKeyOnly;
                    }
                }
                if (sourceTable.PrimaryIndex is ColumnStoreClusteredIndex && destinationTable.PrimaryIndex is ColumnStoreClusteredIndex)
                {
                    if (sourceTable.PrimaryIndex.IsPartitioned && destinationTable.PrimaryIndex.IsPartitioned)
                    {
                        _logger.Info($"{t} -> Source and destination are partitioned but not RowStores. Enabling ORDER hint on partition column.");
                        orderHintType = OrderHintType.PartionKeyOnly;
                    }
                }

                // Use partitions if that make sense
                var partitionType = "Unknown";
                if (usePartitioning == true)
                {
                    var tableSize = sourceTable.Size;

                    // Check if table is big enough to use partitions
                    if (tableSize.RowCount > _config.BatchSize || tableSize.SizeInGB > 1)
                    {
                        // Create the Work Info data based on partition type
                        if (sourceTable.PrimaryIndex.IsPartitioned)
                        {
                            var cis = CreatePhysicalPartitionedTableCopyInfo(sourceTable);
                            cis.ForEach(ci =>
                            {
                                ci.SourceTableInfo = sourceTable;
                                ci.DestinationTableInfo = destinationTable;
                                ci.OrderHintType = orderHintType;
                            });
                            result.CopyInfo.AddRange(cis);
                            partitionType = "Physical";

                        }
                        else
                        {
                            var cis = CreateLogicalPartitionedTableCopyInfo(sourceTable);
                            cis.ForEach(ci =>
                            {
                                ci.SourceTableInfo = sourceTable;
                                ci.DestinationTableInfo = destinationTable;
                                ci.OrderHintType = orderHintType;
                            });
                            result.CopyInfo.AddRange(cis);
                            partitionType = "Logical";
                        }
                    }
                    else
                    {
                        _logger.Info($"{t} -> Table is small, partitioned copy will not be used.");
                        usePartitioning = false;
                    }
                }

                // Otherwise just copy the table, possibility using 
                // and ordered bulk copy                                
                if (usePartitioning == false)
                {
                    var ci = new NoPartitionsCopyInfo() { SourceTableInfo = sourceTable };
                    ci.OrderHintType = orderHintType;
                    ci.DestinationTableInfo = destinationTable;
                    result.CopyInfo.Add(ci);
                    partitionType = "None";
                }

                _logger.Info($"{t} -> Analysis result: usePartioning={usePartitioning}, partitionType={partitionType}, orderHintType={orderHintType}");
            }

            result.Outcome = AnalysisOutcome.Success;
            return result;
        }

        private List<CopyInfo> CreateLogicalPartitionedTableCopyInfo(TableInfo ti)
        {
            string tableName = ti.TableName;
            TableSize tableSize = ti.Size;

            var copyInfo = new List<CopyInfo>();

            long partitionCount = 1;

            _logger.Debug($"{tableName}: RowCount={tableSize.RowCount}, SizeInGB={tableSize.SizeInGB}");

            switch (_config.LogicalPartitioningStrategy)
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
            _logger.Info($"{tableName} -> Parallel load will use {partitionCount} logical partitions (Logical partition size: {ps:0.00} GB, Rows: {pc:0.00}).");

            foreach (var n in Enumerable.Range(1, (int)partitionCount))
            {
                var cp = new LogicalPartitionCopyInfo();
                cp.PartitionNumber = n;
                cp.SourceTableInfo = ti;
                cp.LogicalPartitionsCount = (int)partitionCount;
                copyInfo.Add(cp);
            }

            return copyInfo;
        }

        private List<CopyInfo> CreatePhysicalPartitionedTableCopyInfo(TableInfo ti)
        {
            string tableName = ti.TableName;

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

            _logger.Info($"{tableName} -> Parallel load will use {partitionCount} physical partition(s).");

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

            foreach (var n in Enumerable.Range(1, partitionCount))
            {
                var cp = new PhysicalPartitionCopyInfo();
                cp.PartitionNumber = n;
                cp.SourceTableInfo = ti;
                cp.PartitionColumn = partitionInfo.PartitionColumn;
                cp.PartitionFunction = partitionInfo.PartitionFunction;

                copyInfo.Add(cp);
            }

            return copyInfo;
        }
    }
}
