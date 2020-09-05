using System;
using System.IO;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Data.SqlClient;
using Dapper;
using NLog;

namespace SmartBulkCopy
{
    public class TableSize
    {
        public long RowCount = 0;
        public int SizeInGB = 0;
    }

    public class Column {
        public string ColumnName;
    }

    public class IndexColumn: Column
    {        
        public int OrdinalPosition;
        public bool IsDescending;        
        public int PartitionOrdinal;
    }

    public abstract class Index
    {
        public List<IndexColumn> Columns = new List<IndexColumn>();

        public virtual string GetOrderBy(bool excludePartitionColumn = true){
            return string.Empty;
        }
        public virtual string GetPartitionBy(){
            return string.Empty;
        }

        public virtual bool IsPartitioned { get; }
    }

    public class UnknownIndex: Index 
    { }

    public class Heap: Index
    { 
        public string PartitionColumn = string.Empty;

        public override string GetPartitionBy()
        {
            return PartitionColumn;
        }

        public override bool IsPartitioned {
            get {
                return PartitionColumn != string.Empty;
            }
        }
    }

    public class RowStoreClusteredIndex: Index
    {      
        public override string GetOrderBy(bool excludePartitionColumn = true)
        {           
            int op = -1;
            if (excludePartitionColumn == true) op = 1;

            var orderList = from c in Columns 
                        where c.PartitionOrdinal != op
                        orderby c.OrdinalPosition
                        select c.ColumnName + (c.IsDescending == true ? " DESC" : "");      
    
            return string.Join(",", orderList);
        }

        public override string GetPartitionBy()
        {
            var orderList = from c in Columns 
                        where c.PartitionOrdinal != 0
                        orderby c.PartitionOrdinal
                        select c.ColumnName;      
    
            return string.Join(",", orderList);
        }

        public override bool IsPartitioned {
            get {
                return Columns.Any(c => c.PartitionOrdinal != 0);
            }
        }

    }

    public class ColumnStoreClusteredIndex: Index
    { 
        public string PartitionColumn = string.Empty;

        public override string GetPartitionBy()
        {
            return PartitionColumn;
        }

        public override bool IsPartitioned {
            get {
                return PartitionColumn != string.Empty;
            }
        }
    }

    public class TableInfo 
    {
        public readonly string ServerName;
        public readonly string DatabaseName;
        public readonly string TableName;
        public bool Exists;        
        public Index PrimaryIndex = new UnknownIndex();
        public List<string> Columns = new List<string>();
        public string TableLocation => $"{DatabaseName}.{TableName}@{ServerName}";
        public TableSize Size = new TableSize();
        
        public TableInfo(string serverName, string databaseName, string tableName)
        {
            this.ServerName = serverName;
            this.DatabaseName = databaseName;
            this.TableName = tableName;
        }

        public override string ToString()
        {
            return TableLocation;
        }
    }    

    public class UnknownTableInfo: TableInfo
    {
        public UnknownTableInfo(): base("Unknown", "Unknown", "Unknown") { }

    }

    public class TablesInfoCollector    
    {
        private readonly ILogger _logger;        
        private readonly List<string> _tablesToAnalyze = new List<string>();
        private readonly string _connectionString;
        public TablesInfoCollector(string connectionString, List<string> tablesToAnalyze, ILogger logger)
        {
            this._connectionString = connectionString;
            this._tablesToAnalyze.AddRange(tablesToAnalyze);
            this._logger = logger;
        }

        public async Task<List<TableInfo>> CollectTablesInfoAsync()
        {
            var result = new List<TableInfo>();
           
            var collector = new TableInfoCollector(_connectionString, _logger);

            foreach(var ta in _tablesToAnalyze)
            {                
                var ti = await collector.CollectAsync(ta);
                result.Add(ti);
            }             

            return result;
        }      
    }    

    public class TableInfoCollector
    {
        private readonly ILogger _logger;        
        private readonly SqlConnection _conn;
        private readonly string _serverName;
        private readonly string _databaseName;
        private TableInfo _tableInfo;
        
        public TableInfoCollector(string connectionString, ILogger logger)
        {
            var sqsb = new SqlConnectionStringBuilder(connectionString);
            this._serverName = sqsb.DataSource;
            this._databaseName = sqsb.InitialCatalog;            
            this._conn = new SqlConnection(connectionString);
            this._logger = logger;
        }

        public async Task<TableInfo> CollectAsync(string tableName)
        {
            _tableInfo = new TableInfo(this._serverName, this._databaseName, tableName);            

            await CheckTableExistenceAsync();
            if (_tableInfo.Exists == false) return _tableInfo;

            // Get info on primary index
            await GetHeapInfoAsync();
            await GetRowStoreClusteredInfoAsync();
            await GetColumnStoreClusteredInfoAsync();

            // Check if secondary index exists
            // TODO -> Is really needed?

            await GetTableSizeAsync();
            await GetColumnsForBulkCopyAsync();

            return _tableInfo;
        }

        private async Task CheckTableExistenceAsync()
        {
            bool result = false;            
            try
            {
                var tableName = await _conn.QuerySingleOrDefaultAsync<string>(@"select 
                        [FullName] = QUOTENAME(s.[name]) + '.' + QUOTENAME(t.[name]) 
                    from 
                        sys.tables t 
                    inner join 
                        sys.schemas s on t.[schema_id] = s.[schema_id]
                    where
                        s.[name] = PARSENAME(@tableName, 2)
                    and
                        t.[name] = PARSENAME(@tableName, 1)", new { @tableName = _tableInfo.TableName });
                
                if (tableName != default(string)) result = true;
            }
            catch (InvalidOperationException)
            {
                result = false;
            }
            finally
            {
                _conn.Close();
            }

            _tableInfo.Exists = result;
        }

        private async Task GetRowStoreClusteredInfoAsync()
        {
            if (!(_tableInfo.PrimaryIndex is UnknownIndex)) return;

            string sql = @"
                select
                    c.name as ColumnName,
                    ic.key_ordinal as OrdinalPosition,
                    ic.is_descending_key as IsDescending,
                    ic.partition_ordinal as PartitionOrdinal
                from
                    sys.indexes i
                inner join
                    sys.index_columns ic on ic.index_id = i.index_id and ic.[object_id] = i.[object_id] 
                inner join
                    sys.columns c on ic.column_id = c.column_id and ic.[object_id] = c.[object_id]
                where
                    i.[object_id] = object_id(@tableName) 
                and
                    i.[type] in (1)
                order by
                    ic.key_ordinal,
                    ic.partition_ordinal
            ";

            LogDebug($"Collecting Clustered RowStore Info. Executing:\n{sql}");

            var qr = await _conn.QueryAsync<IndexColumn>(sql, new { @tableName = _tableInfo.TableName });

            var rci = new RowStoreClusteredIndex();
            rci.Columns.AddRange(qr.ToList());

            if (rci.Columns.Count > 0)
            {
                LogDebug($"Detected Clustered RowStore Index: {rci.GetOrderBy()}");
                _tableInfo.PrimaryIndex = rci;

                if (rci.Columns.Any(c => c.PartitionOrdinal != 0)) {
                    LogDebug($"Clustered RowStore Index is Partitioned on: {rci.GetPartitionBy()}");                    
                }
            }
        }

        private async Task GetHeapInfoAsync()
        {
            if (!(_tableInfo.PrimaryIndex is UnknownIndex)) return;

            string sql = @"
                select
                    c.name as ColumnName
                from
                    sys.indexes i
                left join
                    sys.index_columns ic on ic.index_id = i.index_id and ic.[object_id] = i.[object_id] 
                left join
                    sys.columns c on ic.column_id = c.column_id and ic.[object_id] = c.[object_id]
                where
                    i.[object_id] = object_id(@tableName) 
                and
                    i.[type] in (0)
                and
                    (ic.partition_ordinal != 0 or ic.partition_ordinal is null)
            ";            

            LogDebug($"Collecting Heap Info. Executing:\n{sql}");

            var columns = (await _conn.QueryAsync<Column>(sql, new { @tableName = _tableInfo.TableName })).ToList();;
            
            if (columns.Count() == 1) 
            {
                var h = new Heap();

                if (columns[0].ColumnName != null)
                {                    
                    h.PartitionColumn = columns[0].ColumnName;
                    LogDebug($"Heap is Partitioned on: {h.GetPartitionBy()}");                                        
                }

                _tableInfo.PrimaryIndex = h;
            }
        }

        private async Task GetColumnStoreClusteredInfoAsync()
        {
            if (!(_tableInfo.PrimaryIndex is UnknownIndex)) return;
            
            string sql = @"
                with cte as
                (
                    select
                        1 as sortKey,
                        c.name as ColumnName
                    from
                        sys.indexes i
                    left join
                        sys.index_columns ic on ic.index_id = i.index_id and ic.[object_id] = i.[object_id] 
                    left join
                        sys.columns c on ic.column_id = c.column_id and ic.[object_id] = c.[object_id]
                    where
                        i.[object_id] = object_id(@tableName) 
                    and
                        i.[type] in (5)
                    and
                        (ic.partition_ordinal != 0 or ic.partition_ordinal is null)
                    
                    union

                    select
                        2 as sortKey,
                        null as ColumnName
                    from
                        sys.indexes i
                    where
                        i.[object_id] = object_id(@tableName) 
                    and
                        i.[type] in (5)
                )
                select top(1) ColumnName from cte order by sortKey               
            ";

            LogDebug($"Collecting Clustered ColumnStore Info. Executing:\n{sql}");

            var columns = (await _conn.QueryAsync<Column>(sql, new { @tableName = _tableInfo.TableName })).ToList();;
            
            if (columns.Count() == 1) 
            {
                var h = new ColumnStoreClusteredIndex();

                if (columns[0].ColumnName != null)
                {                    
                    h.PartitionColumn = columns[0].ColumnName;
                    LogDebug($"Clustered ColumnStore is Partitioned on: {h.GetPartitionBy()}");                                        
                }

                _tableInfo.PrimaryIndex = h;
            }
        }

        private async Task GetTableSizeAsync()
        {
            string sql = @"
                select 
                    sum(row_count) as [RowCount],
                    cast((sum(used_page_count) * 8) / 1024. / 1024. as int) as SizeInGB
                from 
                    sys.dm_db_partition_stats 
                where 
                    [object_id] = object_id(@tableName) 
                and 
                    index_id in (0, 1, 5) 
                group by 
                    [object_id]
            ";

            LogDebug($"Executing:\n{sql}");

            _tableInfo.Size = await _conn.QuerySingleAsync<TableSize>(sql, new { @tableName = _tableInfo.TableName });
        }

        private async Task GetColumnsForBulkCopyAsync()
        {
            LogDebug($"Creating column list...");

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

            LogDebug($"Executing:\n{sql}");

            var columns = await _conn.QueryAsync<string>(sql, new { @tableName = _tableInfo.TableName });
            _tableInfo.Columns = columns.ToList();            
        }

        private void LogDebug(string message)
        {
            _logger.Debug($"[{_tableInfo.TableLocation}] ${message}");
        }
    }
}