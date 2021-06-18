using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Dapper;
using NLog;

namespace SmartBulkCopy
{
    public class HistoryInfo
    {
        public string HistoryTable;
        public string PeriodStartColumn;
        public string PeriodEndColumn;
        public string RetentionPeriod; 
    }
    public enum TableType {
        Regular = 0,
        SystemVersionedTemporal = 2,
        HistoryTable = 1
    }
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
        public bool IsComputed;
    }

    public abstract class Index
    {
        public List<IndexColumn> Columns = new List<IndexColumn>();

        public virtual bool IsPartitioned => Columns.Any(c => c.PartitionOrdinal != 0);

        public virtual IOrderedEnumerable<IndexColumn> GetOrderBy() => this.Columns.Where(c => c.IsComputed == false).OrderBy(c => c.OrdinalPosition);
        
        public virtual IOrderedEnumerable<IndexColumn> GetPartitionBy() => this.Columns.Where(c => c.PartitionOrdinal != 0).OrderBy(c => c.PartitionOrdinal);        

        public virtual string GetOrderByString() {
            var orderList = 
                from c in this.GetOrderBy()
                //dbender: Key word throws exception if not in the brackets
                //select c.ColumnName + (c.IsDescending == true ? " DESC" : "");
                select "[" + c.ColumnName + (c.IsDescending == true ? "] DESC" : "]");

            return string.Join(",", orderList);
        }

        public virtual string GetPartitionByString(){
            var orderList = 
                from c in this.GetPartitionBy()
                select c.ColumnName;      
    
            return string.Join(",", orderList);
        }
    }

    public class UnknownIndex: Index 
    { }

    public class Heap: Index
    {        
        public override string GetOrderByString()
        {      
            return string.Empty;
        }
    }

    public class RowStoreClusteredIndex: Index
    { }

    public class ColumnStoreClusteredIndex: Index
    { 
        public override string GetOrderByString()
        {                       
            return string.Empty;
        }
    }

    public class TableInfo
    {
        public readonly string ServerName;
        public readonly string DatabaseName;
        public readonly string TableName;
        public bool Exists = true;
        public Index PrimaryIndex = new UnknownIndex();
        public int SecondaryIndexes = 0;
        public int ForeignKeys = 0;
        public List<string> Columns = new List<string>();
        public string TableLocation => $"{DatabaseName}.{TableName}@{ServerName}";
        public TableSize Size = new TableSize();
        public TableType Type = TableType.Regular;        
        public HistoryInfo HistoryInfo = null;
        
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

            // Get all other info
            await GetHeapInfoAsync();
            await GetRowStoreClusteredInfoAsync();
            await GetColumnStoreClusteredInfoAsync();
            await GetSecondaryIndexesCountAsync();
            await GetForeignKeyCountInfo();
            await GetTableTypeAsync();
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
                    ic.partition_ordinal as PartitionOrdinal,
                    c.is_computed as IsComputed
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
                LogDebug($"Detected Clustered RowStore Index: {rci.GetOrderByString()}");
                _tableInfo.PrimaryIndex = rci;

                if (rci.IsPartitioned) {
                    LogDebug($"Clustered RowStore Index is Partitioned on: {rci.GetPartitionByString()}");                    
                }
            }
        }

        private async Task GetHeapInfoAsync()
        {
            if (!(_tableInfo.PrimaryIndex is UnknownIndex)) return;

            string sql = @"
                select
                    c.name as ColumnName,
                    ic.key_ordinal as OrdinalPosition,
                    ic.is_descending_key as IsDescending,
                    ic.partition_ordinal as PartitionOrdinal,
                    c.is_computed as IsComputed
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

            var columns = (await _conn.QueryAsync<IndexColumn>(sql, new { @tableName = _tableInfo.TableName })).ToList();;
            
            if (columns.Count() == 1) 
            {                
                var h = new Heap();
                if (columns.First().ColumnName != null)
                {
                    h.Columns.Add(columns.First());
                    LogDebug($"Heap is Partitioned on: {h.GetPartitionByString()}");                                        
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
                        c.name as ColumnName,
                        ic.key_ordinal as OrdinalPosition,
                        ic.is_descending_key as IsDescending,
                        ic.partition_ordinal as PartitionOrdinal,
                        c.is_computed as IsComputed
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
                        null as ColumnName,
                        null as OrdinalPosition,
                        null as IsDescending,
                        null as PartitionOrdinal,
                        null as IsComputed
                    from
                        sys.indexes i
                    where
                        i.[object_id] = object_id(@tableName) 
                    and
                        i.[type] in (5)
                )
                select top(1) * from cte order by sortKey               
            ";

            LogDebug($"Collecting Clustered ColumnStore Info. Executing:\n{sql}");

            var columns = (await _conn.QueryAsync<IndexColumn>(sql, new { @tableName = _tableInfo.TableName })).ToList();;
            
            if (columns.Count() == 1) 
            {
                var h = new ColumnStoreClusteredIndex();

                if (columns[0].ColumnName != null)
                {                    
                    h.Columns.Add(columns[0]);
                    LogDebug($"Clustered ColumnStore is Partitioned on: {h.GetPartitionByString()}");                                        
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
                    and 
                        [system_type_id] not in (189) -- timestamp/rowversion
                    ";

            LogDebug($"Executing:\n{sql}");

            var columns = await _conn.QueryAsync<string>(sql, new { @tableName = _tableInfo.TableName });
            _tableInfo.Columns = columns.ToList();            
        }

        private async Task GetTableTypeAsync()
        {
            LogDebug($"Identifying table type...");

            var sql = $@"
                    SELECT 
                        CASE 
                            WHEN CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) >= 13 OR CAST(SERVERPROPERTY('Edition') AS SYSNAME) LIKE N'%Azure%'
                                THEN 
                            (
                                SELECT [temporal_type] FROM sys.tables WHERE [object_id] = object_id(@tableName) 
                            )
                            ELSE 
                            (
                                SELECT 0 as [temporal_type] FROM sys.tables WHERE [object_id] = object_id(@tableName)  
                            )
                        END AS [temporal_type]
                    ";
            LogDebug($"Executing:\n{sql}");

            _tableInfo.Type = await _conn.QuerySingleAsync<TableType>(sql, new { @tableName = _tableInfo.TableName });

            if (_tableInfo.Type == TableType.SystemVersionedTemporal)
            {
                LogDebug($"Getting details on system versioning table and columns...");

                sql = $@"
                    select
                        QUOTENAME(SCHEMA_NAME(h.schema_id)) + '.' + QUOTENAME(h.name) as HistoryTable,
                        (select QUOTENAME([name]) from sys.columns c1 where c1.[object_id] = t.[object_id] and [generated_always_type] = 1) as PeriodStartColumn,
                        (select QUOTENAME([name]) from sys.columns c1 where c1.[object_id] = t.[object_id] and [generated_always_type] = 2) as PeriodEndColumn,
                        ISNULL(CAST(NULLIF([t].[history_retention_period], -1) AS SYSNAME) + ' ', '') + [t].[history_retention_period_unit_desc] as RetentionPeriod
                    from
                        sys.tables t
                    inner join
                        sys.tables h  on t.[history_table_id] = h.[object_id]
                    where 
                        t.[object_id] = object_id(@tableName) 
                ";

                LogDebug($"Executing:\n{sql}");

                _tableInfo.HistoryInfo = await _conn.QuerySingleAsync<HistoryInfo>(sql, new { @tableName = _tableInfo.TableName });
            }

        }

        private async Task GetSecondaryIndexesCountAsync()
        {
            LogDebug($"Gathering secondary indexes info...");

            var sql = $@"
                    select 
                        count(*) as SecondaryIndexesCount
                    from 
                        sys.indexes 
                    where 
                        [object_id] = object_id(@tableName) 
                    and
                        [type] not in (0,1,5)
                    ";

            LogDebug($"Executing:\n{sql}");

            _tableInfo.SecondaryIndexes = await _conn.QuerySingleOrDefaultAsync<int>(sql, new { @tableName = _tableInfo.TableName });
        }

        private async Task GetForeignKeyCountInfo()
        {
            LogDebug($"Gathering foreign keys info...");

            var sql = $@"
                    select
	                    count(*) as ForeignKeysCount
                    from
                        sys.[foreign_keys]
                    where
                        [parent_object_id] = object_id(@tableName) 
                    or 
                        [referenced_object_id] = object_id(@tableName) 
                    ";

            LogDebug($"Executing:\n{sql}");

            _tableInfo.ForeignKeys = await _conn.QuerySingleOrDefaultAsync<int>(sql, new { @tableName = _tableInfo.TableName });
        }

        private void LogDebug(string message)
        {
            _logger.Debug($"[{_tableInfo.TableLocation}] {message}");
        }
    }
}