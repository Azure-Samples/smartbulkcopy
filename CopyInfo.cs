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

namespace SmartBulkCopy
{

    public class TableOrderInfo
    {
        public List<SortColumn> SortColumns = new List<SortColumn>();

        public bool IsFound => SortColumns.Count > 0;

        public string GetPartitionOrderBy()
        {
            var orderList = from c in SortColumns 
                        where c.OrdinalPosition == 0
                        orderby c.OrdinalPosition
                        select c.ColumnName + (c.IsDescending == true ? " DESC" : "");      
    
            return string.Join(",", orderList);
        }

        public string GetOrderBy(bool excludePartitionColumn = true)
        {           
            int op = 0;
            if (excludePartitionColumn == true) op = -1;

            var orderList = from c in SortColumns 
                        where c.OrdinalPosition != op
                        orderby c.OrdinalPosition
                        select c.ColumnName + (c.IsDescending == true ? " DESC" : "");      
    
            return string.Join(",", orderList);
        }
    }

    public class SortColumn {
        public string ColumnName;
        public int OrdinalPosition;
        public bool IsDescending;
    }

    enum OrderHintType
    {
        None,
        ClusteredIndex,
        PartionKeyOnly
    }

    abstract class CopyInfo
    {
        public string TableName;
        public List<string> Columns = new List<string>();
        public TableOrderInfo OrderInfo = new TableOrderInfo();
        public OrderHintType OrderHintType = OrderHintType.None;
        public int PartitionNumber;
        public abstract string GetPredicate();
        public string GetSelectList()
        {
            return "[" + string.Join("],[", this.Columns) + "]";
        }
        public string GetOrderBy()
        {           
            return OrderInfo.GetOrderBy(excludePartitionColumn:true);
        }
    }

    class NoPartitionsCopyInfo : CopyInfo
    {
        public NoPartitionsCopyInfo()
        {
            PartitionNumber = 1;
        }

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
                return $"ABS(CAST(%%PhysLoc%% AS BIGINT)) % {LogicalPartitionsCount} = {PartitionNumber - 1} OPTION (MAXDOP 1)";
            else
                return String.Empty;
        }
    }
}