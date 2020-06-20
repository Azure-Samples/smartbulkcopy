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
    public class ClusteredIndexInfo
    {
        public List<IndexColumn> IndexColumns = new List<IndexColumn>();

        public string GetOrderBy(bool excludePartitionColumn = true)
        {           
            int op = 0;
            if (excludePartitionColumn == true) op = -1;

            var orderList = from c in IndexColumns 
                        where c.OrdinalPosition != op
                        orderby c.OrdinalPosition
                        select c.ColumnName + (c.IsDescending == true ? " DESC" : "");      
    
            return string.Join(",", orderList);
        }
    }

    public class IndexColumn {
        public string ColumnName;
        public int OrdinalPosition;
        public bool IsDescending;
    }

    abstract class CopyInfo
    {
        public string TableName;
        public List<string> Columns = new List<string>();
        public ClusteredIndexInfo ClusteredIndex = new ClusteredIndexInfo();
        public int PartitionNumber;
        public abstract string GetPredicate();
        public string GetSelectList()
        {
            return "[" + string.Join("],[", this.Columns) + "]";
        }
        public string GetOrderBy()
        {           
            return ClusteredIndex.GetOrderBy(excludePartitionColumn:true);
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