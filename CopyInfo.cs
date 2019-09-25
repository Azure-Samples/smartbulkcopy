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
    abstract class CopyInfo
    {
        public string TableName;
        public List<string> Columns = new List<string>();
        public int PartitionNumber;
        public abstract string GetPredicate();
        public string GetSelectList()
        {
            return "[" + string.Join("],[", this.Columns) + "]";
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
                return $"ABS(CAST(%%PhysLoc%% AS BIGINT)) % {LogicalPartitionsCount} = {PartitionNumber - 1}";
            else
                return String.Empty;
        }
    }
}