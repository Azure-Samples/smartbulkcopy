using System;
using System.Collections.Generic;

namespace SmartBulkCopy
{
    public enum OrderHintType
    {
        None,
        ClusteredIndex,
        PartionKeyOnly
    }

    public abstract class CopyInfo
    {
        public TableInfo SourceTableInfo = new UnknownTableInfo();
        public TableInfo DestinationTableInfo = new UnknownTableInfo();
        public OrderHintType OrderHintType = OrderHintType.None;
        public int PartitionNumber;        

        public string TableName => SourceTableInfo?.TableName;
        public List<string> Columns => SourceTableInfo.Columns;

        public abstract string GetPredicate();
        public string GetSelectList()
        {
            return "[" + string.Join("],[", this.Columns) + "]";
        }
        public string GetOrderBy()
        {           
            return SourceTableInfo.PrimaryIndex.GetOrderByString();
        }
    }

    public class NoPartitionsCopyInfo : CopyInfo
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

    public class PhysicalPartitionCopyInfo : CopyInfo
    {
        public string PartitionFunction;
        public string PartitionColumn;

        public override string GetPredicate()
        {
            return $"$partition.{PartitionFunction}({PartitionColumn}) = {PartitionNumber}";
        }
    }

    public class LogicalPartitionCopyInfo : CopyInfo
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