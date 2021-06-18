using NUnit.Framework;
using NLog;
using SmartBulkCopy;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System;
using DotNetEnv;

namespace SmartBulkCopy.Tests
{
    public class IssuesTests: BaseTests
    {               
         [Test]
        public async Task Issue17_Small()
        {
            var tar = await AnalyzeTable("dbo.Issue17");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(NoPartitionsCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(1, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.ClusteredIndex, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("[PartitionDate],[TransactionId],[CategoryId]", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderByString());
            Assert.AreEqual("[PartitionDate]", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionByString());
        }

         [Test]
        public async Task Issue17_Big()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_CLUSTERED_ROWSTORE_PARTITIONED_ISSUE17");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(PhysicalPartitionCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(85, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.ClusteredIndex, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("[L_COMMITDATE],[L_ORDERKEY],[L_LINENUMBER] DESC", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderByString());
            Assert.AreEqual("[L_COMMITDATE]", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionByString());
        }        
    }
}