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
    public class HeapTests: BaseTests
    {
        [Test]
        public async Task Heap_Small()
        {
            var tar = await AnalyzeTable("schema1.heap");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(NoPartitionsCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(1, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.None, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderByString());
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionByString());
        }

        [Test]
        public async Task Heap_Big()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_HEAP");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(LogicalPartitionCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(9, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.None, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderByString());
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionByString());
        }

        [Test]
        public async Task Heap_Big_Partitioned()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_HEAP_PARTITIONED");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(PhysicalPartitionCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(85, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.PartionKeyOnly, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderByString());
            Assert.AreEqual("[L_COMMITDATE]", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionByString());
        }
    }
}