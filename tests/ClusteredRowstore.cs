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
    public class ClusteredRowstoreTests : BaseTests
    {
        [Test]
        public async Task ClusteredRowstore_Small()
        {
            var tar = await AnalyzeTable("schema1.clustered_rowstore");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(NoPartitionsCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(1, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.ClusteredIndex, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("col17,col19", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderByString());
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionByString());
        }

        [Test]
        public async Task ClusteredRowstore_Big()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_CLUSTERED_ROWSTORE");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(NoPartitionsCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(1, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.ClusteredIndex, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("[L_ORDERKEY],[L_LINENUMBER]", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderByString());
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionByString());
        }

        [Test]
        public async Task ClusteredRowstore_Big_Partitioned()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_CLUSTERED_ROWSTORE_PARTITIONED");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(PhysicalPartitionCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(85, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.ClusteredIndex, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("[L_ORDERKEY],[L_LINENUMBER],[L_COMMITDATE]", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderByString());
            Assert.AreEqual("[L_COMMITDATE]", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionByString());
        }

        [Test]
        public async Task ClusteredRowstore_Calculated_Big()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_CLUSTERED_ROWSTORE_CALCULATED");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(NoPartitionsCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(1, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.ClusteredIndex, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderByString());
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionByString());
        }

        [Test]
        public async Task ClusteredRowstore_Calculated_Big_Partitioned()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_CLUSTERED_ROWSTORE_CALCULATED_PARTITIONED");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(PhysicalPartitionCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(85, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.ClusteredIndex, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("[L_COMMITDATE]", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderByString());
            Assert.AreEqual("[L_COMMITDATE]", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionByString());
        }
    }
}