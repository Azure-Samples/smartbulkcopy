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
    public class Tests
    {
        private SmartBulkCopyConfiguration _config;
        private ILogger _logger;

        [SetUp]
        public void Setup()
        {
            Env.Load();

            _logger = LogManager.GetCurrentClassLogger();
            
            _config = SmartBulkCopyConfiguration.EmptyConfiguration;
            _config.SourceConnectionString = Environment.GetEnvironmentVariable("source-connection-string");
            _config.DestinationConnectionString = Environment.GetEnvironmentVariable("destination-connection-string");
            _config.LogicalPartitioningStrategy = LogicalPartitioningStrategy.Count;
            _config.LogicalPartitions = 7;
        }

        [Test]
        public async Task Heap_Small()
        {
            var tar = await AnalyzeTable("schema1.heap");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(NoPartitionsCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(1, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.None, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: true));
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: false));
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionBy());
        }

        [Test]
        public async Task Heap_Big()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_HEAP");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(LogicalPartitionCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(7, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.None, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: true));
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: false));
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionBy());
        }

        [Test]
        public async Task Heap_Big_Partitioned()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_HEAP_PARTITIONED");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(PhysicalPartitionCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(85, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.PartionKeyOnly, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: true));
            Assert.AreEqual("L_COMMITDATE", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: false));
            Assert.AreEqual("L_COMMITDATE", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionBy());
        }

        [Test]
        public async Task ClusteredRowstore_Small()
        {
            var tar = await AnalyzeTable("schema1.clustered_rowstore");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(NoPartitionsCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(1, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.ClusteredIndex, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("col17,col19", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: true));
            Assert.AreEqual("col17,col19", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: false));
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionBy());
        }

        [Test]
        public async Task ClusteredRowstore_Big()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_CLUSTERED_ROWSTORE");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(NoPartitionsCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(1, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.ClusteredIndex, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("L_ORDERKEY,L_LINENUMBER", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: true));
            Assert.AreEqual("L_ORDERKEY,L_LINENUMBER", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: false));
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionBy());
        }


        [Test]
        public async Task ClusteredColumnstore_Small()
        {
            var tar = await AnalyzeTable("schema1.clustered_columnstore");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(NoPartitionsCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(1, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.None, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: true));
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: false));
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionBy());
        }

        [Test]
        public async Task ClusteredColumnstore_Big()
        {
            var tar = await AnalyzeTable("dbo.LINEITEM_CLUSTERED_COLUMNSTORE");

            Assert.AreEqual(AnalysisOutcome.Success, tar.Outcome);
            Assert.IsInstanceOf(typeof(LogicalPartitionCopyInfo), tar.CopyInfo[0]);
            Assert.AreEqual(7, tar.CopyInfo.Count);
            Assert.AreEqual(OrderHintType.None, tar.CopyInfo[0].OrderHintType);
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: true));
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetOrderBy(excludePartitionColumn: false));
            Assert.AreEqual("", tar.CopyInfo[0].SourceTableInfo.PrimaryIndex.GetPartitionBy());
        }

        private async Task<AnalysisResult> AnalyzeTable(string tableForTest)
        {
            var testTable = new List<string>() { tableForTest };

            var ticSource = new TablesInfoCollector(_config.SourceConnectionString, testTable, _logger);
            var ticDestination = new TablesInfoCollector(_config.DestinationConnectionString, testTable, _logger);

            var tiSource = await ticSource.CollectTablesInfoAsync();
            var tiDestination = await ticDestination.CollectTablesInfoAsync();

            var ta = new TableAnalyzer(_config, _logger);
            return ta.Analyze(testTable, tiSource, tiDestination);
        }
    }
}