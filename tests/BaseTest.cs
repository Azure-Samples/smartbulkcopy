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
    public class BaseTests
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
            _config.LogicalPartitioningStrategy = LogicalPartitioningStrategy.Auto;
            //_config.LogicalPartitions = 7;
        }

        protected async Task<AnalysisResult> AnalyzeTable(string tableForTest)
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