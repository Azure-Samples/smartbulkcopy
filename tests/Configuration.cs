using NUnit.Framework;
using NLog;
using SmartBulkCopy;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System;
using DotNetEnv;
using System.IO;

namespace SmartBulkCopy.Tests
{
    public class Configuration
    {
        private SmartBulkCopyConfiguration _config;
        private ILogger _logger;

        [SetUp]
        public void Setup()
        {
            Env.Load();

            _logger = LogManager.GetCurrentClassLogger();

            _config = SmartBulkCopyConfiguration.LoadFromConfigFile("smartbulkcopy.config.test.json", _logger);
        }

        [Test]
        public void CommandTimeOut()
        {
            Assert.AreEqual(_config.CommandTimeOut, 90 * 60);
        }

        [Test]
        public void StopIfSecondaryIndex()
        {
            Assert.IsTrue(_config.StopIf.HasFlag(StopIf.SecondaryIndex));
        }

        [Test]
        public void DontStopIfTemporalTable()
        {
            Assert.IsFalse(_config.StopIf.HasFlag(StopIf.TemporalTable));
        }

        [Test]
        public void PersistConfigurationToFile()
        {
            string configuration = System.IO.File.ReadAllText(Path.Combine(Directory.GetCurrentDirectory(), "smartbulkcopy.config.test.json"));
            SmartBulkCopyConfiguration.PersistConfigurationToFile(configuration);

            Assert.That(Path.Combine(Directory.GetCurrentDirectory(), "smartbulkcopy.config.json"), Does.Exist);
            SmartBulkCopyConfiguration config = SmartBulkCopyConfiguration.LoadFromConfigFile(_logger);
            
            Assert.AreEqual(System.Text.Json.JsonSerializer.Serialize(_config), System.Text.Json.JsonSerializer.Serialize(config));
        }
    }
}