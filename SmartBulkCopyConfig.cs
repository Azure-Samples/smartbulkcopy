using System;
using System.IO;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.DependencyInjection;

namespace SmartBulkCopy
{
    class SmartBulkCopyConfiguration 
    {
        public string SourceConnectionString;
        
        public string DestinationConnectionString;         

        public List<string> TablesToCopy = new List<string>();

        private int _batchSize = 100000;
        public int BatchSize {
            get { return _batchSize; }
            set {
                if (value < 1000) throw new ArgumentException($"{nameof(BatchSize)}cannot be less than 1000");
                if (value > 100000000) throw new ArgumentException($"{nameof(BatchSize)} cannot be greather than 100000000");
                _batchSize = value;
            }
        }

        private int _maxParallelTasks = 7;
        public int MaxParallelTasks {
            get {
                return _maxParallelTasks;
            }
            set {
                if (value < 1) throw new ArgumentException($"{nameof(MaxParallelTasks)}cannot be less than 1");
                if (value > 32) throw new ArgumentException($"{nameof(MaxParallelTasks)} cannot be greather than 32");
                _maxParallelTasks = value;
            }
        }

        private int _logicalPartitions = 7;
        public int LogicalPartitions  {
            get {
                return _logicalPartitions;
            }
            set {
                if (value < 1) throw new ArgumentException($"{nameof(LogicalPartitions)} cannot be less than 1");
                if (value > 32) throw new ArgumentException($"{nameof(LogicalPartitions)} cannot be greather than 32");
                _logicalPartitions = value;
            }
        }

        public bool TruncateTables = false;

        public bool CheckUsingSnapshot = true;

        private SmartBulkCopyConfiguration() {}

        public static SmartBulkCopyConfiguration LoadFromConfigFile()
        {
            return LoadFromConfigFile("smartbulkcopy.config");
        }

        public static SmartBulkCopyConfiguration LoadFromConfigFile(string configFile)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile(configFile, optional: false, reloadOnChange: false)
                .Build();                 

            var sbcc = new SmartBulkCopyConfiguration();                

            sbcc.SourceConnectionString = config["source:connection-string"];
            sbcc.DestinationConnectionString = config["destination:connection-string"];
            sbcc.BatchSize = int.Parse(config?["options:batch-size"] ?? sbcc.BatchSize.ToString());
            sbcc.LogicalPartitions = int.Parse(config?["options:logical-partitions"] ?? sbcc.LogicalPartitions.ToString());
            sbcc.MaxParallelTasks = int.Parse(config?["options:tasks"] ?? sbcc.MaxParallelTasks.ToString());
            sbcc.TruncateTables = bool.Parse(config?["options:truncate-tables"] ?? sbcc.TruncateTables.ToString());
            sbcc.CheckUsingSnapshot = bool.Parse(config?["options:check-snapshot"] ?? sbcc.CheckUsingSnapshot.ToString());
            
            var tablesArray = config.GetSection("tables").GetChildren();                        
            foreach(var t in tablesArray) {
                sbcc.TablesToCopy.Add(t.Value);
            }

            return sbcc;
        }
    }
}