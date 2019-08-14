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

namespace HSBulkCopy
{
    class SmartBulkCopyConfiguration 
    {
        public string SourceConnectionString;
        
        public string DestinationConnectionString;         

        public List<string> TablesToCopy = new List<string>();

        public int BatchSize = 100000;

        public int MaxParallelTasks = 7;

        public int LogicalPartitions = 7;

        private SmartBulkCopyConfiguration() {}

        public static SmartBulkCopyConfiguration LoadFromConfigFile()
        {
            return LoadFromConfigFile("hsbulkcopy.config");
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
            sbcc.LogicalPartitions = int.Parse(config["destination:logical-partitions"] ?? sbcc.LogicalPartitions.ToString());
            sbcc.MaxParallelTasks = int.Parse(config["destination:tasks"] ?? sbcc.MaxParallelTasks.ToString());
            
            var tablesArray = config.GetSection("tables").GetChildren();                        
            foreach(var t in tablesArray) {
                sbcc.TablesToCopy.Add(t.Value);
            }

            return sbcc;
        }
    }
}