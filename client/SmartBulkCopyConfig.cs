using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using NLog;

namespace SmartBulkCopy
{
    [Flags]
    public enum StopIf {       
        None, 
        SecondaryIndex,
        TemporalTable
    }

    public enum SafeCheck {
        None,
        Snapshot,
        ReadOnly
    }

    public enum LogicalPartitioningStrategy {
        Auto,
        Size,
        Count
    }

    public class SmartBulkCopyConfiguration 
    {        
        public bool UseCompatibilityMode = false;

        public string SourceConnectionString;
        
        public string DestinationConnectionString;         

        public List<string> TablesToCopy = new List<string>();

        private int _batchSize = 100000;
        public int BatchSize {
            get { return _batchSize; }
            set {
                if (value < 0) throw new ArgumentException($"{nameof(BatchSize)}cannot be less than 0");                
                _batchSize = value;
            }
        }

        private int _maxParallelTasks = 7;
        public int MaxParallelTasks {
            get {
                return _maxParallelTasks;
            }
            set {
                if (value < 1) throw new ArgumentException($"{nameof(MaxParallelTasks)} cannot be less than 1");
                if (value > 32) throw new ArgumentException($"{nameof(MaxParallelTasks)} cannot be greather than 32");
                _maxParallelTasks = value;
            }
        }

        private int _logicalPartitions = 1;
        public int LogicalPartitions  {
            get {
                return _logicalPartitions;
            }
            set 
            {
                if (LogicalPartitioningStrategy == LogicalPartitioningStrategy.Auto)
                {
                    throw new ArgumentException("Cannot set LogicalPartitions when LogicalPartitionStrategy is set to \"Auto\"");
                }
                if (LogicalPartitioningStrategy == LogicalPartitioningStrategy.Count)
                {
                    if (value < 1) throw new ArgumentException($"{nameof(LogicalPartitions)} count cannot be less than 1");
                    if (value > 128) throw new ArgumentException($"{nameof(LogicalPartitions)} count cannot be greather than 128");                    
                } 
                if (LogicalPartitioningStrategy == LogicalPartitioningStrategy.Size)
                {
                    if (value < 1) throw new ArgumentException($"{nameof(LogicalPartitions)} size cannot be less than 1 GB");
                    if (value > 8) throw new ArgumentException($"{nameof(LogicalPartitions)} size cannot be greather than 8 GB");                    
                } 
                _logicalPartitions = value;
            }
        }

        public int CommandTimeOut = 90 * 60; // 90 minutes

        public bool SyncIdentity = false;

        private LogicalPartitioningStrategy _logicalPartitioningStrategy = LogicalPartitioningStrategy.Auto;

        public LogicalPartitioningStrategy LogicalPartitioningStrategy {
            get {
                return _logicalPartitioningStrategy;
            } 
            set {
                _logicalPartitioningStrategy = value;

                // force check for logical partition value correctness
                if (_logicalPartitioningStrategy != LogicalPartitioningStrategy.Auto)
                    LogicalPartitions = _logicalPartitions; 
            }
        }

        public bool TruncateTables = false;

        public SafeCheck SafeCheck = SafeCheck.ReadOnly;

        public StopIf StopIf = StopIf.SecondaryIndex | StopIf.TemporalTable;

        public int RetryMaxAttempt = 5;
        
        public int RetryDelayIncrement = 10;

        private SmartBulkCopyConfiguration() {}

        public static SmartBulkCopyConfiguration EmptyConfiguration => new SmartBulkCopyConfiguration();

        public static SmartBulkCopyConfiguration LoadFromConfigFile(ILogger logger)        
        {
            return LoadFromConfigFile("smartbulkcopy.config", logger);
        }

        public static SmartBulkCopyConfiguration LoadFromConfigFile(string configFile, ILogger logger)
        {
            // If config file is not found, automatically add .json extension            
            if (!File.Exists(Path.Combine(Directory.GetCurrentDirectory(), configFile)))
            {
                if (Path.GetExtension(configFile) != ".json")
                    configFile += ".json";
            }

            logger.Info($"Loading configuration from: {configFile}...");

            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile(configFile, optional: false, reloadOnChange: false)
                .Build();                 

            var sbcc = new SmartBulkCopyConfiguration();                

            sbcc.SourceConnectionString = config["source:connection-string"] ?? Environment.GetEnvironmentVariable("source-connection-string");
            sbcc.DestinationConnectionString = config["destination:connection-string"] ?? Environment.GetEnvironmentVariable("destination-connection-string");
            sbcc.CommandTimeOut = int.Parse(config?["options:command-timeout"] ?? sbcc.CommandTimeOut.ToString());
            sbcc.BatchSize = int.Parse(config?["options:batch-size"] ?? sbcc.BatchSize.ToString());
            sbcc.MaxParallelTasks = int.Parse(config?["options:tasks"] ?? sbcc.MaxParallelTasks.ToString());
            sbcc.TruncateTables = bool.Parse(config?["options:truncate-tables"] ?? sbcc.TruncateTables.ToString());
            sbcc.SyncIdentity = bool.Parse(config?["options:sync-identity"] ?? sbcc.SyncIdentity.ToString());
            sbcc.UseCompatibilityMode = bool.Parse(config?["options:compatibility-mode"] ?? sbcc.UseCompatibilityMode.ToString());
            sbcc.RetryMaxAttempt = int.Parse(config?["options:retry-connection:max-attempt"] ?? sbcc.RetryMaxAttempt.ToString());
            sbcc.RetryDelayIncrement = int.Parse(config?["options:retry-connection:delay-increment"] ?? sbcc.RetryDelayIncrement.ToString());
            
            var logicalPartitions = (config?["options:logical-partitions"] ?? String.Empty).ToLower().Trim();
            int logicalPartitionSizeOrCount = 0;
            if (logicalPartitions == string.Empty || logicalPartitions == "auto")
            {
                    sbcc.LogicalPartitioningStrategy = LogicalPartitioningStrategy.Auto;
            } 
            else if (logicalPartitions.EndsWith("gb"))
            {
                sbcc.LogicalPartitioningStrategy = LogicalPartitioningStrategy.Size;
                sbcc.LogicalPartitions = int.Parse(logicalPartitions.Replace("gb", string.Empty));                
            }
            else if (int.TryParse(logicalPartitions, out logicalPartitionSizeOrCount))
            {
                sbcc.LogicalPartitioningStrategy = LogicalPartitioningStrategy.Count;
                sbcc.LogicalPartitions = logicalPartitionSizeOrCount;                
            }
            else {
                throw new ArgumentException("Option logical-partitions can only contain \"auto\", or a number (eg: 7) or a size in GB (eg: 10GB)");
            }
                
            var safeCheck = config?["options:safe-check"];
            if (!string.IsNullOrEmpty(safeCheck))
            {
                switch (safeCheck.ToLower()) 
                {
                    case "none": sbcc.SafeCheck = SafeCheck.None;
                        break;

                    case "read-only":
                    case "readonly": sbcc.SafeCheck = SafeCheck.ReadOnly;
                        break;

                    case "snapshot": sbcc.SafeCheck = SafeCheck.Snapshot;
                        break;

                    default: 
                        throw new ArgumentException("Option safe-check can only contain 'none', 'readonly' or 'snapshot' values.");
                }
            }

            var stopIf = config.GetSection("options:stop-if")?.GetChildren();
            foreach(var s in stopIf)
            {
                if (s.Key == "secondary-indexes" && bool.Parse(s.Value) == false) sbcc.StopIf -= StopIf.SecondaryIndex;
                if (s.Key == "temporal-table" && bool.Parse(s.Value) == false) sbcc.StopIf -= StopIf.TemporalTable;                    
            }
            
            // Support for Include and Exclude or fall back to old "include-only" behavior
            var includeTables = config.GetSection("tables:include")?.GetChildren();
            if (includeTables.Count() != 0) 
            {
                foreach(var t in includeTables) {
                    sbcc.TablesToCopy.Add("+:" + t.Value);
                }

                var excludeTables = config.GetSection("tables:exclude")?.GetChildren();
                if (excludeTables != null)
                {
                    foreach(var t in excludeTables) {
                        sbcc.TablesToCopy.Add("-:" + t.Value);
                    }
                }
            } else {
                var tablesArray = config.GetSection("tables").GetChildren();                        
                foreach(var t in tablesArray) {
                    sbcc.TablesToCopy.Add(t.Value);
                }
            }

            return sbcc;
        }
    }
}