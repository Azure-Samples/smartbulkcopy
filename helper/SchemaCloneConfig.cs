using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using NLog;

namespace SmartBulkCopy
{
    [Flags]
    public enum StopOn {
        None = 0,
        AnyUserObject = 1,
        All = (~1 << 2)
    }

    [Flags]
    public enum ExcludeObjects {
        None = 0,
        PrimaryKeys = 1 << 0,
        UniqueKeys = 1 << 1,
        ForeignKeys = 1 << 2,
        RowstoreNonclusteredIndexes = 1 << 3,
        RowstoreClusteredIndexes = 1 << 4,
        ColumnstoreNonclusteredIndexes = 1 << 5,
        ColumnstoreClusteredIndexes = 1 << 6,
        FullText = 1 << 7,
        All = (~1 << 8)
    }

    public class SchemaCloneConfiguration 
    {        
        public string SourceConnectionString;
        
        public string DestinationConnectionString;    

        public StopOn StopOn = StopOn.All;

        public ExcludeObjects ExcludeObjects = ExcludeObjects.None;    

        private SchemaCloneConfiguration() {}

        public static SchemaCloneConfiguration EmptyConfiguration => new SchemaCloneConfiguration();

        public static SchemaCloneConfiguration LoadFromConfigFile(ILogger logger)        
        {
            return LoadFromConfigFile("../client/smartbulkcopy.config", logger);
        }

        public static SchemaCloneConfiguration LoadFromConfigFile(string configFile, ILogger logger)
        {
            // If config file is not found, automatically add .json extension            
            if (!File.Exists(Path.Combine(Directory.GetCurrentDirectory(), configFile)))
            {
                if (Path.GetExtension(configFile) != ".json")
                    configFile += ".json";
            }

            configFile = Path.GetFullPath(configFile);

            logger.Info($"Loading configuration from: {configFile}...");

            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile(configFile, optional: false, reloadOnChange: false)
                .Build();                 

            var shc = new SchemaCloneConfiguration();                

            shc.SourceConnectionString = config["source:connection-string"] ?? Environment.GetEnvironmentVariable("source-connection-string");
            shc.DestinationConnectionString = config["destination:connection-string"] ?? Environment.GetEnvironmentVariable("destination-connection-string");            

            shc.StopOn = Enum.Parse<StopOn>((config["extensions:schema-clone:stop-if"] ?? "AnyUserObject").Replace("-", ""), ignoreCase: true);

            foreach(var c in config.GetSection("extensions:schema-clone:exclude")?.GetChildren())
            {
                var eo = Enum.Parse<ExcludeObjects>(c.Value.Replace("-", ""), ignoreCase: true);
                shc.ExcludeObjects |= eo;
                logger.Info($"Excluding {eo} from deployment...");
            }

            return shc;
        }
    }
}