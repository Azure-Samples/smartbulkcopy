using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using NLog;

namespace SmartBulkCopy
{
    public class SchemaCloneConfiguration 
    {        
        public string SourceConnectionString;
        
        public string DestinationConnectionString;         

        private SchemaCloneConfiguration() {}

        public static SchemaCloneConfiguration EmptyConfiguration => new SchemaCloneConfiguration();

        public static SchemaCloneConfiguration LoadFromConfigFile(ILogger logger)        
        {
            return LoadFromConfigFile("smartbulkcopy.config", logger);
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

            return shc;
        }
    }
}