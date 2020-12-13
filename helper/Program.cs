using System;
using System.Threading.Tasks;
using NLog;

namespace SmartBulkCopy
{
    class Program
    {
        public static int Main(string[] args)
        {
            var logger = LogManager.GetCurrentClassLogger();

            var v = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString();

            logger.Info($"Schema Helper (part of Smart Bulk Copy toolset) - v. {v}");

            SchemaCloneConfiguration config;
            if (args.Length > 0)
                config = SchemaCloneConfiguration.LoadFromConfigFile(args[0], logger);
            else 
                config = SchemaCloneConfiguration.LoadFromConfigFile(logger);

            var sh = new SchemaClone(config, logger);
            
            return sh.CloneDatabaseSchema();
        }
    }
}
