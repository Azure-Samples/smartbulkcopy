using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Extensions.Logging;


namespace HSBulkCopy
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            int result = 0;
            var logger = LogManager.GetCurrentClassLogger();
            try
            {
                var config = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("hsbulkcopy.config", optional: false, reloadOnChange: false)
                    .Build();                 

                var sourceConnectionString = config["source:connection-string"];
                var destinationConnectionString = config["destination:connection-string"];
                var tablesArray = config.GetSection("tables").GetChildren();
                
                var tablesToCopy = new List<string>();
                foreach(var t in tablesArray) {
                    tablesToCopy.Add(t.Value);
                }

                var bulkCopyConfig = new SmartBulkCopyConfiguration(
                    sourceConnectionString,
                    destinationConnectionString                    
                );                

                var sbc = new SmartBulkCopy(bulkCopyConfig, logger);
                result = await sbc.Copy(tablesToCopy);
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Stopped program because of exception: ");
                result = 1;
            }
            finally
            {
                LogManager.Shutdown();
            }

            return result;
        }
    }
}
