using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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

                var _sourceConnectionString = config["source:connection-string"];
                var _destinationConnectionString = config["destination:connection-string"];

                var sbc = new SmartBulkCopy(_sourceConnectionString, _destinationConnectionString, logger);
                result = await sbc.Copy();
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Stopped program because of exception");
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
