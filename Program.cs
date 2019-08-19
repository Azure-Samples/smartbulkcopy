using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
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
                SmartBulkCopyConfiguration bulkCopyConfig;
                if (args.Length > 0)
                    bulkCopyConfig = SmartBulkCopyConfiguration.LoadFromConfigFile(args[0]);
                else 
                    bulkCopyConfig = SmartBulkCopyConfiguration.LoadFromConfigFile();
                
                var sbc = new SmartBulkCopy(bulkCopyConfig, logger);
                result = await sbc.Copy();
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Stopped program because of exception.");
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
