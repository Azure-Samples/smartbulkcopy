using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;

namespace HSBulkCopy
{   
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("hsbulkcopy.config.json", optional: false, reloadOnChange: false)
                .Build();

            var _sourceConnectionString = config["source:connection-string"];
            var _destinationConnectionString = config["destination:connection-string"];

            var sbc = new SmartBulkCopy(_sourceConnectionString, _destinationConnectionString);
            var result = await sbc.Copy();

            return result;
        }                
    }
}
