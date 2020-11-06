using System;
using System.IO;
using Microsoft.SqlServer.Dac;
using System.Data.SqlClient;
using System.Data;
using NLog;

namespace SmartBulkCopy
{
    public class SchemaClone
    {
        SchemaCloneConfiguration _config;
        ILogger _log;         

        public SchemaClone(SchemaCloneConfiguration config, ILogger log)
        {
            _config = config;
            _log = log;
        }

        public void CloneDatabaseSchema()
        {
            var ms = new MemoryStream();

            var scs = new SqlConnectionStringBuilder(_config.SourceConnectionString);
            var dcs = new SqlConnectionStringBuilder(_config.DestinationConnectionString);
            
            _log.Info($"Extracting database schema from {scs.DataSource}/{scs.InitialCatalog}...");            
            var dc = new DacServices(scs.ConnectionString);       
            dc.ProgressChanged += OnProgressChanged;
            dc.Message += OnMessage;
            dc.Extract(ms, scs.InitialCatalog, "SmartBulkCopy.SchemaClone", new Version(1,0));
            _log.Info($"Done.");            

            _log.Info($"Loading generated DacPac...");
            var dp = DacPackage.Load(ms);
            _log.Info($"Done.");            

            // Console.WriteLine("-----> Generating Script...");
            // var s = DacServices.GenerateCreateScript(dp, dcs.InitialCatalog, ddo);
            // File.WriteAllText("./test.sql", s);

            _log.Info($"Deploying schema to {dcs.DataSource}/{dcs.InitialCatalog}...");            
            var dc2 = new DacServices(dcs.ConnectionString);
            dc2.ProgressChanged += OnProgressChanged;
            dc2.Message += OnMessage;
            var ddo = new DacDeployOptions();        
            ddo.CreateNewDatabase = false;
            ddo.BlockOnPossibleDataLoss = true;        
            dc2.Deploy(dp, dcs.InitialCatalog, true, ddo);       
            _log.Info($"Done.");  
        }

        public void OnProgressChanged(object sender, DacProgressEventArgs e)
        {
            _log.Info($"{e.Message}: {e.Status}");
        }

        public void OnMessage(object server, DacMessageEventArgs e)
        {
            _log.Info($"{e.Message}");
        }
    }
}
