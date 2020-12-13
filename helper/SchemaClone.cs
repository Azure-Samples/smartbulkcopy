using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using Microsoft.SqlServer.Dac;
using Microsoft.SqlServer.Dac.Deployment;
using Microsoft.SqlServer.Dac.Model;
using System.Data.SqlClient;
using System.Data;
using NLog;
using Dapper;

namespace SmartBulkCopy
{
    public class ObjectDropInfo
    {
        public ExcludeObjects ObjectType;
        public TSqlObject Object;
        public string DropScript;
        public string CreateScript;
        public int DropOrder;
    }

    public class SchemaClone
    {
        SchemaCloneConfiguration _config;
        ILogger _log;         

        public SchemaClone(SchemaCloneConfiguration config, ILogger log)
        {
            _config = config;
            _log = log;
        }

        public int CloneDatabaseSchema()
        {                    
            var ms = new MemoryStream();

            var scs = new SqlConnectionStringBuilder(_config.SourceConnectionString);
            var dcs = new SqlConnectionStringBuilder(_config.DestinationConnectionString);
            
            if (_config.StopOn.HasFlag(StopOn.AnyUserObject))
            {
                _log.Info($"Checking for destination {dcs.DataSource}/{dcs.InitialCatalog} to be empty...");            

                if (!IsDatabaseEmpty(dcs.ConnectionString))
                {
                    _log.Error($"Destination database {dcs.DataSource}/{dcs.InitialCatalog} is not empty.");
                    _log.Info($"Stopped.");
                    return -1;
                }
            }            

            _log.Info($"Extracting database schema from {scs.DataSource}/{scs.InitialCatalog}...");            
            var dc = new DacServices(scs.ConnectionString);       
            dc.ProgressChanged += OnProgressChanged;
            dc.Message += OnMessage;
            dc.Extract(ms, scs.InitialCatalog, "SmartBulkCopy.SchemaClone", new Version(1,0));
            _log.Info($"Done.");            

            _log.Info($"Analyzing generated objects...");
            var dp = DacPackage.Load(ms);      
            var mlo = new ModelLoadOptions();            
            var model = TSqlModel.LoadFromDacpac(ms, mlo);            
            var excludedObjects = new List<ObjectDropInfo>();
            _log.Info($"Analyzing tables...");
            var tables = model.GetObjects(DacQueryScopes.Default, Table.TypeClass).ToList();
            excludedObjects.AddRange(GetObjectsToDrop(tables));
            _log.Info($"Analyzing views...");
            var views = model.GetObjects(DacQueryScopes.Default, View.TypeClass).ToList();
            excludedObjects.AddRange(GetObjectsToDrop(views));
            _log.Info($"Done.");            
            
            _log.Info($"Deploying schema to {dcs.DataSource}/{dcs.InitialCatalog}...");            
            var dc2 = new DacServices(dcs.ConnectionString);
            dc2.ProgressChanged += OnProgressChanged;
            dc2.Message += OnMessage;
                       
            var ddo = new DacDeployOptions();                    
            ddo.CreateNewDatabase = false;
            ddo.BlockOnPossibleDataLoss = true;          
            ddo.AllowIncompatiblePlatform = true;
            dc2.Deploy(dp, dcs.InitialCatalog, true, ddo);               

            _log.Info($"Removing excluded objects...");            
            using(var conn = new SqlConnection(dcs.ConnectionString))
            {
                foreach(var o in excludedObjects.OrderBy(o => o.DropOrder))
                {
                    _log.Info($"Dropping: {o.Object.Name}...");
                    _log.Debug(o.DropScript);
                    try {
                        conn.Execute(o.DropScript);    
                    } catch (SqlException)
                    {
                        _log.Error($"Cannot drop: {o.Object.Name}.");
                    }                    
                }
            }
            _log.Info($"Done.");  

            return 0;
        }

        private void OnProgressChanged(object sender, DacProgressEventArgs e)
        {
            _log.Info($"{e.Message}: {e.Status}");
        }

        private void OnMessage(object server, DacMessageEventArgs e)
        {
            _log.Info($"{e.Message}");
        }

        private List<ObjectDropInfo> GetObjectsToDrop(List<TSqlObject> tables)
        {
            var excludedObjects = new List<ObjectDropInfo>();

            foreach(var t in tables)
            {
                foreach (var c in t.GetChildren())
                {
                    if (c.ObjectType.Name != "Column")
                    {
                        if (
                                (c.ObjectType == Microsoft.SqlServer.Dac.Model.PrimaryKeyConstraint.TypeClass && _config.ExcludeObjects.HasFlag(ExcludeObjects.PrimaryKeys)) ||
                                (c.ObjectType == Microsoft.SqlServer.Dac.Model.UniqueConstraint.TypeClass && _config.ExcludeObjects.HasFlag(ExcludeObjects.UniqueKeys))                                
                            )
                        {                        
                            var cp = c.ObjectType.Properties.Where(p => p.Name == "Clustered").First();
                            var isClustered = cp.GetValue<bool>(c);
                            
                            string script = string.Empty;
                            if (c.TryGetScript(out script))
                            {
                                var odi = new ObjectDropInfo {
                                    Object = c,
                                    CreateScript = script,
                                    DropScript = $"ALTER TABLE {t.Name} DROP CONSTRAINT [{c.Name.Parts[1]}]",
                                    DropOrder = isClustered ? 100 : 10
                                };

                                excludedObjects.Add(odi);
                            }
                        }

                        if (
                                (c.ObjectType == Microsoft.SqlServer.Dac.Model.ForeignKeyConstraint.TypeClass && _config.ExcludeObjects.HasFlag(ExcludeObjects.ForeignKeys))
                            )
                        {                        
                            string script = string.Empty;
                            if (c.TryGetScript(out script))
                            {
                                var odi = new ObjectDropInfo {
                                    Object = c,
                                    CreateScript = script,
                                    DropScript = $"ALTER TABLE {t.Name} DROP CONSTRAINT [{c.Name.Parts[1]}]",
                                    DropOrder = 1
                                };

                                excludedObjects.Add(odi);
                            }
                        }                                    

                        if (c.ObjectType == Microsoft.SqlServer.Dac.Model.Index.TypeClass)
                        {
                            var cp = c.ObjectType.Properties.Where(p => p.Name == "Clustered").First();
                            var isClustered = cp.GetValue<bool>(c);
                            
                            if (_config.ExcludeObjects.HasFlag(ExcludeObjects.RowstoreClusteredIndexes) && isClustered)                           
                            {
                                string script = string.Empty;
                                if (c.TryGetScript(out script))
                                {
                                    var odi = new ObjectDropInfo {
                                        Object = c,
                                        CreateScript = script,
                                        DropScript = $"DROP INDEX [{c.Name.Parts[2]}] ON {t.Name}",
                                        DropOrder = 100
                                    };

                                    excludedObjects.Add(odi);
                                }
                            }  

                            if (_config.ExcludeObjects.HasFlag(ExcludeObjects.RowstoreNonclusteredIndexes) & !isClustered)                           
                            {
                                string script = string.Empty;
                                if (c.TryGetScript(out script))
                                {
                                    var odi = new ObjectDropInfo {
                                        Object = c,
                                        CreateScript = script,
                                        DropScript = $"DROP INDEX [{c.Name.Parts[2]}] ON {t.Name}",
                                        DropOrder = 10
                                    };

                                    excludedObjects.Add(odi);
                                }
                            }  
                        }

                        if (c.ObjectType == Microsoft.SqlServer.Dac.Model.ColumnStoreIndex.TypeClass)
                        {
                            var cp = c.ObjectType.Properties.Where(p => p.Name == "Clustered").First();
                            var isClustered = cp.GetValue<bool>(c);
                            
                            if (_config.ExcludeObjects.HasFlag(ExcludeObjects.ColumnstoreClusteredIndexes) && isClustered)                           
                            {
                                string script = string.Empty;
                                if (c.TryGetScript(out script))
                                {
                                    var odi = new ObjectDropInfo {
                                        Object = c,
                                        CreateScript = script,
                                        DropScript = $"DROP INDEX [{c.Name.Parts[2]}] ON {t.Name}",
                                        DropOrder = 100
                                    };

                                    excludedObjects.Add(odi);
                                }
                            }  

                            if (_config.ExcludeObjects.HasFlag(ExcludeObjects.ColumnstoreNonclusteredIndexes) & !isClustered)                           
                            {
                                string script = string.Empty;
                                if (c.TryGetScript(out script))
                                {
                                    var odi = new ObjectDropInfo {
                                        Object = c,
                                        CreateScript = script,
                                        DropScript = $"DROP INDEX [{c.Name.Parts[2]}] ON {t.Name}",
                                        DropOrder = 10
                                    };

                                    excludedObjects.Add(odi);
                                }
                            } 
                        }
                    }               
                }
            }

            return excludedObjects;
        }
    
        private bool IsDatabaseEmpty(string connectionString)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                var result = conn.QueryFirstOrDefault<int>("select count(*) as objects_found from sys.objects where is_ms_shipped = 0");

                return result == 0;
            }
        }
    }
}
