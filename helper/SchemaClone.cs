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

            _log.Info($"Analyzing generated objects...");
            var dp = DacPackage.Load(ms);      
            var mlo = new ModelLoadOptions();            
            var model = TSqlModel.LoadFromDacpac(ms, mlo);            
            var tables = model.GetObjects(DacQueryScopes.Default, Table.TypeClass).ToList();
            var excludedObjects = GetObjectsToDrop(tables);

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

            return;


        }

        public void OnProgressChanged(object sender, DacProgressEventArgs e)
        {
            _log.Info($"{e.Message}: {e.Status}");
        }

        public void OnMessage(object server, DacMessageEventArgs e)
        {
            _log.Info($"{e.Message}");
        }

        public List<ObjectDropInfo> GetObjectsToDrop(List<TSqlObject> tables)
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
    }
}
