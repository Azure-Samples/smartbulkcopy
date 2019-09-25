using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using Dapper;
using NLog;
using System.Text.RegularExpressions;

namespace SmartBulkCopy
{    
    public static class SqlConnectionExtensions
    {
        static ILogger _logger = LogManager.GetCurrentClassLogger();

        // Added all error code listed here: "https://docs.microsoft.com/en-us/azure/sql-database/sql-database-develop-error-messages"
        // Added Error Code 0 to automatically handle killed connections
        // Added Error Code 4891 to automatically handle "Insert bulk failed due to a schema change of the target table" error
        // Added Error Code 10054 to handle "The network path was not found" error that could happen if connection is severed (e.g.: cable unplugged)
        // Added Error Code 53 to handle "No such host is known" error that could happen if connection is severed (e.g.: cable unplugged)
        // Added Error Code 11001 to handle transient network errors
        // Added Error Code 10065 to handle transient network errors
        // Added Error Code 10060 to handle transient network errors
        public static readonly List<int> TransientErrors = new List<int>() { 0, 4891, 10054, 4060, 40197, 40501, 40613, 49918, 49919, 49920, 10054, 53, 11001, 10065, 10060};        

        public static object TryExecuteScalar(this SqlConnection conn, string sql) {
            int attempts = 0;            
            int delay = 10;
            int waitTime = attempts * delay;

            object result = null;
            while (attempts < 5) {
                attempts += 1;
                try {
                    conn.TryOpen();
                    result = conn.ExecuteScalar(sql);                    
                    attempts = int.MaxValue;
                } 
                catch (SqlException se)
                {
                    if (TransientErrors.Contains(se.Number))
                    {                                    
                        waitTime = attempts * delay;

                        _logger.Warn($"[TryExecuteScalar]: Transient error while copying data. Waiting {waitTime} seconds and then trying again...");
                        _logger.Warn($"[TryExecuteScalar]: [{se.Number}] {se.Message}");

                        Task.Delay(waitTime * 1000).Wait();
                    } else {
                        _logger.Error($"[TryExecuteScalar]: [{se.Number}] {se.Message}");
                        throw;
                    }           
                } finally {
                    if (conn.State == ConnectionState.Open)
                        conn.Close();
                }
            }

            if (attempts != int.MaxValue) throw new ApplicationException("[TryExecuteScalar] Cannot open connection to SQL Server / Azure SQL");
            return result;
        }

        public static void TryOpen(this SqlConnection conn)
        {
            int attempts = 0;            
            int delay = 10;
            int waitTime = attempts * delay;

            while (attempts < 5) {
                attempts += 1;
                try {
                    conn.Open();
                    attempts = int.MaxValue;
                } 
                catch (SqlException se)
                {
                    if (TransientErrors.Contains(se.Number))
                    {                                    
                         waitTime = attempts * delay;

                        _logger.Warn($"[TryOpen]: Transient error while copying data. Waiting {waitTime} seconds and then trying again...");
                        _logger.Warn($"[TryOpen]: [{se.Number}] {se.Message}");

                        Task.Delay(waitTime * 1000).Wait();
                    } else {
                        _logger.Error($"[TryOpen]: [{se.Number}] {se.Message}");
                        throw;
                    }           
                }
            }

            if (attempts != int.MaxValue) throw new ApplicationException("[TryOpen] Cannot open connection to SQL Server / Azure SQL");
        }
    }
}