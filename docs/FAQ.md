# Frequently Asked Questions

Here's a list of common questions on how to use Smart Bulk Copy at its best.


## Is the physical location of a row really always the same in a Database Snapshot?

There is no official documentation, but from all my test the answer is YES. I've also included a test script that you can use to verify this. IF you discover something different please report it here. I used SQL Server 2017 to run my tests.

## How to generate destination database schema?

Smart Bulk Copy only copies data between existing database and existing objects. It will NOT create database or tables for you. This allows you to have full control on how database and tables are created. If you are migrating your database and you'll like to have the schema automatically created for you, you can use one of the two following tool:

- [Database Migration Assistant](https://docs.microsoft.com/en-us/sql/dma/dma-overview)
- [sqlpackage](https://docs.microsoft.com/en-us/sql/tools/sqlpackage/sqlpackage)
- [mssql-scripter](https://github.com/microsoft/mssql-scripter)

## How can I be sure I'm moving data as fast as possible?

Remember that Azure SQL cannot go faster that ~100 MB/sec due to log flush speed governance. Best practices on how to quickly load data into a table can be found here:

- [Prerequisites for Minimal Logging in Bulk Import](https://docs.microsoft.com/en-us/sql/relational-databases/import-export/prerequisites-for-minimal-logging-in-bulk-import?view=sql-server-2017)
- Old but still applicable: [The Data Loading Performance Guide](https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008/dd425070(v=sql.100)?redirectedfrom=MSDN)

In summary, before starting the copy process, make sure that, for the tables that will be copied:

- Tables must be empty
- Drop any Foreign Key Constraint 
- Drop any Secondary (Non-Clustered) Index 

From version 1.7 if you have a table with a clustered index on it, Smart Bulk Copy will try to copy it as fast as possible, using partitioning if available and ORDER hint to avoid unnecessary sort. Here's how Smart Bulk Copy will try to load tables based on indexes and partitioning:

**Non-Partitioned Tables**
- *HEAP Table*: Parallel Bulk Load using Logical Partitions
- *Table with CLUSTERED ROWSTORE*: Single Bulk Load, Ordered by Index Key Columns
- *Table with CLUSTERED COLUMNSTORE*: Parallel Bulk Load using Logical Partitions

**Partitioned Tables**
- *HEAP Table*: Parallel Bulk Load using Physical Partitions
- *Table with CLUSTERED ROWSTORE*: Parallel Bulk Load using Physical Partitions, Ordered by Index Key Columns
- *Table with CLUSTERED COLUMNSTORE*: Parallel Bulk Load using Physical Partitions

Recreate Foreign Key constraints and indexes after the data has been copied successfully.

## I have a huge database and recreating indexes could be a challenge

If you have a huge table you may want to bulk load data WITHOUT removing the Clustered Index so to avoid the need to recreate it once on Azure SQL. If the table is really big (for example, 500GB size and more) rebuilding the Clustered Index could be a very resource and time-consuming operation. In such case you may want to keep the clustered index. From version 1.7 Smart Bulk Copy will allow you to do that. 

## Temporal Tables Support

From version 1.7.1 Smart Bulk Copy can detect and handle Temporal Tables. When a temporal table is detected on the destination database, it will be disabled to allow bulk insert. 

```sql
ALTER TABLE <schema>.<table> DROP PERIOD FOR SYSTEM_TIME;
ALTER TABLE <schema>.<table> SET (SYSTEM_VERSIONING = OFF);
```

After the Smart Bulk Copy process has finished, the Temporal Table support will be re-enabled

```sql
ALTER TABLE <schema>.<table> 
ADD PERIOD FOR SYSTEM_TIME (<period_start_column>, <period_end_column>);

ALTER TABLE <schema>.<table>
SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = <schema>.<history_table>));
```

If you prefer to handle this process manually, or if Smart Bulk Copy was stopped via a double `Ctrl-C` while copying the table, you may need to manually re-enable the temporal support. In any case, please read the following links:

[Alter non-temporal table to be a system-versioned temporal table](https://docs.microsoft.com/en-us/sql/relational-databases/tables/creating-a-system-versioned-temporal-table?view=sql-server-ver15#alter-non-temporal-table-to-be-a-system-versioned-temporal-table)

## Azure SQL / SQL Server Data Types support

In Azure SQL / SQL Server you can use `HiearchyId`, `Geography` and `Geometry` data types. Those data types are implemented as SQLCLR data types, residing in the Microsoft.SqlServer.Types assembly. Those types are 100% compatible with the System.Data.SqlClient library, [which is being replaced by](https://devblogs.microsoft.com/dotnet/introducing-the-new-microsoftdatasqlclient/) the new Microsoft.Data.SqlClient, that supports also .NET Core.

Smart Bulk Copy has been lately updated to use the latest version of Microsoft.Data.SqlClient - 2.0.1 at time of writing - which add support for the ORDER hint in Bulk Load, but that unfortunately completely [broke support to Microsoft.SqlServer.Types](https://github.com/dotnet/SqlClient/issues/30).

Luckily, for doing a Bulk Load, the only thing really needed to move data stored in those columns is the ability to Serialize and De-Serialize binary data, via the [IBinarySerialize](https://docs.microsoft.com/en-us/dotnet/api/microsoft.sqlserver.server.ibinaryserialize) interface which is already available in [Microsoft.Data.SqlClient.Server](https://github.com/dotnet/SqlClient/blob/0d4c9bb3d55e096a0f3196565b2c786a9125aaf8/src/Microsoft.Data.SqlClient/netcore/ref/Microsoft.Data.SqlClient.cs#L1500).

This means it is possible to create a custom, unofficial, implementation on the above types, that only focus on serialization and de-serialization, just to allow Bulk Load to work. Thanks to Assembly redirection, every time a method tries to access Microsoft.SqlServer.Types it can be redirected to the custom-made implementation, so that everything can the BulkCopy object can work without errors. 

From version 1.9.1 of Smart Bulk Copy, this has been done. You can see the implementation of the faked types in the `hack/` folder. Please DO NOT USE THAT ASSEMBLY IN ANY OTHER PROJECT, otherwise it is very likely that you'll get errors. I have extensively tested the implementation, but is really an hack, so do a random check of your data once it has move to the destination table, just as an additional precaution.

## I would change the code here and there, can I?

Sure, feel free to contribute! I created this tool just with the goal to get the job done in the easiest way possible. I tried to apply some of the well-known best practices, but in general I've followed the KISS principle by favoring simplicity over everything else. 