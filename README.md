# Smart Bulk Copy

Smart, High-Speed, Bulk Copy tool to move data from one Azure SQL / SQL Server database to another. Smartly uses logical or physical partitions to maximize transfer speed using parallel copy tasks.

It can be used to efficiently and quickly move data from two instances of SQL Server running in two different cloud providers or to move from on-premises to the cloud.

## How it works

Smart Bulk Copy uses [Bulk Copy API](https://docs.microsoft.com/en-us/dotnet/api/system.data.sqlclient.sqlbulkcopy) with parallel tasks. A source table is split in partitions, and each partition is copied in parallel with others, up to a defined maxium, in order to use all the available bandwidth and all the cloud or server resources available to minimize the load times.

### Partitioned Source Tables

When a source table is partitioned, it uses the physical partitions to execute several queries like the following:

```sql
SELECT * FROM <sourceTable> WHERE $partition.<partitionFunction>(<partitionColumn>) = <n>
```

Queries are executed in parallel to load, always in parallel, data into the destination table. `TABLOCK` options is used - when possible - on the table to allow fully parallelizable bulk inserts. `ORDER` option is also used when possibile to minimize the sort operations on the destination table, when insert into a table with an existing clustered rowstore index.

### Non-Partitioned Source Tables

If a source table is not partitioned, then Smart Bulk Copy will use the `%%PhysLoc%%` virtual column to logically partition tables into non-overlapping partitions that can be safely read in parallel. `%%PhysLoc%%` is *not* documented, but more info are available here:

[Where is a record really located?](https://techcommunity.microsoft.com/t5/Premier-Field-Engineering/Where-is-a-record-really-located/ba-p/370972)

If the configuration file specifies a value greater than 1 for `logical-partitions` the following query will be used to read the logical partition in parallel:

```sql
SELECT * FROM <sourceTable> WHERE ABS(CAST(%%PhysLoc%% AS BIGINT)) % <logical-partitions-count> = <n>
```

*PLEASE NOTE* that the physical position of a row may change at any time if there is any activity on the database (updates, index reorgs, etc...) so it is recommended that this approach is used only in three cases:

1. You're absolutely sure there is no activity of any kind on the source database, or
2. You're using a database snapshot as the source database
3. You're using a database set in READ_ONLY mode

## Heaps, Clustered Rowstores, Clustered Columnstores

From version 1.7 Smart Bulk Copy will smartly copy tables with no clustered index (heaps), and tables with clustered index (rowstore or columnstore it does't matter.)

Couple of notes for the Columnstore:
- Smart Bulk Copy will always use a Batch Size of 1048576 rows, no matter what specified in the configuration, in order to maximize compression and reduce number of rowgroups, as per [best pratices](https://docs.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-data-loading-guidance?view=sql-server-ver15#plan-bulk-load-sizes-to-minimize-delta-rowgroups).
- When copying a Columnstore table, you may see very low values (<20Mb/Sec) for the "Log Flush Speed". *This is correct and expected* as Columnstore is extremely compressed and thus the log generation rate (which is what is measured by the Log Flush Speed) is much lower than with Rowstore tables.

## How to use it

Download or clone the repository, make sure you have .NET Core 3.1 installed and then create a `smartbulkcopy.config` file from the provided `smartbulkcopy.config.template`. If you want to start right away just provide source and destination connection strings and leave all the options as is. Make sure the source database is a database snapshot:

[Create a Database Snapshot](https://docs.microsoft.com/en-us/sql/relational-databases/databases/create-a-database-snapshot-transact-sql?view=sql-server-2017)

Or that the database is set to be in Read-Only mode:

[Setting the database to READ_ONLY](https://docs.microsoft.com/en-us/sql/t-sql/statements/alter-database-transact-sql-set-options?view=sql-server-2017#b-setting-the-database-to-read_only)

Then just run:

```bash
dotnet run
```

and Smart Bulk Copy will start to copy data from source database to destination database. Please keep in mind that *all destination tables will be truncated by default*. This means that Foreign key constraints must be dropped in the destination database before copying. Read more about `TRUNCATE TABLE` restrictions here: [TRUNCATE TABLE](https://docs.microsoft.com/en-us/sql/t-sql/statements/truncate-table-transact-sql?view=sql-server-2017#restrictions)

## Configuration Notes

Here's how you can change Smart Bulk Copy configuration to better suits your needs. Everything is conviniently found in `smartbulkcopy.config` file. Aside from the obvious source and destination connection strings, here's the configuration options you can use:

### Tables to copy

`tables`: an array of string values the contains the two-part names of the table you want to copy. For example:

```
'tables': ['dbo.Table1', 'Sales.Customers']
```

An asterisk `*` will be expanded to all tables available in the source database:

```
'tables': ['*']
```

You can use schema to limit the wildcard scope:

```
'tables': ['dbo.*']
```

#### Copy behaviour

You can fine tune Smart Bulk Copy behaviour with the configuration settings available in `option` secion:

`"tasks": 7`

Define how many parallel task will move data from source to destination. Smart Bulk Copy uses a Concurrent Queue behind the scenes that is filled will all the partition that must be copied. Then as many as `tasks` are created and each of of those will dequeue work as fast as possibile. How many task you want or can have depends on how much bandwidth you have and how much resources are available in the destination. Maximum value is 32.

`"logical-partitions": "auto"`

In case a table is not physically partitioned, this option is used to create the logical partitions as described before. As a general rule at least the same amout of tasks is recommended in order to maximize throughput, but you need to tune it depending on how big your table is and how fast your destination server can do the bulk load.
There are three values supported by `logical-partitions`:

- `"auto"`: will try to automatically set the correct number of logical partitions per table, taking into account both row count and table size
- `"8gb"`: a string with a number followed by `gb` will be interpreted as the maximum size, in GB, that you want the logical partitions to be. Usually big partitions size (up to 8gb) provides better throughput, but with unreliable network connections remember that in case of transaction failure, the entire partition needs be reloaded
- `7`: a number will be interpreted as the number of logical partition you want to create.

Logical partitions will always be rounded to the next odd number (as this will create a better distribution across all partitions)

`"batch-size": 100000`

Batch size used by Bulk Insert API. More info here if you need it: [SQL Server 2016, Minimal logging and Impact of the Batchsize in bulk load operations](https://blogs.msdn.microsoft.com/sql_server_team/sql-server-2016-minimal-logging-and-impact-of-the-batchsize-in-bulk-load-operations/)

If you're unsure of what value you should use, leave the suggested 100000.

`"truncate-tables": true`

Instruct Smart Bulk Copy to truncate tables on the destination before loading them. This requires that destination table doesn't have any Foreign Key constraint: [TRUNCATE TABLE - Restrictions](https://docs.microsoft.com/en-us/sql/t-sql/statements/truncate-table-transact-sql?view=sql-server-2017#restrictions) 

`"safe-check": "readonly"`

Check that source database is actually a database snapshot or that database is set to readonly mode. Using one of the two options is recommended to avoid data modification while copy is in progress as this can lead to inconsistencies. Supported values are `readonly` and `snapshot`. If you want to disable the safety check use `none`: disabling the security check is *not* recommendded.

## Notes on Azure SQL

Azure SQL is log-rated as described in [Transaction Log Rate Governance](https://docs.microsoft.com/en-us/azure/sql-database/sql-database-resource-limits-database-server#transaction-log-rate-governance) and it can do 96 MB/sec of log flushing. Smart Bulk Load will report the detected log flush speed every 5 seconds so that you can check if you can actually increase the number of parallel task to go faster or you're already at the limit. Please remember that 96 MB/Sec are done with higher SKU, so if you're already using 7 parallel tasks and you're not seeing something close to 96 MB/Sec please check that

1. You have enough bandwidth (this should not be a problem if you're copying data from cloud to cloud)
2. You're not using some very low SKU (like P1 or lower or just 2 vCPU). In this case move to an higher SKU for the bulk load duration. 

An exception to what said is the Azure SQL Hyperscale SKU always provide 100 MB/Sec of maximum log throughput, no matter the number of vCores.

## Observed Performances

Tests have been ran using the `LINEITEM` table of TPC-H 10GB test database. Uncompressed table size is around 8.8 GB with 59,986,052 rows. Source database was a SQL Server 2017 VM running on Azure and the target was Azure SQL Hyperscale Gen8 8vCores. Smart Bulk Copy was running on the same Virtual Machine where also source database was hosted. Both the VM and the Azure SQL database were in the same region. 
Used configuration settings:
```json
"tasks": 7,
"logical-partitions": "auto",
"batch-size": 100000
```

Here's the result of the tests:

|Table|Copy Time (in sec)|
|---|---|
|HEAP|135 |
|HEAP, PARTITIONED|**111**|
|CLUSTERED ROWSTORE|505|
|CLUSTERED ROWSTORE, PARTITIONED |207|
|CLUSTERED COLUMNSTORE|315|
|CLUSTERED COLUMNSTORE, PARTITIONED |196|

## Questions and Answers

### Is the physical location of a row really always the same in a Database Snapshot?

There is no official documentation, but from all my test the answer is YES. I've also included a test script that you can use to verify this. IF you discover something different please report it here. I used SQL Server 2017 to run my tests.

### How to generate destination database schema?

Smart Bulk Copy only copies data between existing database and existings objects. It will NOT create database or tables for you. This allows you to have full control on how database and tables are created. If you are migrating your database and you'll like to have the schema automatically created for you, you can use one of the two following tool:

- [Database Migration Assistant](https://docs.microsoft.com/en-us/sql/dma/dma-overview?view=sql-server-2017)
- [mssql-scripter](https://github.com/microsoft/mssql-scripter)

### How can I be sure I'm moving data as fast as possible?

Remember that Azure SQL cannot go faster that ~100 MB/sec due to log rate governance. The best practices to quickly load data into a table can be found here:

- [Prerequisites for Minimal Logging in Bulk Import](https://docs.microsoft.com/en-us/sql/relational-databases/import-export/prerequisites-for-minimal-logging-in-bulk-import?view=sql-server-2017)
- Old but still applicable: [The Data Loading Performance Guide](https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008/dd425070(v=sql.100)?redirectedfrom=MSDN)

In summary, before starting the copy process, make sure that, for the table that will be copied:

- Tables must be empty
- Drop any Foreign Key Constraint 
- Drop any Secondary (Non-Clustered) Index 

From version 1.7 if you have a table with a clustered index on it, Smart Bulk Copy will try to copy it as fast as possible, using partitioning if possible and ORDER hint to avoid unnecessary sort. Here's how Smart Bulk Copy will try to load tables based on indexes and partitioning:

**Non-Partitioned Tables**
- *HEAP Table*: Parallel Bulk Load using Logical Partitions
- *Table with CLUSTERED ROWSTORE*: Single Bulk Load, Ordered by Index Key Columns
- *Table with CLUSTERED COLUMNSTORE*: Parallel Bulk Load using Logical Partitions

**Partitioned Tables**
- *HEAP Table*: Parallel Bulk Load using Logical Partitions
- *Table with CLUSTERED ROWSTORE*: Parallel Bulk Load using Physical Partitions, Ordered by Index Key Columns
- *Table with CLUSTERED COLUMNSTORE*: Parallel Bulk Load using Physical Partitions

Recreate Foreign Key constraints and indexes after the data has been copied successfully.

### I would change the code here and there, can I?

Sure feel free to contribute! I created this tool just with the goal to get the job done in the easiest way possibile. Code can be largely improved even, if I tried to apply some of the best practies, but when I had to make some choice I chose simplicity over everything else.

## Tests

This tool has been tested agains the following sample database with success:

- TPC-H (1)
- TPC-E (1)
- AdventureWorks2012 (1)
- AdventureWorks2014 (1)
- AdventureWorksDW2012 (1)
- AdventureWorksDW2012 (1)

(1):Foreign Keys and Views *must* be dropped from target table before starting bulk copy


