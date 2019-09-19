# Smart Bulk Copy

Smart, High-Speed, Bulk Copy tool to move data from one Azure SQL / SQL Server database to another. Smartly uses logical or physical partitions to maximize transfer speed using parallel copy tasks.

It can be used ot efficiently and quickly move data from two instances of SQL Server running in two different cloud providers or to move from on-premises to the cloud.

## How it works

Smart Bulk Copy usese [Bulk Copy API](https://docs.microsoft.com/en-us/dotnet/api/system.data.sqlclient.sqlbulkcopy) with parallel tasks. A source table is split in partitions, and each parition is copied in parallel with other, up to a defined maxium, in order to use all the available bandwidth and all the cloud or server resources available to minimize the load times.

### Partitioned Source Tables

When a source table is partitioned, it uses the physical partitions to execute several queries like the following:

```sql
SELECT * FROM <sourceTable> WHERE $partition.<partitionFunction>(<partitionColumn>) = <n>
```

in parallel and to load, in parallel, data into the destination table. `TABLOCK` options is used on the table to allow fully parallelizable bulk inserts.

### Non-Partitioned Source Tables

If a source table is not partitioned, then Smart Bulk Insert will use the `%%PhysLoc%%` virtual column to logically partition tables into non-overlapping partitions that can be safely read in parallel. `%%PhysLoc%%` is *not* documented, but more info are available here:

[Where is a record really located?](https://techcommunity.microsoft.com/t5/Premier-Field-Engineering/Where-is-a-record-really-located/ba-p/370972)

If the configuration file specify a value greater than 1 for `logical-partitions` the following query will be used to read the logical partition in parallel:

```sql
SELECT * FROM <sourceTable> WHERE ABS(CAST(%%PhysLoc%% AS BIGINT)) % <logical-partitions-count> = <n>
```

*PLEASE NOTE* that the physical position of a row may change at any time if there is any activity on the database (updates, index reorgs) so it is recommended that this approach is used only in two cases:

1. You're absolutely sure there is no activity of any kind on the source database, or
2. You're using a database snapshot as the source database

## How to use it

Download or clone the repository, make sure you have .NET Core 2.1 installed and then create a `smartbulkcopy.config` file from the provided `smartbulkcopy.config.template`. If you want to start right away just provide source and destination connection strings and leave all the options as is. Make sure the source database is a database snapshot:

[Create a Database Snapshot](https://docs.microsoft.com/en-us/sql/relational-databases/databases/create-a-database-snapshot-transact-sql?view=sql-server-2017)

Then just run:

```bash
dotnet run
```

and Smart Bulk Copy will start to copy data from source database to destination database. Please keep in mind that *all destination tables will be truncated by default*.

## Configuration Notes

Here's how you can change Smart Bulk Copy configuration to better suits your needs. Everything is convinently found in `smartbulkcopy.config` file. Aside from the obvious source and destination connection strings, here the configuration option you can use:

### Tables to copy

`tables`: an array of string values the contains the two-part names of the table you want to copy. For example:

```
'tables': ['dbo.Table1', 'Sales.Customers']
```

An asterisk `*` will be expanded to all tables available in the source database:

```
'tables': ['*']
```

#### Copy behaviour

You can fine tune Smart Bulk copy behaviour with the configuration settings available in `option` secion:

`"tasks": 7`

Define how many parallel task will move data from source to destination. Smart Bulk Copy usesa Concurrent Queue behind the scenes that is filled will all the partition that must be copied. Then as many as `tasks` are created and each of of those will dequeue work as fast as possibile. How many task you want or can have depends on how much bandwidth you have and how much resources are available in the destination. Maximum value is 32.

`"logical-partitions": 7`

In case a table is not physically partitioned, this number is used to create the logical partitions as described before. As a general rule tt least the same amout of tasks is recommended in order to maximize throughput, but you need to tune it depending on how big your table is and how fast your destination server can do the bulk load.

`"batch-size": 100000`

Batch size used by Bulk Insert API. More info here if you need it: [SQL Server 2016, Minimal logging and Impact of the Batchsize in bulk load operations](https://blogs.msdn.microsoft.com/sql_server_team/sql-server-2016-minimal-logging-and-impact-of-the-batchsize-in-bulk-load-operations/)

If you're unsure of what value you should use, leave the suggested 100000.

`"truncate-tables": true`

Instruct Smart Bulk Coy to truncate tables on the destination before loading them.

`"safe-check": "readonly"`

Check that source database is actually a database snapshot or that database is set to readonly mode. Using one of the two options is recommended to avoid data modification while copy is in progress as this can lead to inconsistencies. Supported values are `readonly` and `snapshot`. If you want to disable the safety check use `none`: disabling the security check is *not* recommendded.

## Notes on Azure SQL

Azure SQL is log-rated as described in [Transaction Log Rate Governance](https://docs.microsoft.com/en-us/azure/sql-database/sql-database-resource-limits-database-server#transaction-log-rate-governance) and it can do 96 MB/sec of log flushing. Smart Bulk Load will report the detected log flush speed every 5 seconds so that you can check if you can actually increase the number of parallel task to go faster or you're already at the limit. Please remember that 96 MB/Sec are done with higher SKU, so if you're already using 7 parallel tasks and you
re not seeing something close to 96 MB/Sec please check that

1. You have enough bandwidth (this should not be a problem if you're copying data from cloud to cloud)
2. You're not using some very low SKU (like P1 or lower or just 2 vCPU). In this case move to an higher SKU for the bulk load duration.

## Questions and Answers

### Is the physical location of a row really always the same in a Database Snapshot

There is on official documentation, but from all my test the answer is YES. I've also included a test script that you can use to verify this. IF you discover something different please report it here. I used SQL Server 2017 to run my tests.

### I would change the code here and there

Sure feel free to contribute! I created this tool just with the goal to get the job done in the easiest way possibile. Code can be largely improved even, if I tried to apply some of the best practies, but when I had to make some choice I chose simplicity over everything else.
