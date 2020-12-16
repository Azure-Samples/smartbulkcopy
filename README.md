---
page_type: sample
languages:
- tsql
- sql
- csharp
products:
- azure-sql-database
- sql-server
- azure-sql-managed-instance
- azure-sqlserver-vm
- azure
- dotnet
- dotnet-core
description: "Smart, High-Speed, Bulk Copy tool to move data from one Azure SQL or SQL Server database to another"
urlFragment: "smart-bulk-copy"
---

# Smart Bulk Copy

<!-- 
Guidelines on README format: https://review.docs.microsoft.com/help/onboard/admin/samples/concepts/readme-template?branch=master

Guidance on onboarding samples to docs.microsoft.com/samples: https://review.docs.microsoft.com/help/onboard/admin/samples/process/onboarding?branch=master

Taxonomies for products and languages: https://review.docs.microsoft.com/new-hope/information-architecture/metadata/taxonomies?branch=master
-->

![License](https://img.shields.io/badge/license-MIT-green.svg) ![Run Tests](https://github.com/yorek/smartbulkcopy/workflows/Run%20Tests/badge.svg) 

*Latest Stable Version: 1.9.4*

Smart, High-Speed, Bulk Copy tool to move data from one Azure SQL / SQL Server database to another. Smartly uses logical or physical partitions to maximize transfer speed using parallel copy tasks.

It can be also used to efficiently and quickly move data from two instances of SQL Server running in two different cloud providers or to move from on-premises to the cloud.

Smart Bulk Copy is also available as a [Docker Image](https://hub.docker.com/repository/docker/yorek/smartbulkcopy). To run Smart Bulk Copy via docker, you have to map a volume where the desired .config file can be found. For example (on Windows):

```
docker run -it -v c:\work\_git\smart-bulk-copy\client\configs:/app/client/configs yorek/smartbulkcopy:latest /app/client/configs/smartbulkcopy.config.json
```

## How it works

Smart Bulk Copy uses [Bulk Copy API](https://docs.microsoft.com/en-us/dotnet/api/system.data.sqlclient.sqlbulkcopy) with parallel tasks. A source table is split in partitions, and each partition is copied in parallel with others, up to a defined maximum, in order to use all the available network bandwidth and all the cloud or server resources available to minimize the load times.

### Partitioned Source Tables

When a source table is partitioned, it uses the physical partitions to execute several queries like the following:

```sql
SELECT * FROM <sourceTable> WHERE $partition.<partitionFunction>(<partitionColumn>) = <n>
```

Queries are executed in parallel to load, always in parallel, data into the destination table. `TABLOCK` options is used - when possible and needed - on the table to allow fully parallelizable bulk inserts. `ORDER` option is also used when possible to minimize the sort operations on the destination table, for example when inserting into a table with an existing clustered rowstore index.

### Non-Partitioned Source Tables

If a source table is not partitioned, then Smart Bulk Copy will use the `%%PhysLoc%%` virtual column to logically partition tables into non-overlapping partitions that can be safely read in parallel. `%%PhysLoc%%` is *not* documented, but more info are available here:

[Where is a record really located?](https://techcommunity.microsoft.com/t5/Premier-Field-Engineering/Where-is-a-record-really-located/ba-p/370972)

If the configuration file specifies a value greater than 1 for `logical-partitions` the following query will be used to read the logical partitions in parallel:

```sql
SELECT * FROM <sourceTable> WHERE ABS(CAST(%%PhysLoc%% AS BIGINT)) % <logical-partitions-count> = <n>
```

*PLEASE NOTE* that the physical position of a row may change at any time if there is any activity on the database (updates, index reorgs, etc...) so it is recommended that this approach is used only in three cases:

1. You're absolutely sure there is no activity of any kind on the source database, or
2. You're using a database snapshot as the source database
3. You're using a database set in READ_ONLY mode

## Heaps, Clustered Rowstores, Clustered Columnstores

From version 1.7 Smart Bulk Copy will smartly copy tables with no clustered index (heaps), and tables with clustered index (rowstore or columnstore it doesn't matter.)

Couple of notes for tables with Clustered Columnstore index:
- Smart Bulk Copy will always use a Batch Size of a minimum of 102400 rows, no matter what specified in the configuration as per [best practices](https://docs.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-data-loading-guidance?view=sql-server-ver15#plan-bulk-load-sizes-to-minimize-delta-rowgroups). If you have columnstore it is generally recommended to increase the value to 1048576 in order to maximize compression and [reduce number of rowgroups](https://docs.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-data-loading-guidance?view=sql-server-ver15#plan-bulk-load-sizes-to-minimize-delta-rowgroups).
- When copying a Columnstore table, you may see very low values (<20Mb/Sec) for the "Log Flush Speed". *This is correct and expected* as Columnstore is extremely compressed and thus the log generation rate (which is what is measured by the Log Flush Speed) is much lower than with Rowstore tables.

## How to use it

Download or clone the repository, make sure you have .NET Core 3.1 installed and then create a `smartbulkcopy.config` file from the provided `smartbulkcopy.config.template`. If you want to start right away just, provide source and destination connection strings and leave all the options as is. Make sure the source database is a database snapshot:

[Create a Database Snapshot](https://docs.microsoft.com/en-us/sql/relational-databases/databases/create-a-database-snapshot-transact-sql?view=sql-server-2017)

Or that the database is set to be in Read-Only mode:

[Setting the database to READ_ONLY](https://docs.microsoft.com/en-us/sql/t-sql/statements/alter-database-transact-sql-set-options?view=sql-server-2017#b-setting-the-database-to-read_only)

Then just run:

```bash
cd client
dotnet run
```

and Smart Bulk Copy will start to copy data from source database to destination database. Please keep in mind that *all destination tables will be truncated by default*. This means that Foreign key constraints must be dropped in the destination database before copying. Read more about `TRUNCATE TABLE` restrictions here: [TRUNCATE TABLE](https://docs.microsoft.com/en-us/sql/t-sql/statements/truncate-table-transact-sql?view=sql-server-2017#restrictions)

## Configuration Notes

Here's how you can change Smart Bulk Copy configuration to better suits your needs. Everything is conveniently found in `smartbulkcopy.config` file. Aside from the obvious source and destination connection strings, here's the configuration options you can use:

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

From **version 1.7.1** you can also specify tables to be included and excluded:

```
"tables": {
    "include": ["dbo.*"],
    "exclude": ["dbo.ORDERS"]
}
```

### Configuration Options

Smart Bulk Copy is highly configurable. Read more in the dedicated document: [Smart Bulk Copy Configuration Options](./docs/CONFIG.md)

## Notes on Azure SQL

Azure SQL is log-rated as described in [Transaction Log Rate Governance](https://docs.microsoft.com/en-us/azure/sql-database/sql-database-resource-limits-database-server#transaction-log-rate-governance) and it can do 96 MB/sec of log flushing. Smart Bulk Load will report the detected log flush speed every 5 seconds so that you can check if you can actually increase the number of parallel task to go faster or you're already at the limit. Please remember that 96 MB/Sec are done with higher SKU, so if you're already using 7 parallel tasks and you're not seeing something close to 96 MB/Sec please check that

1. You have enough network bandwidth (this should not be a problem if you're copying data from cloud to cloud)
2. You're not using some very low SKU (like [P1 or lower](https://docs.microsoft.com/en-us/azure/azure-sql/database/resource-limits-dtu-single-databases) or just [2 vCPU](https://docs.microsoft.com/en-us/azure/azure-sql/database/resource-limits-vcore-single-databases)). In this case move to an higher SKU for the bulk load duration. 

There are a couple of exceptions to what just described:
- [Azure SQL Hyperscale](https://docs.microsoft.com/en-us/azure/azure-sql/database/service-tier-hyperscale) always provides 100 MB/Sec of maximum log throughput, no matter the number of vCores. Of course, if using a small number of cores on Hyperscale, other factors (for example: sorting when inserting into a table with indexes) could come into play and prevent you to reach the mentioned 100 Mb/Sec. 
- [M-series](https://docs.microsoft.com/en-us/azure/azure-sql/database/service-tiers-vcore?tabs=azure-portal#m-series) that can do up to 256 MB/sec of log throughput.

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

As the document was getting longer and longer, it has been moved here: [Smart Bulk Copy FAQ](./docs/FAQ.md)

## Tests

This tool has been tested against the following sample database with success:

- TPC-H
- TPC-E
- AdventureWorks2012
- AdventureWorks2014
- AdventureWorksDW2012
- AdventureWorksDW2014

Note that Foreign Keys and Views were dropped from target table before starting bulk copy


