# Configuration Options

You can fine tune Smart Bulk Copy behavior with the configuration settings available in `option` section:

- [Parallelism](#parallelism)
- [Partitions](#partitions)
- [Batch Size](#batch-Size)
- [Table Truncation](#table-truncation)
- [Identity Sync](#identity-sync)
- [Safety Checks](#safety-checks)
- [Connection Resiliency](#connection-resiliency)
- [Command Timeout](#command-timeout)
- [Compatibility Mode](#compatibility-mode)

## Parallelism

`"tasks": 7`

Define how many parallel task will move data from source to destination. Smart Bulk Copy uses a Concurrent Queue behind the scenes that is filled will all the partition that must be copied. Then as many as `tasks` are created and each of of those will dequeue work as fast as possible. How many task you want or can have depends on how much network bandwidth you have and how much resources are available in the destination. Maximum value is 32.

## Partitions

`"logical-partitions": "auto"`

In case a table is not physically partitioned, this option is used to create the logical partitions as described before. As a general rule at least the same amount of tasks is recommended in order to maximize throughput, but you need to tune it depending on how big your table is and how fast your destination server can do the bulk load.
There are three values supported by `logical-partitions`:

- `"auto"`: will try to automatically set the correct number of logical partitions per table, taking into account both row count and table size
- `"8gb"`: a string with a number followed by `gb` will be interpreted as the maximum size, in GB, that you want the logical partitions to be. Usually big partitions size (up to 8gb) provides better throughput, but with unreliable network connections remember that in case of transaction failure, the entire partition needs be reloaded
- `7`: a number will be interpreted as the number of logical partition you want to create.

Logical partitions will always be rounded to the next odd number (as this will create a better distribution across all partitions)

## Batch Size

`"batch-size": 100000`

Batch size used by Bulk Insert API. More info here if you need it: [SQL Server 2016, Minimal logging and Impact of the Batchsize in bulk load operations](https://blogs.msdn.microsoft.com/sql_server_team/sql-server-2016-minimal-logging-and-impact-of-the-batchsize-in-bulk-load-operations/)

If you're unsure of what value you should use, leave the suggested 100000.

## Table Truncation

`"truncate-tables": true`

Instruct Smart Bulk Copy to truncate tables on the destination before loading them. This requires that destination table doesn't have any Foreign Key constraint: [TRUNCATE TABLE - Restrictions](https://docs.microsoft.com/en-us/sql/t-sql/statements/truncate-table-transact-sql?view=sql-server-2017#restrictions) 

## Identity Synchronization

`"sync-identity": false`

Introduced in **version 1.9.5**: If set to `true`, tells Smart Bulk Copy to make sure that IDENTITY values are synchronized from the source to the target database. This should already happen normally, so by default is set to `false`. If you have manually reseeded the IDENTITY values on the source table this option will make sure the same reseed will happen also on the target tables.

## Safety Checks

`"safe-check": "readonly"`

Check that source database is actually a database snapshot or that database is set to readonly mode. Using one of the two options is recommended to avoid data modification while copy is in progress as this can lead to inconsistencies. Supported values are `readonly` and `snapshot`. If you want to disable the safety check use `none`: disabling the security check is *not* recommended.

`"stop-if": {"secondary-indexes": true, "temporal-table": true}`

Introduced in **version 1.7.1** allows to set when Smart Bulk Copy should not even start the bulk copy process as some table in the destination are not set for maximum performance or 

The available options are:

`"secondary-indexes": true`

Stop Smart Bulk Copy if secondary indexes are detected on any table in the destination. Secondary indexes can slow down a lot the copy process and even cause deadlocks. Unless you have very strong reasons for not creating secondary indexes *after* bulk load has been done, keep this option to `true`

`"temporal-table": true`

If Smart Bulk Copy detects a Temporal Table on the destintation database, it will stop by default. If this options is set to `false` Smart Bulk Copy will automatically disable and re-enabled Temporal Tables in the destination. As this operation requires execution of an `ALTER TABLE`, make sure the used login accout has enough permission and that you are confident and happy of what happen behind the scenes: [DisableSystemVersioning()](https://github.com/yorek/smartbulkcopy/blob/47d0e2347e2e18d62eadbf7be2d9a809022ff787/SmartBulkCopy.cs#L373) and [EnableSystemVersioning()](https://github.com/yorek/smartbulkcopy/blob/47d0e2347e2e18d62eadbf7be2d9a809022ff787/SmartBulkCopy.cs#L386)

## Connection Resiliency

`"retry-connection": {"delay-increment": 10, "max-attempt": 5}`

Defines how many times an operation should be attempted if a disconnection is detected and how much time (in seconds) should pass between two retries. Delay is incremented by `delay-incremement` every time a new attempt is tried.

## Command Timeout

`"command-timeout": 5400`

Introduced in **version 1.9.4** allows to set the timeout, in seconds, for a command before an error is generated. Default is set to 90 minutes (as some operations, like re-enabling Temporal Tables, if data size is big, can take quite a long time.)

## Compatibility Mode

`"compatibility-mode": false`

Introduced in **version 1.9.8**. Do not use unless you encounter issues with regular code path. When moving XML tables with big XML documents, it can happen that a thread get locked in a deadlock. You'll observe insanely high ASYNC_NETWORK_IO wait types both on source and destination, transfer speed down to 0.0 Mb/Sec while Smart Bulk Copy is still running. If this happens (so far only one instance of this issue has been reported) you can set `compatibility-mode` to true, so that the non-async method `WriteToServer` will be used (instead of `WriteToServerAsync`). This does not suffer of the deadlock problem, but it also cannot be nicely cancelled and the exception management, and thus connection recovery, is more difficult. So use it only if you have issues with the regular code path.
