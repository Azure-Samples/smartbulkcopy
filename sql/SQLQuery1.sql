CREATE SCHEMA [schema1]
GO
CREATE SCHEMA [schema2]
GO
CREATE SCHEMA [schema3]
GO
CREATE SCHEMA [schema4]
GO
CREATE SCHEMA [schema5]
GO
CREATE SCHEMA [schema6]
GO
CREATE SCHEMA [schema7]
GO
CREATE SCHEMA [schema8]
GO
CREATE SCHEMA [schema9]
GO

DROP TABLE IF EXISTS [schema1].[heap];
CREATE TABLE [schema1].[heap]
(
	id INT IDENTITY NOT NULL,
	col1 VARCHAR(100) NOT NULL,
	col3 VARCHAR(100) NULL,
	col2 NVARCHAR(100) NOT NULL,
	col4 NVARCHAR(100) NULL,
	col5 TINYINT NOT NULL,
	col6 TINYINT NULL,
	col7 SMALLINT NOT NULL,
	col8 SMALLINT NULL,
	col9 INT NOT NULL,
	col10 INT NULL,
	col11 BIGINT NOT NULL,
	col12 BIGINT NULL,
	col13 SMALLDATETIME NOT NULL,
	col14 SMALLDATETIME NULL,
	col15 DATETIME NOT NULL,
	col16 DATETIME NULL,
	col17 DATE NOT NULL,
	col18 DATE NULL,
	col19 TIME(7) NOT NULL,
	col20 TIME(7)  NULL,
	col21 DATETIME2(7) NOT NULL,
	col22 DATETIME2(7) NULL,
	col23 DATETIMEOFFSET NOT NULL,
	col24 DATETIMEOFFSET NULL,
	col25 SMALLMONEY NOT NULL,
	col26 SMALLMONEY NULL,
	col27 MONEY NOT NULL,
	col28 MONEY NULL,
	col29 DECIMAL(18,8) NOT NULL,
	col30 DECIMAL(18,8) NULL,
	col31 FLOAT NOT NULL,
	col32 FLOAT NULL,
	col33 TIMESTAMP NOT NULL
)
GO

SELECT * INTO [schema1].[clustered_columnstore] FROM [schema1].[heap]
SELECT * INTO [schema1].[clustered_rowstore] FROM [schema1].[heap]
SELECT * INTO [schema1].[mix] FROM [schema1].[heap]
SELECT * INTO [schema1].[nonclustered_columnstore] FROM [schema1].[heap]
SELECT * INTO [schema1].[nonclustered_rowstore]	FROM [schema1].[heap]
SELECT * INTO [schema1].[partitioned_clustered_columnstore]	FROM [schema1].[heap]
SELECT * INTO [schema1].[partitioned_clustered_rowstore] FROM [schema1].[heap]
SELECT * INTO [schema1].[partitioned_mix] FROM [schema1].[heap]
SELECT * INTO [schema1].[partitioned_nonclustered_columnstore] FROM [schema1].[heap]
SELECT * INTO [schema1].[partitioned_nonclustered_rowstore] FROM [schema1].[heap]
SELECT * INTO [schema1].[partitioned_heap] FROM [schema1].[heap]
go

CREATE PARTITION FUNCTION pf_dummy (int)  
AS RANGE LEFT FOR VALUES (1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000);
GO

CREATE PARTITION SCHEME ps_dummy
AS PARTITION pf_dummy
ALL TO ([Primary])
GO

CREATE CLUSTERED INDEX ixc_dummy ON [schema1].partitioned_heap (id) ON ps_dummy(id)
GO
DROP INDEX ixc_dummy ON [schema1].partitioned_heap 
GO

CREATE CLUSTERED INDEX ixc_dummy ON [schema1].partitioned_clustered_rowstore (col17, col19) ON ps_dummy(id)
GO

CREATE CLUSTERED INDEX ixc_dummy ON [schema1].clustered_rowstore (col17, col19) 
GO

ALTER TABLE [schema1].partitioned_clustered_columnstore
DROP COLUMN col33;
GO

CREATE CLUSTERED INDEX ixc_dummy ON [schema1].partitioned_clustered_columnstore (id) ON ps_dummy(id)
GO
DROP INDEX ixc_dummy ON [schema1].partitioned_clustered_columnstore
GO
CREATE CLUSTERED COLUMNSTORE INDEX ixcc_dummy ON [schema1].partitioned_clustered_columnstore ON ps_dummy(id)
GO

ALTER TABLE [schema1].clustered_columnstore
DROP COLUMN col33;
GO

CREATE CLUSTERED COLUMNSTORE INDEX ixcc_dummy ON [schema1].clustered_columnstore
GO


