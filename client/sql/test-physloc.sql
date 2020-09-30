-- cleanup if we are running it second tinme
use master
go
drop database if exists PhysLocTestSnapshot
go
drop database if exists PhysLocTest;
go

create database PhysLocTest
go

use PhysLocTest
go

drop table if exists PhysLocTestTable;
go

create table PhysLocTestTable 
(
	id int not null,
	longtext varchar(4000) not null
)
go

-- All rows will fit in one table
insert into dbo.PhysLocTestTable (id, longtext) values 
(1, REPLICATE('A', 1000)),
(2, REPLICATE('B', 1000)),
(3, REPLICATE('C', 1000)),
(5, REPLICATE('E', 1000)),
(6, REPLICATE('F', 2000)),
(7, REPLICATE('G', 2000)),
(4, REPLICATE('D', 1000))
go

drop database if exists PhysLocTestSnapshot
go

select * from sys.database_files 
go

-- read path dynamically to create snapshot.
declare @path nvarchar(520)
declare @CreateDB_str nvarchar(4000)
select @path = LEFT(physical_name, Len(physical_name) - Charindex('\', Reverse(physical_name))) from sys.database_files  where type = 0
select @CreateDB_str =
'create database PhysLocTestSnapshot on
(name = ''PhysLocTest'', FILENAME = '''+ @path + '\PhysLocTest_mdf.ss1'')
as snapshot of PhysLocTest'
exec (@CreateDB_str)
go

-- Check rows location (File:Page:Slot)
select sys.fn_PhysLocFormatter(%%PhysLoc%%), * from dbo.PhysLocTestTable
select sys.fn_PhysLocFormatter(%%PhysLoc%%), * from PhysLocTestSnapshot.dbo.PhysLocTestTable

-- Update one row
update dbo.PhysLocTestTable set longtext = REPLICATE('C2', 2000) where id = 3

-- Check rows location (File:Page:Slot)
-- Row Id 3 is still in the same place due to the forwarding pointers feature
select sys.fn_PhysLocFormatter(%%PhysLoc%%), * from dbo.PhysLocTestTable
select sys.fn_PhysLocFormatter(%%PhysLoc%%), * from PhysLocTestSnapshot.dbo.PhysLocTestTable

-- Create a clustered index
alter table PhysLocTestTable 
add constraint pk 
primary key clustered (id)

-- Rows in live database have changeed positions 
-- but are still in the old in the Snapshot
select sys.fn_PhysLocFormatter(%%PhysLoc%%), * from dbo.PhysLocTestTable
select sys.fn_PhysLocFormatter(%%PhysLoc%%), * from PhysLocTestSnapshot.dbo.PhysLocTestTable

-- Update one row
update dbo.PhysLocTestTable set longtext = REPLICATE('D2', 2000) where id = 4

-- Again, rows in live database have changeed positions 
-- but are still in the old in the Snapshot
select sys.fn_PhysLocFormatter(%%PhysLoc%%), * from dbo.PhysLocTestTable
select sys.fn_PhysLocFormatter(%%PhysLoc%%), * from PhysLocTestSnapshot.dbo.PhysLocTestTable
