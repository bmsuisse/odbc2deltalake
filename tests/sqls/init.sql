drop table if exists dbo.[user2];
drop table if exists dbo.[user3];
drop table if exists dbo.[user3_];
drop table if exists dbo.[user4];
drop table if exists dbo.[user];
drop table if exists dbo.[company];
drop table if exists dbo.[company2];
drop table if exists [long schema].[long table name];
GO
create table dbo.[company](
        id varchar(10) collate Icelandic_100_CI_AI_SC primary key,
        name varchar(100),
        SysStartTime datetime2 GENERATED ALWAYS AS ROW START,
        SysEndTime datetime2 GENERATED ALWAYS AS ROW
    END,
    PERIOD FOR SYSTEM_TIME(SysStartTime, SysEndTime)
);
create table dbo.[company2](
        id varchar(10) collate Icelandic_100_CI_AI_SC primary key,
        name varchar(100),
        SysStartTime datetime2 GENERATED ALWAYS AS ROW START,
        SysEndTime datetime2 GENERATED ALWAYS AS ROW
    END,
    PERIOD FOR SYSTEM_TIME(SysStartTime, SysEndTime)
);
insert into dbo.[company](id, name)
select 'c1',
    'The First company';
insert into dbo.[company](id, name)
select 'c2',
    'The Second company';
create table dbo.[user](
    [User - iD] bigint primary key identity(1, 1),
    FirstName varchar(100),
    LastName nvarchar(max),
    Age decimal(15, 3),
    companyid varchar(10)  collate Icelandic_100_CI_AI_SC  not null references dbo.company(id),
    [time stämp] timestamp
);
create table dbo.[user2](
    [User - iD] bigint primary key identity(1, 1),
    FirstName varchar(100),
    LastName nvarchar(max),
    Age decimal(15, 3),
    companyid varchar(10) collate Icelandic_100_CI_AI_SC  not null references dbo.company(id),
    [time stämp] timestamp
);
create table dbo.[user3](
    [User - iD] bigint primary key identity(1, 1),
    FirstName varchar(100),
    LastName nvarchar(max),
    Age decimal(15, 3),
    companyid varchar(10)  collate Icelandic_100_CI_AI_SC not null references dbo.company(id),
    [time stämp] timestamp
);
create table dbo.[user4](
    [User - iD] bigint primary key identity(1, 1),
    FirstName varchar(100),
    LastName nvarchar(max),
    Age decimal(15, 3),
    companyid varchar(10)  collate Icelandic_100_CI_AI_SC not null references dbo.company(id),
    [time stämp] timestamp
);
insert into dbo.[user](FirstName, LastName, Age, companyid)
select *
FROM (
        VALUES('John', 'Anders', 14, 'c1'),
            ('Peter', 'Johniingham', 23, 'c1'),
            ('Petra', 'wayne', 24, 'c1')
    ) as x(fn, ln, a, ci);
;
insert into dbo.[user2](FirstName, LastName, Age, companyid)
select FirstName,
    LastName,
    Age,
    companyid
from dbo.[user];
insert into dbo.[user3](FirstName, LastName, Age, companyid)
select FirstName,
    LastName,
    Age,
    companyid
from dbo.[user];
insert into dbo.[user4](FirstName, LastName, Age, companyid)
select FirstName,
    LastName,
    Age,
    companyid
from dbo.[user];
IF NOT EXISTS(
    Select *
    from sys.schemas
    where name = 'long schema'
) begin exec sp_executesql N'CREATE SCHEMA [long schema]'
end;
CREATE TABLE [long schema].[long table name] (
    [long column name] int,
    dt xml,
    uid uniqueidentifier default newid(),
    [date] date
);
INSERT INTO [long schema].[long table name] ([long column name], dt, [date])
SELECT 1,
    '<root><child>text</child></root>',
    '2023-01-01'
union all
SELECT 2,
    '<root><child>text 2345asdf</child></root>',
    '2024-01-01';