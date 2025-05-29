if exists (select 1 from sys.databases where name='DWH');

begin
	alter database DWH set single_user with rollback immediate;
	drop database DWH
end;

create database DWH;


create schema bronze;
create schema silver;
create schema gold;
