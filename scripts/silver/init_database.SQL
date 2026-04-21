/*
================================
CREATE DATABASE AND SCHEMAS
================================
SCRIPT PURPOSE
  THIS SCRIPT CREATES DATABASE NAMED DATAWAREHOUSE AFTER CHECKING IF DATABSE IS ALREADY EXISTS.
  IF THE DATABASE EXISTS IT IS DROPPED AND RECREATED. 
  SRIPT SETS UP 3 SCHEMAS: 'bronze', 'silver' AND 'gold'.
================================================================================================
*/

USE master;
GO
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouse
END;
GO


CREATE DATABASE Datawarehouse;
USE Datawarehouse

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
