USE master;


IF NOT EXISTS 
	(SELECT name FROM sys.databases WHERE name='GlobalRetail_DW')
BEGIN
	CREATE DATABASE GlobalRetail_DW;
END


USE GlobalRetail_DW;


--*********************************************************************

-- Schema for Staging

--check if schema exists

IF NOT EXISTS
 (SELECT * FROM sys.schemas WHERE name='Staging')
BEGIN
 EXEC('CREATE SCHEMA Staging');
 PRINT 'Created schema: Staging';
END
ELSE
 PRINT 'Schema already exists: Staging';

--*********************************************************************

--Step 2 - create the staging tables

-- Create the Staging.RawCustomers table using the same fields as the csv file, use nvarchar for all fields
-- Remember that staging tables do not contain pks and fks or other constraints

create table Staging.RawCustomers(
	CustomerID NVARCHAR(50),
	FirstName NVARCHAR(100),
	LastName NVARCHAR(100),
	BirthDate NVARCHAR(50),
	Region NVARCHAR(50),
	Country NVARCHAR(50),
	City NVARCHAR(50),
	Gender NVARCHAR(20),
	Occupation NVARCHAR(50),
	YearlyIncome NVARCHAR(50),
	SourceFileData NVARCHAR(50), --Helps to track data versions 
)


-- Create the Staging.RawProducts staging table
CREATE TABLE Staging.RawProducts (
 ProductID NVARCHAR(50),
 ProductName NVARCHAR(50),
 Brand NVARCHAR(50),
 Category NVARCHAR(50),
 SubCategory NVARCHAR(50),
 CurrentPrice NVARCHAR(50)
);

-- Create the Staging.RawSales staging table
CREATE TABLE Staging.RawSales (
 OrderID NVARCHAR(50),
 OrderDate NVARCHAR(50),
 ProductID NVARCHAR(50),
 CustomerID NVARCHAR(50),
 StoreID NVARCHAR(50),
 Quantity NVARCHAR(50),
 UnitPriceSold NVARCHAR(50),
 LineTotal NVARCHAR(50)
);

-- Create the Staging.RawStores staging table
CREATE TABLE Staging.RawStores (
 	StoreId NVARCHAR(50),
 	StoreName NVARCHAR(100),
 	City NVARCHAR(50),
 	Country NVARCHAR(50),
 	Region NVARCHAR(50),
 	Managername NVARCHAR(100),
 	
);

-- Create the Staging.RawReviews staging table
CREATE TABLE Staging.RawReviews (
 ReviewID NVARCHAR(50),
 ProductID NVARCHAR(50),
 UserID NVARCHAR(50),
 Rating NVARCHAR(50),
 ReviewDate NVARCHAR(50),
 Tag NVARCHAR(50)
);

-- Remember there is no need to create a staging table for date or time, these are automatically generated through code 

-- Step 3 - Create the ETL LOG Table - this table will store information about each process. this is not a staging table, use suitable constraints

Create Table dbo.ETLLog(
	LogID INT IDENTITY(1,1) PRIMARY KEY,
	ProcedureName NVARCHAR(100) NOT NULL,
	StartTime DATETIME NOT NULL,
	EndTime DATETIME NULL, -- filled in later
	RowsAffected INT NULL,
	STATUS NVARCHAR(50) NOT NULL, --Sucecess, Failure, Running
	ErrorMessage NVARCHAR(MAX) NULL,
	ExecutionTimeSeconds AS DATEDIFF(SECOND, StartTime, EndTime)
);

Create Table dbo.ETLConfig(
	ConfigKey NVARCHAR(50) Primary Key,
	ConfigValue NVARCHAR(200) NOT NULL,
	Description NVARCHAR(500) NULL,
	LastModified DATETIME DEFAULT GETDATE()
);






/*
Staging Tables to hold raw data before ETL processing
These are needed for:

1. Data Quality and Validation
- Inspect and validate incoming data without affecting production tables.
- Identify and handle anomalies, missing values, or inconsistencies.

2. Transformation Buffer
- Store raw data in its oroginal from source systems
- Apply transformations (cleansing, standardisation, business rules) before 
  loading to dimension / fact tables
- Seperate Extract, Transform, Load (ETL) processes for better maintainability

3. Performance and Optimisation
- Bulk load data quickly into staging without constraint, indexes or triggers
- Process transformations in batches without locking production tables
- Minimize impact on source systems by extracting once

4. Error Recovery and Debugging
- If a load fails, raw data remains in staging for reprocessing
- Easy to delete and reload without affecting data warehouse
- Audit trail of what data was received vs. what was loaded

5. Change Data Capture
- Compare staging data with existing data to identify:
  - New records to insert
  - Changed Records (updates for SCD-2)
  - Deleted records to remove
- Track SourceFileDate to distinguish between different data batches

6. Data Integration from Multiple Sources
- Standardize data from various source systems in staging
- Merge, deduplicate and consolidaate before loading into DW

Typical Flow:
Source Systems --> Staging Tables --> ETL Transformations --> DW Dimension/Fact Tables
*/









/*
ETL Support Tables (Logging And Configuration)
*/

/*
ETL Logging Table
Tracks and monitors ETL process execution

- Performance Monitoring
	- ExecutionTimeSeconds shows how long each ETL procedure takes
	- Identify bottlenecks and optimize slow processes
	- Track performance trends over time
- Error Tracking
	- Status field indicates Success, Failure, Running
	- ErrorMessage captures details of any failures for debugging
	- Helps in root cause analysis and resolution
- Audit Trail
	- Complete history of when each ETL process ran (StartTime, EndTime)
	- RowsAffected shows data volume processed
	- Prove compliance and data lineage for audits
- Operational Monitoring
	- Identify currently running processes (Status = 'Running')
	- Detect stuck or long-running jobs for intervention

*/







/* 
ETL Configuration Table
Centralise ETL settings and parameters

- Flexibility when Code Changes
	- Change behaviour without modifying stored proecedures
	- DimDate_StartYear / EndYer - adjust date dimension range without redeploying code
	- ETLBatchSize - tune performance by changing batch size dynamically
- Environment Management
	- Differenet settings for Dev, Test, Prod environments
	- No hardcoded values scattered across multiple procedures
- Consistency
	- Single source of truth for configuration values
	- All ETL procedures reference the same settings
	- Example: InitialLoad_BackdateYear ensures all dimensions use same backdate logic
- Change Tracking
	- LastModified column tracks when settings were last changed
	- Helps in auditing and understanding configuration history
	  - For example: "Did performance degrade after changing ETLBatchSize?"
- Self-Documenting
	- Description field explains purpose of each config key
	- Easier for new developers/DBAs to understand ETL settings
*/







/*
Scenario this Setup

Without these tables:
- ETL fails at 3AM - No error deetails, manual intervention needed
- Need to change date range - Find and modify multiple stored procedures
- Performance degrades - No history of previous execution times

With these tables:
- ETL fails - Check ETLLog for error message, quickly identify and fix issue
- Change date range - Update ETLConfig once, all procedures use new range
- Performance issues - Analyze ETLLog trends, optimize slow processes
*/

