/*
=================================================
STORED PROCEDURE: LOAD BRONZE LAYER (SOURCE ---> BRONZE)
=================================================
This stored procedure loads data into 'bronze' schema from CSV files.
It performes following actions:
- truncates bronze tables before loading data,
- ususes 'BULK INSERT' to load data from csv files to bronze tables 

EXEC bronze.load_bronze;
==================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;
	BEGIN TRY
		SET @BATCH_START_TIME = GETDATE()
		PRINT '=============================='
		PRINT 'lOADING BRONZE LAYER'
		PRINT '=============================='
		PRINT ' LOADING CRM'
		PRINT '=============================='
		SET @START_TIME = GETDATE()
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\igor1\OneDrive\Pulpit\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT ' LOADTIME: ' + CAST(DATEDIFF(second, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'

		SET @START_TIME = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\igor1\OneDrive\Pulpit\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT ' LOADTIME: ' + CAST(DATEDIFF(second, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'

		SET @START_TIME = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\igor1\OneDrive\Pulpit\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT ' LOADTIME: ' + CAST(DATEDIFF(second, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'

		PRINT '====================='
		PRINT 'LOADING ERP'
		PRINT '====================='
		
		SET @START_TIME = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\igor1\OneDrive\Pulpit\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT ' LOADTIME: ' + CAST(DATEDIFF(second, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'

		SET @START_TIME = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\igor1\OneDrive\Pulpit\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT ' LOADTIME: ' + CAST(DATEDIFF(second, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'

		SET @START_TIME = GETDATE()
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\igor1\OneDrive\Pulpit\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT ' LOADTIME: ' + CAST(DATEDIFF(second, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'
		SET @BATCH_END_TIME = GETDATE()
		PRINT ' LOADTIME BRONZE LAYER: ' + CAST(DATEDIFF(second, @BATCH_START_TIME, @BATCH_END_TIME) AS NVARCHAR) + ' SECONDS'
		END TRY
		BEGIN CATCH
		PRINT 'ERROR DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		END CATCH
END
