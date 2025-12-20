/*
Stored Procedure: Load Bronze Layer (Source -> Bronze)
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:

Truncates the bronze tables before loading data.
Uses the BULK INSERT command to load data from csv files to bronze tables.
Parameters:
None.

This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    -- ============================================
    -- Bronze Layer Load Procedure
    -- Measures:
    -- 1. Load time per table (ms)
    -- 2. Total load time for all files (ms)
    -- ============================================

    DECLARE 
        @proc_start_time DATETIME2(3),
        @proc_end_time   DATETIME2(3),
        @start_time      DATETIME2(3),
        @end_time        DATETIME2(3);

    BEGIN TRY
        -- Start total timer
        SET @proc_start_time = SYSDATETIME();
        PRINT 'Starting full Bronze layer load...';

        -- ============================
        -- CRM Customer Info
        -- ============================
        PRINT 'Loading CRM Customer Info...';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '>> CRM Customer Info load time: '
            + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20))
            + ' ms';

        -- ============================
        -- CRM Product Info
        -- ============================
        PRINT 'Loading CRM Product Info...';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '>> CRM Product Info load time: '
            + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20))
            + ' ms';

        -- ============================
        -- CRM Sales Details
        -- ============================
        PRINT 'Loading CRM Sales Details...';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '>> CRM Sales Details load time: '
            + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20))
            + ' ms';

        -- ============================
        -- ERP Customer
        -- ============================
        PRINT 'Loading ERP Customer...';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\cust_AZ12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '>> ERP Customer load time: '
            + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20))
            + ' ms';

        -- ============================
        -- ERP Location
        -- ============================
        PRINT 'Loading ERP Location...';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\loc_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '>> ERP Location load time: '
            + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20))
            + ' ms';

        -- ============================
        -- ERP Product Category
        -- ============================
        PRINT 'Loading ERP Product Category...';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '>> ERP Product Category load time: '
            + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR(20))
            + ' ms';

        -- ============================
        -- Total Load Time
        -- ============================
        SET @proc_end_time = SYSDATETIME();

        PRINT '------------------------------------------';
        PRINT 'Total Bronze load duration: '
            + CAST(DATEDIFF(MILLISECOND, @proc_start_time, @proc_end_time) AS NVARCHAR(20))
            + ' ms';
        PRINT 'Bronze layer data load completed successfully.';

    END TRY
    BEGIN CATCH
        PRINT 'Error occurred during Bronze layer load.';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error Message: ' + ERROR_MESSAGE();
    END CATCH
END;
