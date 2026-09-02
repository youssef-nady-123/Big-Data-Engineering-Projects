/*
    =============================
    == Create The Bronze Layer ==
    =============================
*/

USE master;
GO

DROP DATABASE IF EXISTS TestDataWarehouse;
GO

CREATE DATABASE TestDataWarehouse;
GO

USE TestDataWarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

------------------------------------------------

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO

---------------------------------------------------

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 

    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY

    SET @batch_start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_cust_info;

        SET @start_time = GETDATE();
        PRINT '-----------------------------------------------------------------------------------------------';
        PRINT 'Load Data Into Table: bronze.crm_cust_info';
        PRINT '-----------------------------------------------------------------------------------------------';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\02 projects\06 DataWarehouseProject\Datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
        PRINT '>> --------------------------';




        TRUNCATE TABLE bronze.crm_prd_info;

        SET @start_time = GETDATE();
        PRINT '-----------------------------------------------------------------------------------------------';
        PRINT 'Load Data Into Table: bronze.crm_prd_info';
        PRINT '-----------------------------------------------------------------------------------------------';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\02 projects\06 DataWarehouseProject\Datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST( DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
        PRINT '>> --------------------------';



        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '-----------------------------------------------------------------------------------------------';
        PRINT 'Load Data Into Table: bronze.crm_sales_details';
        PRINT '-----------------------------------------------------------------------------------------------';


        SET @start_time = GETDATE();
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\02 projects\06 DataWarehouseProject\Datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
        PRINT '>> --------------------------';



        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '-----------------------------------------------------------------------------------------------';
        PRINT 'Load Data Into Table: bronze.erp_cust_az12';
        PRINT '-----------------------------------------------------------------------------------------------';

        SET @start_time = GETDATE();
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\02 projects\06 DataWarehouseProject\Datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Loading Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
        PRINT '>> --------------------------';


        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '-----------------------------------------------------------------------------------------------';
        PRINT 'Load Data Into Table: bronze.erp_loc_a101';
        PRINT '-----------------------------------------------------------------------------------------------';

        SET @start_time = GETDATE();
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\02 projects\06 DataWarehouseProject\Datasets\source_erp\LOC_A101.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();

        PRINT 'Loading Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' Millseconds';
        PRINT '>> --------------------------';


        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '-----------------------------------------------------------------------------------------------';
        PRINT 'Load Data Into Table: bronze.erp_px_cat_g1v2';
        PRINT '-----------------------------------------------------------------------------------------------';


        SET @start_time = GETDATE();
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\02 projects\06 DataWarehouseProject\Datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time = GETDATE();

        PRINT 'Loading Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
        PRINT '>> --------------------------';

    SET @batch_end_time = GETDATE();
    PRINT 'Loading Bronze Layer Duration: ' + CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Milliseconds';
    END TRY

    BEGIN CATCH 
        PRINT '==============================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==============================================';
    END CATCH 

END
GO


-- execute stored procedure 
EXEC bronze.load_bronze;

------------------------------------------------------------

