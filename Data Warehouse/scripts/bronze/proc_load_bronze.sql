/*
===============================================================================
Bronze Layer: Load Data (Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'bronze' schema from external CSV files.

    It performs the following actions:
    - Truncates the Bronze tables before loading data.
    - Uses BULK INSERT to load CSV files into Bronze tables.
===============================================================================
*/

USE DataWarehouse;
GO

PRINT '================================================';
PRINT 'Loading Bronze Layer';
PRINT '================================================';


/*
===============================================================================
CRM Tables
===============================================================================
*/

PRINT '------------------------------------------------';
PRINT 'Loading CRM Tables';
PRINT '------------------------------------------------';


-- =================================================
-- Load: bronze.crm_cust_info
-- =================================================

PRINT '>> Truncating Table: bronze.crm_cust_info';

TRUNCATE TABLE bronze.crm_cust_info;

PRINT '>> Inserting Data Into: bronze.crm_cust_info';

BULK INSERT bronze.crm_cust_info
FROM 'D:\projects\06 DataWarehouseProject\Datasets\source_crm\cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '>> -------------';


-- =================================================
-- Load: bronze.crm_prd_info
-- =================================================

PRINT '>> Truncating Table: bronze.crm_prd_info';

TRUNCATE TABLE bronze.crm_prd_info;

PRINT '>> Inserting Data Into: bronze.crm_prd_info';

BULK INSERT bronze.crm_prd_info
FROM 'D:\projects\06 DataWarehouseProject\Datasets\source_crm\prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '>> -------------';


-- =================================================
-- Load: bronze.crm_sales_details
-- =================================================

PRINT '>> Truncating Table: bronze.crm_sales_details';

TRUNCATE TABLE bronze.crm_sales_details;

PRINT '>> Inserting Data Into: bronze.crm_sales_details';

BULK INSERT bronze.crm_sales_details
FROM 'D:\projects\06 DataWarehouseProject\Datasets\source_crm\sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '>> -------------';


/*
===============================================================================
ERP Tables
===============================================================================
*/

PRINT '------------------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '------------------------------------------------';


-- =================================================
-- Load: bronze.erp_cust_az12
-- =================================================

PRINT '>> Truncating Table: bronze.erp_cust_az12';

TRUNCATE TABLE bronze.erp_cust_az12;

PRINT '>> Inserting Data Into: bronze.erp_cust_az12';

BULK INSERT bronze.erp_cust_az12
FROM 'D:\projects\06 DataWarehouseProject\Datasets\source_erp\CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '>> -------------';


-- =================================================
-- Load: bronze.erp_loc_a101
-- =================================================

PRINT '>> Truncating Table: bronze.erp_loc_a101';

TRUNCATE TABLE bronze.erp_loc_a101;

PRINT '>> Inserting Data Into: bronze.erp_loc_a101';

BULK INSERT bronze.erp_loc_a101
FROM 'D:\projects\06 DataWarehouseProject\Datasets\source_erp\LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '>> -------------';


-- =================================================
-- Load: bronze.erp_px_cat_g1v2
-- =================================================

PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';

BULK INSERT bronze.erp_px_cat_g1v2
FROM 'D:\Projects\06 DataWarehouseProject\Datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

PRINT '>> -------------';


PRINT '==========================================';
PRINT 'Loading Bronze Layer is Completed';
PRINT '==========================================';
GO