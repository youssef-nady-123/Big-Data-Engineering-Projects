USE DataWarehouse;


SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
  FROM bronze.crm_sales_details;

---------------------------------------------------------------------------------

-- check for unwanted spaces
SELECT 
	sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

---------------------------------------------------------------------------------

-- check for invalid dates
SELECT 
	sls_order_dt,
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE 
	sls_order_dt <= 0 
	OR LEN(sls_order_dt) != 8
	OR sls_order_dt > 20500101
	OR sls_order_dt < 19000101	-- start date of the company 

---------------------------------------------------------------------------------

SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,

	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL

		-- cast first to VARCHAR then to DATE 
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,


	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL

		-- cast first to VARCHAR then to DATE 
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,


	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL

		-- cast first to VARCHAR then to DATE 
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details;

------------------------------------------------------------------

-- Check data consistency between sales, quantity, and price
-- >> sales = quantity * price
-- >> values must not be NULL, zero, or negative

SELECT DISTINCT
    sls_sales,
    sls_quantity,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE  sls_sales
	END AS sls_sales,

	CASE 
		-- if there is zero, replace it with Null
		WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price 	
	END AS sls_price
FROM bronze.crm_sales_details
WHERE 
    sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0
ORDER BY 
	sls_sales,
	sls_quantity,
	sls_price;

-------------------------------------------------------------

SELECT DISTINCT
    sls_sales AS old_sls_sales,
    sls_quantity,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE  sls_sales
	END AS sls_sales,

	CASE 
		-- if there is zero, replace it with Null
		WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price 	
	END AS sls_price
FROM bronze.crm_sales_details;

--------------------------------------------------------------------------

-- truncate table silver.crm_sales_details
TRUNCATE TABLE silver.crm_sales_details;

-- insert new data to silver layer 
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,

	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL

		-- cast first to VARCHAR then to DATE 
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,

	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL

		-- cast first to VARCHAR then to DATE 
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,

	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE  sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		-- if there is zero, replace it with Null
		WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price 	
	END AS sls_price
FROM bronze.crm_sales_details;


---------------------------------------------------------------------------

SELECT *
FROM silver.crm_sales_details

---------------------------------------------------------------------------
