USE DataWarehouse;
GO

SELECT 
	cid,
	cntry
FROM bronze.erp_loc_a101;

---------------------------------------------------

-- removes dashes form the table 
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE 
		WHEN cntry IS NULL THEN 'n/a'
		ELSE cntry
	END AS cntry
FROM bronze.erp_loc_a101

---------------------------------------------------

-- check for the ids on the other table 
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE 
		WHEN cntry IS NULL THEN 'n/a'
		ELSE cntry
	END AS cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN 
	(SELECT cst_key FROM silver.crm_cust_info)

---------------------------------------------------

-- remove dashes from the id 
-- handle countries 
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE cntry
	END AS cntry
FROM bronze.erp_loc_a101;
GO

-----------------------------------------------------

-- truncate table first 
TRUNCATE TABLE silver.erp_loc_a101;
GO
 
-- insert new data to the silver lauer 
INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE cntry
	END AS cntry
FROM bronze.erp_loc_a101;
GO

-------------------------------------

-- show the new data
SELECT * FROM silver.erp_loc_a101;

-------------------------------------