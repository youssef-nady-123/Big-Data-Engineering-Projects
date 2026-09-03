USE DataWarehouse;
GO


SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2

-----------------------------------------------------------

-- check for unwanted spaces
SELECT * 
FROM bronze.erp_px_cat_g1v2
WHERE 
cat != TRIM(cat) 
OR subcat != TRIM(subcat)
OR maintenance != TRIM(maintenance);


----------------------------------------------------------------

-- data standardizations 
SELECT DISTINCT 
	cat
FROM bronze.erp_px_cat_g1v2;
GO

-- data standardizations 
SELECT DISTINCT 
	subcat
FROM bronze.erp_px_cat_g1v2;
GO

-- data standardizations 
SELECT DISTINCT 
	maintenance
FROM bronze.erp_px_cat_g1v2;
GO

----------------------------------------------------------------

-- truncate table first 
TRUNCATE TABLE silver.erp_px_cat_g1v2;
GO

-- insert data to the silver layer 
INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance)
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2

---------------------------------------------------------------

-- show the new data 
SELECT *
FROM silver.erp_px_cat_g1v2;
GO
