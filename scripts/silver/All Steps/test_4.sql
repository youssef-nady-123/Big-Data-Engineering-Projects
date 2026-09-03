USE DataWarehouse;
GO

SELECT 
	cid,
	bdate,
	gen
FROM bronze.erp_cust_az12;


-- on this table we can't find the 'NAS' characters on the 'erp_cust_az12' table 
SELECT * FROM silver.crm_cust_info;

---------------------------------------

-- check the dates
SELECT 
	bdate
FROM bronze.erp_cust_az12

-- check for strength dates
WHERE bdate < '1924-01-01'
OR
-- check for birthdate that is higher than current dates 
bdate > GETDATE();

---------------------------------------
-- remove the NAS chars from the cid columns 
-- solve the nulls inside gender 
SELECT 
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;
GO

------------------------------------------------------

-- truncate table first 
TRUNCATE TABLE silver.erp_cust_az12;
Go

-- insert the data to the silver layer 
INSERT INTO silver.erp_cust_az12 (cid, bdate ,gen)
SELECT 
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;
GO

---------------------------------

-- show the new data on the silver layer 
SELECT * FROM silver.erp_cust_az12;
GO