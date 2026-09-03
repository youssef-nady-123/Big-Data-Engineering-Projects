-- check for Nulls or duplicates in primary key
SELECT 
	cst_id,
	COUNT(*) AS total
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
GO



-- check for unwanted spaces
SELECT
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
GO


-- standardization and consistency 
SELECT DISTINCT
	cst_marital_status
FROM silver.crm_cust_info;
GO

SELECT DISTINCT
	cst_gndr
FROM silver.crm_cust_info;
GO

--------------------------------------------------------------------

-- check for Null or negative numbers 
SELECT 
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
GO

--------------------------------------------------------------------

-- check for unwanted spaces
SELECT 
    prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
GO

--------------------------------------------------------------------

-- check for Nulls or duplicates in primary key
SELECT 
    prd_id,
    COUNT(*) AS total
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
GO


--------------------------------------------------------------------

-- check for invalid date 
SELECT 
    prd_start_dt
FROM silver.crm_prd_info;


-- show distinct prd_line 
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


-- check for Null or negative numbers 
SELECT 
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
GO


-- check for unwanted spaces
SELECT 
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
GO



