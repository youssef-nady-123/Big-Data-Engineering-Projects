/*
	====================
	== Loading Silver ==
	====================
	- before build the silver layer, solve the issues on bronze layer 
*/

USE DataWarehouse;


-- check for Nulls or duplicates in primary key
SELECT 
	cst_id,
	COUNT(*) AS total
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-------------------------------------------------------------------

-- check for unwanted spaces
SELECT
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-------------------------------------------------------------------

-- data standardization & consistency 
SELECT 
	cst_marital_status,
	CASE 
		WHEN cst_marital_status = UPPER('M') THEN 'Married'
		WHEN cst_marital_status = UPPER('S') THEN 'Single'
		ELSE 'n/a'
	END AS cst_marital_status
FROM bronze.crm_cust_info;


-- gender standardization 
SELECT
	cst_gndr,
	CASE 
		WHEN cst_gndr = UPPER('M') THEN 'Male'
		WHEN cst_gndr = UPPER('F') THEN 'Female'
		ELSE 'n/a'
	END AS cst_gndr
FROM bronze.crm_cust_info;

-------------------------------------------------------------------

/*
    ==========================================
    -- Data Cleansing & Standardization
    ==========================================
    - Remove duplicate customer records
      by keeping the latest record for each cst_id
    - Remove unwanted spaces from customer names
    - Standardize gender values
    - Standardize marital status values
    - Replace invalid/unexpected values with 'n/a'
*/
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gndr,
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cst_marital_status,
	cst_create_date
FROM (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
)t  WHERE flag_last = 1

-------------------------------------------------------------------------

-- truncate the table first 
TRUNCATE TABLE silver.crm_cust_info;
GO

-- write the insert statement to this table 
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_gndr,
	cst_marital_status,
	cst_create_date)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gndr,
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cst_marital_status,
	cst_create_date
FROM (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t  WHERE flag_last = 1;
GO

--------------------------------------------------

-- show the silver.crm_cust_info table
SELECT *
FROM silver.crm_cust_info;
GO

--------------------------------------------------s








