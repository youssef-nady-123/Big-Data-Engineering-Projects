USE DataWarehouse;
Go

SELECT 
     prd_id
    ,prd_key
    ,prd_nm
    ,prd_cost
    ,prd_line
    ,prd_start_dt
    ,prd_end_dt
  FROM bronze.crm_prd_info

  ---------------------------------------------------------

-- check for Nulls or duplicates in primary key
SELECT 
    prd_id,
    COUNT(*) AS total
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
GO

---------------------------------------------------------

-- check for unwanted spaces
SELECT 
    prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
GO

---------------------------------------------------------

-- check for Null or negative numbers 
SELECT 
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
GO

---------------------------------------------------------

-- show distinct prd_line 
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

---------------------------------------------------------

-- check for invalid date 
SELECT 
    prd_start_dt
FROM bronze.crm_prd_info;

---------------------------------------------------------

-- end date = start date of the 'NEXT' record - 1 
SELECT  
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS end_date
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

---------------------------------------------------------

-- do not forget to look the structure of the bronze.crm_prd_info table [column names and its data types]
-- split the prd_key into two columns 
SELECT  
    prd_id,

    -- will join it with table [erp_px_cat_g1v2]
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

    -- will join with table [sls_prd_key]
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,

    prd_nm,

    -- repace null costs with 0s
    ISNULL(prd_cost, 0) AS prd_cost,

    -- handle product line 
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,

    CAST(prd_start_dt AS DATE) AS prd_start_dt,

    -- end date = start date of the 'NEXT' record - 1 
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt )- 1 AS DATE)  AS end_date
FROM bronze.crm_prd_info;

---------------------------------------------------------

-- first truncate the table 
TRUNCATE TABLE silver.crm_prd_info ;

-- insert the data into silver.crm_prd_info
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt)
SELECT  
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt )- 1 AS DATE)  AS prd_end_dt
FROM bronze.crm_prd_info;

-----------------------------------------------------------

-- show loaded data
SELECT * FROM silver.crm_prd_info;

---------------------------------------------------------
