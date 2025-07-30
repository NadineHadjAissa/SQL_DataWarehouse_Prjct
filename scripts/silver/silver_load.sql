-- =====================================================
-- Load Cleaned Data into silver.crm_cust_info
-- =====================================================

INSERT INTO silver.crm_cust_info (
  cst_id,
  cst_firstname,
  cst_lastname,
  cst_gender,
  cst_birth_dt,
  cst_create_date
)
SELECT 
  cst_id,
  TRIM(cst_firstname),
  TRIM(cst_lastname),
  CASE 
    WHEN UPPER(cst_gender) IN ('M', 'MALE') THEN 'Male'
    WHEN UPPER(cst_gender) IN ('F', 'FEMALE') THEN 'Female'
    ELSE 'n/a'
  END AS cst_gender,
  CAST(cst_birth_dt AS DATE),
  CAST(cst_create_date AS DATE)
FROM bronze.crm_cust_info;
-- =====================================================
-- Quality Checks: crm_cust_info
-- =====================================================

-- Check for untrimmed names
SELECT *
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
   OR cst_lastname  != TRIM(cst_lastname);

-- Check for invalid gender values
SELECT DISTINCT cst_gender
FROM bronze.crm_cust_info;

-- Check for NULL or empty birth or create dates
SELECT *
FROM bronze.crm_cust_info
WHERE cst_birth_dt IS NULL OR cst_create_date IS NULL
   OR cst_birth_dt = '' OR cst_create_date = '';

-- Check for duplicate customer IDs
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;
-- =====================================================
-- Load Cleaned Data into silver.crm_prd_info
-- =====================================================

INSERT INTO silver.crm_prd_info (
  prd_id,
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt,
  cat_id
)
SELECT 
  prd_id,
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
  prd_nm,
  COALESCE(prd_cost, 0) AS prd_cost,
  CASE 
    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    WHEN UPPER(TRIM(prd_line)) = 'O' THEN 'Other Sales'
    ELSE 'n/a'
  END AS prd_line,
  CAST(prd_start_dt AS DATE),
  CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE) AS prd_end_dt,
  REPLACE(SUBSTRING(prd_key, 1, 5), '_', '-') AS cat_id
FROM bronze.crm_prd_info;



-- =====================================================
-- Quality Checks: crm_prd_info
-- =====================================================

-- Check if any prd_end_dt < prd_start_dt
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Check for NULL or empty product names
SELECT *
FROM silver.crm_prd_info
WHERE prd_nm IS NULL OR TRIM(prd_nm) = '';

-- Check raw prd_line values before transformation
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for NULL or negative prd_cost
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

