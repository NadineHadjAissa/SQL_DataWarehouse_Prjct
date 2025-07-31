-- =====================================================
-- crm_sales_details - Null, Negative, and Zero Checks
-- =====================================================
SELECT 
  COUNT(*) FILTER (WHERE sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL) AS null_values,
  COUNT(*) FILTER (WHERE sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0) AS negative_values,
  COUNT(*) FILTER (WHERE sls_quantity = 0 OR sls_price = 0) AS zero_values
FROM silver.crm_sales_details;


-- =====================================================
-- crm_prd_info - Invalid Date Ranges
-- =====================================================
SELECT COUNT(*) AS invalid_date_ranges
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- =====================================================
-- crm_prd_info - NULL or Empty Product Names
-- =====================================================
SELECT COUNT(*) AS empty_or_null_prd_nm
FROM silver.crm_prd_info
WHERE prd_nm IS NULL OR TRIM(prd_nm) = '';


-- =====================================================
-- crm_prd_info - NULL or Negative Product Cost
-- =====================================================
SELECT COUNT(*) AS null_or_negative_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;


-- =====================================================
-- crm_prd_info - Distinct Product Line Values
-- =====================================================
SELECT DISTINCT prd_line
FROM silver.crm_prd_info
ORDER BY prd_line;


-- =====================================================
-- crm_cust_info - Untrimmed First or Last Names
-- =====================================================
SELECT COUNT(*) AS untrimmed_names
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
   OR cst_lastname  != TRIM(cst_lastname);


-- =====================================================
-- crm_cust_info - Invalid Gender Values
-- =====================================================
SELECT DISTINCT cst_gender
FROM bronze.crm_cust_info
ORDER BY cst_gender;


-- =====================================================
-- crm_cust_info - NULL or Empty Date Fields
-- =====================================================
SELECT COUNT(*) AS null_or_empty_dates
FROM bronze.crm_cust_info
WHERE cst_birth_dt IS NULL OR cst_create_date IS NULL
   OR cst_birth_dt = '' OR cst_create_date = '';


-- =====================================================
-- crm_cust_info - Duplicate Customer IDs
-- =====================================================
SELECT COUNT(*) AS duplicate_cst_ids
FROM (
  SELECT cst_id
  FROM silver.crm_cust_info
  GROUP BY cst_id
  HAVING COUNT(*) > 1
) dup;
