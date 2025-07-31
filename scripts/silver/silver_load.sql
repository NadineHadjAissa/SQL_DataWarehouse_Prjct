-- =====================================================
-- Load Cleaned Data into silver.crm_cust_info
-- =====================================================
TRUNCATE TABLE silver.crm_cust_info;
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
-- Load Cleaned Data into silver.crm_prd_info
-- =====================================================
TRUNCATE TABLE silver.crm_prd_info;
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
-- Load Cleaned Data into silver.crm_sales_details
-- =====================================================

TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details (
    sls_ord_num, 
    sls_prd_key, 
    sls_cust_id, 
    sls_order_dt, 
    sls_ship_dt, 
    sls_due_dt, 
    sls_sales, 
    sls_quantity, 
    sls_price, 
    created_date,
	sls_sales_old,
	sls_price_old
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
    END AS sls_order_dt,

    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
    END AS sls_ship_dt,

    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
    END AS sls_due_dt,

    CASE 
        WHEN sls_sales IS NULL OR sls_sales < 0 OR sls_sales != sls_quantity* ABS(sls_price)
        THEN sls_price * ABS(sls_quantity)
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    CASE 
        WHEN sls_price IS NULL OR sls_price < 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price,

    CURRENT_TIMESTAMP AS created_date,
	sls_sales as sls_sales_old,
	sls_price as sls_price_old

FROM bronze.crm_sales_details;


-- =====================================================
-- Load Cleaned Data into silver.erp_cust_az12
-- =====================================================
insert into silver.erp_cust_az12(cid, bdate, gen, created_date)
select 
case when cid like 'NAS%'then substring(cid,4,length(cid))
else cid
end cid ,

 CASE WHEN bdate > current_date then NULL
 else bdate
 end as bdate,

 case when(upper(trim(gen)))='M' then 'Male'
      when (upper(trim(gen)))='F' then 'Female'
	  else 'n/a'
	  end as gen ,
	  current_timestamp as created_date
from bronze.erp_cust_az12 ; done it wokrs


-- =====================================================
-- Load Cleaned Data into silver.erp_loc_a101
-- =====================================================
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101(cid, cntry, created_date)
SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) = 'USA' OR TRIM(cntry) = 'US' THEN 'United States' 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry) 
    END AS cntry,
    CURRENT_TIMESTAMP AS created_date
FROM bronze.erp_loc_a101;



-- =====================================================
-- Load Cleaned Data into silver.erp_px_cat_g1v2
-- =====================================================
--data is already cleaned --
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance, created_date) 
SELECT 
id ,
cat,
subcat,
maintenance,
current_timestamp as created_date
FROM bronze.erp_px_cat_g1v2;
