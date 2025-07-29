-- ==============================================================================
-- Stored Procedure: Load Bronze Layer (CSV -> bronze schema in PostgreSQL)
-- ==============================================================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    total_start TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    -- CRM Tables
    RAISE NOTICE 'Loading CRM Tables';

    -- crm_cust_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info FROM '/tmp/cust_info.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_cust_info loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- crm_prd_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info FROM '/tmp/prd_info.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_prd_info loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- crm_sales_details
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details FROM '/tmp/sales_details.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_sales_details loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- ERP Tables
    RAISE NOTICE 'Loading ERP Tables';

    -- erp_loc_a101
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101 FROM '/tmp/LOC_A101.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_loc_a101 loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- erp_cust_az12
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12 FROM '/tmp/CUST_AZ12.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_cust_az12 loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2 FROM '/tmp/PX_CAT_G1V2.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_px_cat_g1v2 loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Bronze Layer Loading Complete. Total Time: % seconds', EXTRACT(SECOND FROM clock_timestamp() - total_start);
    RAISE NOTICE '================================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'ERROR DURING BRONZE LOAD';
    RAISE NOTICE 'Message: %', SQLERRM;
    RAISE NOTICE '================================================';
END;
$$;

-- ================================================
-- Step: Add Metadata Column to Silver Tables
-- Description: Add and populate `created_date` column 
--              to track when records were inserted 
--              into the Silver layer.
-- Purpose: Helps with auditability and data lineage.
-- ================================================

-- Add and populate created_date for silver.crm_cust_info
ALTER TABLE silver.crm_cust_info ADD COLUMN created_date TIMESTAMP;
UPDATE silver.crm_cust_info SET created_date = CURRENT_TIMESTAMP WHERE created_date IS NULL;

-- Add and populate created_date for silver.crm_prd_info
ALTER TABLE silver.crm_prd_info ADD COLUMN created_date TIMESTAMP;
UPDATE silver.crm_prd_info SET created_date = CURRENT_TIMESTAMP WHERE created_date IS NULL;

-- Add and populate created_date for silver.crm_sales_details
ALTER TABLE silver.crm_sales_details ADD COLUMN created_date TIMESTAMP;
UPDATE silver.crm_sales_details SET created_date = CURRENT_TIMESTAMP WHERE created_date IS NULL;

-- Add and populate created_date for silver.erp_loc_a101
ALTER TABLE silver.erp_loc_a101 ADD COLUMN created_date TIMESTAMP;
UPDATE silver.erp_loc_a101 SET created_date = CURRENT_TIMESTAMP WHERE created_date IS NULL;

-- Add and populate created_date for silver.erp_cust_az12
ALTER TABLE silver.erp_cust_az12 ADD COLUMN created_date TIMESTAMP;
UPDATE silver.erp_cust_az12 SET created_date = CURRENT_TIMESTAMP WHERE created_date IS NULL;

-- Add and populate created_date for silver.erp_px_cat_g1v2
ALTER TABLE silver.erp_px_cat_g1v2 ADD COLUMN created_date TIMESTAMP;
UPDATE silver.erp_px_cat_g1v2 SET created_date = CURRENT_TIMESTAMP WHERE created_date IS NULL;

