-- ==============================================================================
-- Stored Procedure: Load Bronze Layer (CSV -> bronze schema in PostgreSQL)
-- ==============================================================================
-- Purpose:
--     This stored procedure loads CSV files into the bronze layer tables.
--     - Truncates all bronze tables
--     - Loads data from local CSV files using COPY
--     - Logs load duration and catches errors
-- Usage:
--     CALL bronze.load_bronze();   -- run to refresh data
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
    COPY bronze.crm_cust_info FROM '/path/to/cust_info.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_cust_info loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- crm_prd_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info FROM '/path/to/prd_info.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_prd_info loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- crm_sales_details
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details FROM '/path/to/sales_details.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_sales_details loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- ERP Tables
    RAISE NOTICE 'Loading ERP Tables';

    -- erp_loc_a101
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101 FROM '/path/to/loc_a101.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_loc_a101 loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- erp_cust_az12
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12 FROM '/path/to/cust_az12.csv' DELIMITER ',' CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_cust_az12 loaded in % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2 FROM '/path/to/px_cat_g1v2.csv' DELIMITER ',' CSV HEADER;
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

