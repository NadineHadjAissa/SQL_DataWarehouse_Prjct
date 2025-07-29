-- ============================================================
-- DDL Script: Silver Layer Table Creation (from Bronze Layer)
-- Description: Creates cleaned working copies in the Silver schema
-- ============================================================

-- Ensure silver schema exists
CREATE SCHEMA IF NOT EXISTS silver;

-- Duplicate Bronze tables into Silver schema

-- 1. CRM Customer Info
CREATE TABLE silver.crm_cust_info AS
SELECT * FROM bronze.crm_cust_info;

-- 2. CRM Product Info
CREATE TABLE silver.crm_prd_info AS
SELECT * FROM bronze.crm_prd_info;

-- 3. CRM Sales Details
CREATE TABLE silver.crm_sales_details AS
SELECT * FROM bronze.crm_sales_details;

-- 4. ERP Location A101
CREATE TABLE silver.erp_loc_a101 AS
SELECT * FROM bronze.erp_loc_a101;

-- 5. ERP Customer AZ12
CREATE TABLE silver.erp_cust_az12 AS
SELECT * FROM bronze.erp_cust_az12;

-- 6. ERP Product Category G1V2
CREATE TABLE silver.erp_px_cat_g1v2 AS
SELECT * FROM bronze.erp_px_cat_g1v2;

