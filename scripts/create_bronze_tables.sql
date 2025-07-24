-- Create CRM Customer Info Table
CREATE TABLE bronze.crm_cust_info (
    cst_id integer NOT NULL DEFAULT nextval('bronze.crm_cust_info_cst_id_seq'::regclass),
    cst_key character varying,
    cst_firstname character varying,
    cst_lastname character varying,
    cst_marital_status character varying,
    cst_gndr character varying,
    cst_create_date date
);

-- Create CRM Product Info Table
CREATE TABLE bronze.crm_prd_info (
    prd_id integer NOT NULL,
    prd_key character varying,
    prd_nm character varying,
    prd_cost numeric,
    prd_line character varying,
    prd_start_dt timestamp without time zone,
    prd_end_dt timestamp without time zone
);

-- Create CRM Sales Details Table
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num character varying,
    sls_prd_key character varying,
    sls_cust_id integer,
    sls_order_dt integer,
    sls_ship_dt integer,
    sls_due_dt integer,
    sls_sales integer,
    sls_quantity integer,
    sls_price integer
);

-- Create ERP Customer Table (az12)
CREATE TABLE bronze.erp_cust_az12 (
    cid character varying,
    bdate date,
    gen character varying
);

-- Create ERP Location Table (a101)
CREATE TABLE bronze.erp_loc_a101 (
    cid character varying,
    cntry character varying
);

-- Create ERP Product Category Table (g1v2)
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id character varying,
    cat character varying,
    subcat character varying,
    maintenance character varying
);
