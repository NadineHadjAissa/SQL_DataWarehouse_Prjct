-- GOLD Layer Quality Checks

-- 1. Fact-to-Dimension Link Integrity — Customers
SELECT *
FROM gold.dim_customers cs
LEFT JOIN gold.fact_sales fs
  ON cs.customer_key = fs.customer_key
WHERE cs.customer_key IS NULL;

-- 2. Fact-to-Dimension Link Integrity — Products
SELECT *
FROM gold.dim_products dp
LEFT JOIN gold.fact_sales fs
  ON dp.product_key = fs.product_key
WHERE dp.product_key IS NULL;

-- 3. Duplicate Keys in Dimension Tables
-- Customers\SELECT customer_key, COUNT(*) 
FROM gold.dim_customers 
GROUP BY customer_key 
HAVING COUNT(*) > 1;

-- Products
SELECT product_key, COUNT(*) 
FROM gold.dim_products 
GROUP BY product_key 
HAVING COUNT(*) > 1;

-- 4. Orphaned Keys in Fact Table
-- Orphaned product_keys
SELECT * 
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
  ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL;

-- Orphaned customer_keys
SELECT * 
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
  ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;

-- 5. Null Checks in Business-Critical Fields
SELECT *
FROM gold.fact_sales
WHERE order_number IS NULL
   OR order_date IS NULL
   OR product_key IS NULL
   OR customer_key IS NULL;
