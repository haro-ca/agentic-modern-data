-- ============================================
-- neondb SQL Queries
-- ============================================

-- List all tables
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public';

-- Row counts for all tables
SELECT 'media_customer_reviews' as table_name, COUNT(*) as row_count FROM media_customer_reviews
UNION ALL SELECT 'media_gold_reviews_chunked', COUNT(*) FROM media_gold_reviews_chunked
UNION ALL SELECT 'sales_customers', COUNT(*) FROM sales_customers
UNION ALL SELECT 'sales_franchises', COUNT(*) FROM sales_franchises
UNION ALL SELECT 'sales_suppliers', COUNT(*) FROM sales_suppliers
UNION ALL SELECT 'sales_transactions', COUNT(*) FROM sales_transactions;

-- Transactions by month
SELECT TO_CHAR(datetime, 'YYYY-MM') as month, COUNT(*) as transactions, SUM(totalprice)::numeric(10,2) as revenue
FROM sales_transactions
GROUP BY month
ORDER BY month;

-- Top products by revenue
SELECT product, COUNT(*) as transactions, SUM(quantity) as total_qty, SUM(totalprice)::numeric(10,2) as revenue
FROM sales_transactions
GROUP BY product
ORDER BY revenue DESC
LIMIT 10;

-- Franchise performance (top 10)
SELECT f.name, f.city, f.country, COUNT(t.transactionid) as txns, SUM(t.totalprice)::numeric(10,2) as revenue
FROM sales_franchises f
JOIN sales_transactions t ON f.franchiseid = t.franchiseid
GROUP BY f.franchiseid, f.name, f.city, f.country
ORDER BY revenue DESC
LIMIT 10;

-- Payment methods distribution
SELECT paymentmethod, COUNT(*) as count, (COUNT(*)::float / 3333 * 100)::numeric(5,2) as pct
FROM sales_transactions
GROUP BY paymentmethod
ORDER BY count DESC;

-- Customer demographics by gender
SELECT gender, COUNT(*) as customers,
       (SELECT COUNT(*) FROM sales_transactions WHERE customerid IN (SELECT customerid FROM sales_customers WHERE gender = c.gender)) as transactions
FROM sales_customers c
GROUP BY gender;

-- Suppliers by continent
SELECT continent, COUNT(*) as suppliers, array_agg(DISTINCT ingredient) as ingredients
FROM sales_suppliers
GROUP BY continent;

-- Reviews by franchise
SELECT f.name, f.city, COUNT(r.review) as review_count
FROM media_customer_reviews r
JOIN sales_franchises f ON r.franchiseid = f.franchiseid
GROUP BY f.franchiseid, f.name, f.city
ORDER BY review_count DESC;

-- Customers by country
SELECT country, COUNT(*) as customers, continent
FROM sales_customers
GROUP BY country, continent
ORDER BY customers DESC;

-- Key metrics summary
SELECT 
  (SELECT SUM(totalprice)::numeric(10,2) FROM sales_transactions) as total_revenue,
  (SELECT AVG(totalprice)::numeric(10,2) FROM sales_transactions) as avg_transaction,
  (SELECT AVG(quantity) FROM sales_transactions) as avg_quantity,
  (SELECT COUNT(DISTINCT customerid) FROM sales_transactions) as active_customers,
  (SELECT COUNT(DISTINCT franchiseid) FROM sales_transactions) as active_franchises;
