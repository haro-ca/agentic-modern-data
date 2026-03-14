-- DuckDB Queries for sales.duckdb

-- Show all downloads/partitions
SELECT download_id, downloaded_at, COUNT(*) as transactions
FROM sales_transactions_raw
GROUP BY download_id, downloaded_at
ORDER BY download_id DESC;

-- Query specific download
SELECT * FROM sales_transactions_raw WHERE download_id = 1;

-- Latest download only
SELECT * FROM sales_transactions_raw 
WHERE download_id = (SELECT MAX(download_id) FROM sales_transactions_raw);

-- Compare two downloads
SELECT 
    t1.download_id,
    t2.download_id,
    t1.cnt as first_download,
    t2.cnt as second_download,
    t2.cnt - t1.cnt as diff
FROM 
    (SELECT download_id, COUNT(*) as cnt FROM sales_transactions_raw WHERE download_id = 1 GROUP BY download_id) t1
JOIN
    (SELECT download_id, COUNT(*) as cnt FROM sales_transactions_raw WHERE download_id = 2 GROUP BY download_id) t2
ON 1=1;

-- Revenue by product (latest download)
SELECT product, SUM(totalprice) as revenue, COUNT(*) as transactions
FROM sales_transactions_raw
WHERE download_id = (SELECT MAX(download_id) FROM sales_transactions_raw)
GROUP BY product
ORDER BY revenue DESC;

-- Hourly distribution (latest download)
SELECT EXTRACT(HOUR FROM datetime) as hour, COUNT(*) as transactions
FROM sales_transactions_raw
WHERE download_id = (SELECT MAX(download_id) FROM sales_transactions_raw)
GROUP BY hour
ORDER BY hour;
