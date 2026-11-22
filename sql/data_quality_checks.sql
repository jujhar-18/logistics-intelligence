-- Run these queries after the notebooks finish.
SELECT check_name, failing_rows, status, checked_at
FROM workspace.logistics.silver_quality_summary
ORDER BY status DESC, check_name;

SELECT COUNT(*) AS shipment_rows,
       COUNT(DISTINCT order_id) AS distinct_orders,
       SUM(CASE WHEN delivery_days < 0 THEN 1 ELSE 0 END) AS negative_delivery_rows,
       SUM(CASE WHEN is_late = 1 THEN 1 ELSE 0 END) AS late_fact_rows
FROM workspace.logistics.silver_shipments;

SELECT check_name, COUNT(*) AS quarantined_rows
FROM workspace.logistics.silver_quarantine
GROUP BY check_name
ORDER BY quarantined_rows DESC;

SELECT 'route' AS table_name, COUNT(*) AS rows FROM workspace.logistics.gold_otd_by_route
UNION ALL
SELECT 'seller', COUNT(*) FROM workspace.logistics.gold_seller_performance
UNION ALL
SELECT 'monthly', COUNT(*) FROM workspace.logistics.gold_monthly_trends;
