SELECT to_date(created_date) AS day, borough, complaint_type,
       count(*) AS requests,
       sum(CASE WHEN status <> 'CLOSED' OR status IS NULL THEN 1 ELSE 0 END) AS unresolved_now,
       percentile_approx(resolution_hours, array(0.5, 0.9, 0.99)) AS resolution_hour_quantiles,
       sum(CASE WHEN status = 'CLOSED' AND closed_date IS NULL THEN 1 ELSE 0 END) AS closed_missing_date,
       sum(CASE WHEN status <> 'CLOSED' AND closed_date IS NOT NULL THEN 1 ELSE 0 END) AS nonclosed_with_date,
       sum(CASE WHEN invalid_resolution THEN 1 ELSE 0 END) AS invalid_resolution_count
FROM silver_requests
GROUP BY to_date(created_date), borough, complaint_type
