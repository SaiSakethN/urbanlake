WITH lagged AS (
    SELECT day, requests,
           lag(requests, 1) OVER (ORDER BY day) AS lag1,
           lag(requests, 7) OVER (ORDER BY day) AS lag7,
           lag(requests, 14) OVER (ORDER BY day) AS lag14,
           avg(requests) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS mean7,
           pmod(dayofweek(day) + 5, 7) AS weekday,
           month(day) AS month,
           lag(day, 14) OVER (ORDER BY day) AS history_start
    FROM daily_demand
)
SELECT day, requests, lag1, lag7, lag14, mean7, weekday, month
FROM lagged
WHERE datediff(day, history_start) = 14
