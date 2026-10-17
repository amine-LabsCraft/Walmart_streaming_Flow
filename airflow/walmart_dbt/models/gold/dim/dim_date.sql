-- Calendar dimension (one row per day), from 2024 to the end of next year.
-- French labels, Canadian holidays and retail events for the dashboards.
WITH days AS (
    SELECT explode(
        sequence(DATE'2024-01-01', make_date(year(current_date()) + 1, 12, 31), INTERVAL 1 DAY)
    ) AS calendar_date
),

enriched AS (
    SELECT
        calendar_date,
        year(calendar_date) AS year_number,
        month(calendar_date) AS month_number,
        dayofmonth(calendar_date) AS day_number,
        -- Monday = 1 ... Sunday = 7 (Spark dayofweek starts on Sunday).
        ((dayofweek(calendar_date) + 5) % 7) + 1 AS weekday_number,
        -- Black Friday: the day after the fourth Thursday of November.
        date_add(
            next_day(date_sub(make_date(year(calendar_date), 11, 1), 1), 'TH'), 22
        ) AS black_friday
    FROM days
)

SELECT
    CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
    calendar_date AS date,
    year_number AS year,
    quarter(calendar_date) AS quarter,
    CONCAT('T', quarter(calendar_date)) AS quarter_label,
    month_number,
    CASE month_number
        WHEN 1 THEN 'janvier' WHEN 2 THEN 'février' WHEN 3 THEN 'mars'
        WHEN 4 THEN 'avril' WHEN 5 THEN 'mai' WHEN 6 THEN 'juin'
        WHEN 7 THEN 'juillet' WHEN 8 THEN 'août' WHEN 9 THEN 'septembre'
        WHEN 10 THEN 'octobre' WHEN 11 THEN 'novembre' ELSE 'décembre'
    END AS month_name,
    date_format(calendar_date, 'yyyy-MM') AS year_month,
    weekofyear(calendar_date) AS iso_week,
    day_number AS day_of_month,
    weekday_number AS day_of_week,
    CASE weekday_number
        WHEN 1 THEN 'lundi' WHEN 2 THEN 'mardi' WHEN 3 THEN 'mercredi'
        WHEN 4 THEN 'jeudi' WHEN 5 THEN 'vendredi' WHEN 6 THEN 'samedi'
        ELSE 'dimanche'
    END AS day_name,
    weekday_number >= 6 AS is_weekend,
    CASE
        WHEN month_number = 1 AND day_number = 1 THEN 'Jour de l''An'
        WHEN month_number = 7 AND day_number = 1 THEN 'Fête du Canada'
        WHEN month_number = 12 AND day_number = 25 THEN 'Noël'
        WHEN month_number = 12 AND day_number = 26 THEN 'Lendemain de Noël'
    END AS holiday_name,
    CASE
        WHEN calendar_date BETWEEN black_friday AND date_add(black_friday, 3)
            THEN 'Black Friday'
        WHEN month_number = 12 AND day_number BETWEEN 15 AND 24 THEN 'Semaine de Noël'
        WHEN month_number = 12 AND day_number = 26 THEN 'Boxing Day'
        ELSE 'Période normale'
    END AS retail_event
FROM enriched
