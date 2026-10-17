-- Time-of-day dimension at the hour grain (store local time).
SELECT
    hour_of_day AS time_key,
    CONCAT(lpad(CAST(hour_of_day AS STRING), 2, '0'), ' h') AS hour_label,
    CASE
        WHEN hour_of_day BETWEEN 7 AND 10 THEN 'Matin'
        WHEN hour_of_day BETWEEN 11 AND 13 THEN 'Midi'
        WHEN hour_of_day BETWEEN 14 AND 16 THEN 'Après-midi'
        WHEN hour_of_day BETWEEN 17 AND 19 THEN 'Heure de pointe'
        WHEN hour_of_day BETWEEN 20 AND 22 THEN 'Soirée'
        ELSE 'Nuit'
    END AS day_part,
    hour_of_day BETWEEN 7 AND 22 AS is_store_open
FROM (SELECT explode(sequence(0, 23)) AS hour_of_day)
