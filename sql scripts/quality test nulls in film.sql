SELECT COUNT(*) AS num_null_film_ids
FROM AIRBYTE_DATABASE.ANALYTICS.DIM_FILM
WHERE FILM_ID IS NULL;