SELECT RENTAL_ID 
FROM AIRBYTE_DATABASE.ANALYTICS.FACT_RENTAL_SNAPSHOT
WHERE FILM_ID NOT IN (
    SELECT FILM_ID FROM AIRBYTE_DATABASE.ANALYTICS.DIM_FILM
);