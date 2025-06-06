

-- Create BI view
-- This joins all dimensions with the fact table to create a denormalized view for reporting
-- Set the context
USE DATABASE AIRBYTE_DATABASE;
USE SCHEMA ANALYTICS;
bi
CREATE OR REPLACE TABLE AIRBYTE_DATABASE.ANALYTICS.BI_RENTALS_ALL AS
SELECT
    F.RENTAL_ID,
    F.CUSTOMER_ID,
    C.FIRST_NAME || ' ' || C.LAST_NAME AS CUSTOMER_NAME,
    C.CITY AS CUSTOMER_CITY,
    C.COUNTRY AS CUSTOMER_COUNTRY,
    
    F.STAFF_ID,
    S.STAFF_NAME,
    S.STORE_ID,
    S.STORE_ADDRESS,
    S.CITY AS STORE_CITY,
    S.COUNTRY AS STORE_COUNTRY,
    
    F.FILM_ID,
    FI.TITLE AS FILM_TITLE,
    FI.CATEGORY AS FILM_CATEGORY,
    FI.RELEASE_YEAR,
    FI.RATING,
    FI.LENGTH AS FILM_LENGTH,

    D.DATE_ID,
    D.FULL_DATE AS RENTAL_DATE,
    D.WEEKDAY AS RENTAL_WEEKDAY,

    F.RENTAL_DURATION,
    F.TOTAL_AMOUNT,
    F.CUSTOMER_RENTAL_RANK
FROM AIRBYTE_DATABASE.ANALYTICS.FACT_RENTAL_SNAPSHOT F
JOIN AIRBYTE_DATABASE.ANALYTICS.DIM_CUSTOMER_SCD C 
    ON F.CUSTOMER_ID = C.CUSTOMER_ID AND C.CURRENT_FLAG = TRUE
JOIN AIRBYTE_DATABASE.ANALYTICS.DIM_FILM FI 
    ON F.FILM_ID = FI.FILM_ID
JOIN AIRBYTE_DATABASE.ANALYTICS.DIM_DATE D 
    ON F.DATE_ID = D.DATE_ID
JOIN AIRBYTE_DATABASE.ANALYTICS.DIM_STORE_STAFF S 
    ON F.STAFF_ID = S.STAFF_ID;
