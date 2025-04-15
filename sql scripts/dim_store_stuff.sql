-- Set the context
USE DATABASE AIRBYTE_DATABASE;
USE SCHEMA ANALYTICS;

-- Create dimension table
CREATE TABLE IF NOT EXISTS AIRBYTE_DATABASE.ANALYTICS.DIM_STORE_STAFF (
    STAFF_ID INT PRIMARY KEY,
    STAFF_NAME STRING,
    EMAIL STRING,
    STORE_ID INT,
    STORE_ADDRESS STRING,
    CITY STRING,
    COUNTRY STRING
);

-- Populate the dimension table
INSERT INTO AIRBYTE_DATABASE.ANALYTICS.DIM_STORE_STAFF (
    STAFF_ID, STAFF_NAME, EMAIL, STORE_ID, STORE_ADDRESS, CITY, COUNTRY
)
SELECT 
    S.STAFF_ID,
    S.FIRST_NAME || ' ' || S.LAST_NAME AS STAFF_NAME,
    S.EMAIL,
    ST.STORE_ID,
    A.ADDRESS AS STORE_ADDRESS,
    CI.CITY,
    CO.COUNTRY
FROM AIRBYTE_RAW_DATA.STAFF S
JOIN AIRBYTE_RAW_DATA.STORE ST ON S.STORE_ID = ST.STORE_ID
JOIN AIRBYTE_RAW_DATA.ADDRESS A ON ST.ADDRESS_ID = A.ADDRESS_ID
JOIN AIRBYTE_RAW_DATA.CITY CI ON A.CITY_ID = CI.CITY_ID
JOIN AIRBYTE_RAW_DATA.COUNTRY CO ON CI.COUNTRY_ID = CO.COUNTRY_ID;

-- Verify table was created and populated
SELECT COUNT(*) AS STAFF_COUNT FROM AIRBYTE_DATABASE.ANALYTICS.DIM_STORE_STAFF;

-- Show sample data
SELECT * FROM AIRBYTE_DATABASE.ANALYTICS.DIM_STORE_STAFF LIMIT 10;