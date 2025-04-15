-- Check Source Data Worksheet
-- This worksheet verifies that the source tables in AIRBYTE_RAW_DATA schema exist and have data

-- Set the context
USE DATABASE AIRBYTE_DATABASE;

-- First check if AIRBYTE_RAW_DATA schema exists
SHOW SCHEMAS LIKE 'AIRBYTE_RAW_DATA' IN DATABASE AIRBYTE_DATABASE;

-- Check if the ANALYTICS schema exists
SHOW SCHEMAS LIKE 'ANALYTICS' IN DATABASE AIRBYTE_DATABASE;

-- Create the ANALYTICS schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS AIRBYTE_DATABASE.ANALYTICS;

-- Check if the source tables exist and have data
SELECT 
  'CUSTOMER' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.CUSTOMER) AS ROW_COUNT
UNION ALL
SELECT 
  'ADDRESS' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.ADDRESS) AS ROW_COUNT
UNION ALL
SELECT 
  'CITY' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.CITY) AS ROW_COUNT
UNION ALL
SELECT 
  'COUNTRY' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.COUNTRY) AS ROW_COUNT
UNION ALL
SELECT 
  'FILM' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.FILM) AS ROW_COUNT
UNION ALL
SELECT 
  'FILM_CATEGORY' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.FILM_CATEGORY) AS ROW_COUNT
UNION ALL
SELECT 
  'CATEGORY' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.CATEGORY) AS ROW_COUNT
UNION ALL
SELECT 
  'STAFF' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.STAFF) AS ROW_COUNT
UNION ALL
SELECT 
  'STORE' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.STORE) AS ROW_COUNT
UNION ALL
SELECT 
  'RENTAL' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.RENTAL) AS ROW_COUNT
UNION ALL
SELECT 
  'INVENTORY' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.INVENTORY) AS ROW_COUNT
UNION ALL
SELECT 
  'PAYMENT' AS TABLE_NAME, 
  (SELECT COUNT(*) FROM AIRBYTE_RAW_DATA.PAYMENT) AS ROW_COUNT;

-- Show list of all tables in AIRBYTE_RAW_DATA schema
SHOW TABLES IN SCHEMA AIRBYTE_RAW_DATA;