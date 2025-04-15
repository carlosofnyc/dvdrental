-- First check if database and schema exist
SHOW DATABASES LIKE 'AIRBYTE_DATABASE';
SHOW SCHEMAS LIKE 'ANALYTICS' IN DATABASE AIRBYTE_DATABASE;

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS AIRBYTE_DATABASE.ANALYTICS;

-- Set the context explicitly
USE DATABASE AIRBYTE_DATABASE;
USE SCHEMA ANALYTICS;

-- Create a test table
CREATE OR REPLACE TABLE AIRBYTE_DATABASE.ANALYTICS.TEST_TABLE (
  ID INT,
  NAME STRING
);

-- Insert a test record
INSERT INTO AIRBYTE_DATABASE.ANALYTICS.TEST_TABLE VALUES (1, 'Test');

-- Verify the table exists and has data
SELECT * FROM AIRBYTE_DATABASE.ANALYTICS.TEST_TABLE;

-- Show all tables in the schema to verify
SHOW TABLES IN SCHEMA AIRBYTE_DATABASE.ANALYTICS;