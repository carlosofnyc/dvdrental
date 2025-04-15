
USE DATABASE AIRBYTE_DATABASE;
USE SCHEMA ANALYTICS;

BEGIN TRANSACTION;

-- DIM_CUSTOMER_SCD
CREATE TABLE IF NOT EXISTS AIRBYTE_DATABASE.ANALYTICS.DIM_CUSTOMER_SCD (
    CUSTOMER_SK NUMBER AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID INT,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    ADDRESS STRING,
    CITY STRING,
    COUNTRY STRING,
    ACTIVE BOOLEAN,
    VALID_FROM DATE,
    VALID_TO DATE,
    CURRENT_FLAG BOOLEAN
);

MERGE INTO AIRBYTE_DATABASE.ANALYTICS.DIM_CUSTOMER_SCD AS TARGET
USING (
  SELECT 
    C.CUSTOMER_ID,
    C.FIRST_NAME,
    C.LAST_NAME,
    C.EMAIL,
    A.ADDRESS,
    CI.CITY,
    CO.COUNTRY,
    C.ACTIVE,
    CURRENT_DATE AS NEW_VALID_FROM
  FROM AIRBYTE_RAW_DATA.CUSTOMER C
  JOIN AIRBYTE_RAW_DATA.ADDRESS A ON C.ADDRESS_ID = A.ADDRESS_ID
  JOIN AIRBYTE_RAW_DATA.CITY CI ON A.CITY_ID = CI.CITY_ID
  JOIN AIRBYTE_RAW_DATA.COUNTRY CO ON CI.COUNTRY_ID = CO.COUNTRY_ID
) AS SOURCE
ON TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID 
   AND TARGET.CURRENT_FLAG = TRUE
   AND (
      TARGET.FIRST_NAME <> SOURCE.FIRST_NAME OR
      TARGET.LAST_NAME <> SOURCE.LAST_NAME OR
      TARGET.EMAIL <> SOURCE.EMAIL OR
      TARGET.ADDRESS <> SOURCE.ADDRESS OR
      TARGET.CITY <> SOURCE.CITY OR
      TARGET.COUNTRY <> SOURCE.COUNTRY OR
      TARGET.ACTIVE <> SOURCE.ACTIVE
   )
WHEN MATCHED THEN 
  UPDATE SET 
    TARGET.VALID_TO = CURRENT_DATE - INTERVAL '1 day',
    TARGET.CURRENT_FLAG = FALSE
WHEN NOT MATCHED THEN 
  INSERT (
    CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, ADDRESS, CITY, COUNTRY, ACTIVE,
    VALID_FROM, VALID_TO, CURRENT_FLAG
  ) VALUES (
    SOURCE.CUSTOMER_ID, SOURCE.FIRST_NAME, SOURCE.LAST_NAME, SOURCE.EMAIL,
    SOURCE.ADDRESS, SOURCE.CITY, SOURCE.COUNTRY, SOURCE.ACTIVE,
    SOURCE.NEW_VALID_FROM, NULL, TRUE
  );

