-- Set the context
USE DATABASE AIRBYTE_DATABASE;
USE SCHEMA ANALYTICS;

MERGE INTO AIRBYTE_DATABASE.ANALYTICS.DIM_STORE_STAFF AS TARGET
USING (
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
  JOIN AIRBYTE_RAW_DATA.COUNTRY CO ON CI.COUNTRY_ID = CO.COUNTRY_ID
) AS SOURCE
ON TARGET.STAFF_ID = SOURCE.STAFF_ID

WHEN MATCHED AND (
    TARGET.STAFF_NAME       != SOURCE.STAFF_NAME OR
    TARGET.EMAIL            != SOURCE.EMAIL OR
    TARGET.STORE_ID         != SOURCE.STORE_ID OR
    TARGET.STORE_ADDRESS    != SOURCE.STORE_ADDRESS OR
    TARGET.CITY             != SOURCE.CITY OR
    TARGET.COUNTRY          != SOURCE.COUNTRY
)
THEN UPDATE SET
    STAFF_NAME      = SOURCE.STAFF_NAME,
    EMAIL           = SOURCE.EMAIL,
    STORE_ID        = SOURCE.STORE_ID,
    STORE_ADDRESS   = SOURCE.STORE_ADDRESS,
    CITY            = SOURCE.CITY,
    COUNTRY         = SOURCE.COUNTRY

WHEN NOT MATCHED THEN INSERT (
    STAFF_ID, STAFF_NAME, EMAIL, STORE_ID, STORE_ADDRESS, CITY, COUNTRY
)
VALUES (
    SOURCE.STAFF_ID, SOURCE.STAFF_NAME, SOURCE.EMAIL, SOURCE.STORE_ID,
    SOURCE.STORE_ADDRESS, SOURCE.CITY, SOURCE.COUNTRY
);
