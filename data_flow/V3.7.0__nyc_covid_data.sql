USE DATABASE francisDB;
USE SCHEMA present;

--Creating a final dynamic tables that encompases all the data
CREATE OR REPLACE DYNAMIC TABLE all_nyc_covid_reports
    TARGET_LAG='1 MINUTE'
    WAREHOUSE=CHALLENGE_WH
AS
SELECT *
FROM brooklyn_covid_data

UNION

SELECT *
FROM bronx_covid_data

UNION

SELECT *
FROM manhattan_covid_data

UNION

SELECT *
FROM statenIsland_covid_data

UNION

SELECT *
FROM queens_covid_data;