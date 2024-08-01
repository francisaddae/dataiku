
USE SCHEMA francisDB.Public;


--Brooklyn
CREATE OR REPLACE DYNAMIC TABLE brooklyn_covid_data
    LAG='DOWNSTREAM'
    WAREHOUSE='CHALLENGE_WH'
AS
SELECT
    'Brooklyn' AS borough,
    date_of_interest,
    bk_case_count AS case_count,
    bk_hospitalized_count AS hospitalized_count,
    bk_probable_case_count AS probable_case_count,
    bk_death_count AS death_count
FROM NYC_COVID_DATA
QUALIFY ROW_NUMBER() OVER(PARTITION BY date_of_interest ORDER BY date_of_interest) = 1;


--Bronx
CREATE OR REPLACE DYNAMIC TABLE bronx_covid_data
    -- LAG='DOWNSTREAM'
    -- WAREHOUSE='CHALLENGE_WH'
AS
SELECT
    'Bronx' AS borough,
    date_of_interest,
    bx_case_count AS case_count,
    bx_hospitalized_count AS hospitalized_count,
    bx_probable_case_count AS probable_case_count,
    bx_death_count AS death_count
FROM NYC_COVID_DATA
QUALIFY ROW_NUMBER() OVER(PARTITION BY date_of_interest ORDER BY date_of_interest) = 1;


--Manhattan
CREATE OR REPLACE DYNAMIC TABLE manhattan_covid_data
    LAG='DOWNSTREAM'
    WAREHOUSE='CHALLENGE_WH'
AS
SELECT
    'Manhattan' AS borough,
    date_of_interest,
    mn_case_count AS case_count,
    mn_hospitalized_count AS hospitalized_count,
    mn_probable_case_count AS probable_case_count,
    mn_death_count AS death_count
FROM NYC_COVID_DATA
QUALIFY ROW_NUMBER() OVER(PARTITION BY date_of_interest ORDER BY date_of_interest) = 1;


-- Queens
CREATE OR REPLACE DYNAMIC TABLE queens_covid_data
    LAG='DOWNSTREAM'
    WAREHOUSE='CHALLENGE_WH'
AS
SELECT
    'Queens' AS borough,
    date_of_interest,
    qn_case_count AS case_count,
    qn_hospitalized_count AS hospitalized_count,
    qn_probable_case_count AS probable_case_count,
    qn_death_count AS death_count
FROM NYC_COVID_DATA
QUALIFY ROW_NUMBER() OVER(PARTITION BY date_of_interest ORDER BY date_of_interest) = 1;


--Staten Island
CREATE OR REPLACE DYNAMIC TABLE statenIsland_covid_data
    LAG='DOWNSTREAM'
    WAREHOUSE='CHALLENGE_WH'
AS
SELECT
    'Staten Island' AS borough,
    date_of_interest,
    si_case_count AS case_count,
    si_hospitalized_count AS hospitalized_count,
    si_probable_case_count AS probable_case_count,
    si_death_count AS death_count
FROM NYC_COVID_DATA
QUALIFY ROW_NUMBER() OVER(PARTITION BY date_of_interest ORDER BY date_of_interest) = 1;