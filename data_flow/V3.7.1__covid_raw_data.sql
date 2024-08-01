
-- This file will create the databse and schema and will specify which schema to use
-- CREATE DATABASE IF NOT EXISTS francisDB;
-- CREATE SCHEMA IF NOT EXISTS francisDB.present;

USE SCHEMA francisDB.Public;

USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR LIMITER
  WITH CREDIT_QUOTA = 100
  TRIGGERS ON 100 PERCENT DO SUSPEND_IMMEDIATE;

CREATE OR REPLACE WAREHOUSE CHALLENGE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 10
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD';


CREATE OR REPLACE NETWORK RULE api_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('data.cityofnewyork.us');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION apis_access_integration
ALLOWED_NETWORK_RULES = (api_network_rule)
ENABLED = true;



--extract function
CREATE OR REPLACE FUNCTION extract_raw_data()
RETURNS TABLE(date_of_interest VARCHAR, case_count NUMBER(10,2), probable_case_count NUMBER(10,2), hospitalized_count NUMBER(10,2), death_count NUMBER(10,2),
            bx_case_count NUMBER(10,2), bx_probable_case_count NUMBER(10,2), bx_hospitalized_count NUMBER(10,2), bx_death_count NUMBER(10,2),
            bk_case_count NUMBER(10,2), bk_probable_case_count NUMBER(10,2), bk_hospitalized_count NUMBER(10,2), bk_death_count NUMBER(10,2),
            mn_case_count NUMBER(10,2), mn_probable_case_count NUMBER(10,2), mn_hospitalized_count NUMBER(10,2), mn_death_count NUMBER(10,2),
            qn_case_count NUMBER(10,2), qn_probable_case_count NUMBER(10,2), qn_hospitalized_count NUMBER(10,2), qn_death_count NUMBER(10,2),
            si_case_count NUMBER(10,2), si_probable_case_count NUMBER(10,2), si_hospitalized_count NUMBER(10,2), si_death_count NUMBER(10,2),
            total_borough_case_count  NUMBER(10,2), total_borough_probable_count  NUMBER(10,2))
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
HANDLER='ApiData'
EXTERNAL_ACCESS_INTEGRATIONS = (apis_access_integration)
packages=('requests') AS

$$
import requests as req

class ApiData:
    # Collect raw data from NYC Covid Public Data
    def process(self):

        api_url='https://data.cityofnewyork.us/resource/rc75-m7u3.json?$limit=2000'

        #accessing data from api
        response = req.get(api_url)

        data = response.json()

        for row in data:

            #aggregate of all case_count for all boroughs vs probable
            total_borough_case_count =  int(row['bx_case_count']) +  int(row['bk_case_count']) +  int(row['mn_case_count']) +  int(row['qn_case_count']) +  int(row['si_case_count'])
            total_borough_probable_count = int(row['bx_probable_case_count']) +  int(row['bk_probable_case_count']) +  int(row['mn_probable_case_count']) +  int(row['qn_probable_case_count']) +  int(row['si_probable_case_count'])

            #cleanup date column
            date_of_interest = row['date_of_interest'].split('T')[0]

            yield(date_of_interest, row['case_count'],  row['probable_case_count'],  row['hospitalized_count'],  row['death_count'],  row['bx_case_count'],
                 row['bx_probable_case_count'],  row['bx_hospitalized_count'],  row['bx_death_count'],  row['bk_case_count'],  row['bk_probable_case_count'],
                 row['bk_hospitalized_count'],  row['bk_death_count'],  row['mn_case_count'],  row['mn_probable_case_count'],  row['mn_hospitalized_count'],
                 row['mn_death_count'], row['qn_case_count'],  row['qn_probable_case_count'],  row['qn_hospitalized_count'],  row['qn_death_count'],
                 row['si_case_count'],  row['si_probable_case_count'],  row['si_hospitalized_count'],  row['si_death_count'], total_borough_case_count, total_borough_probable_count)

$$;

--Creating based data
CREATE OR REPLACE TABLE NYC_COVID_DATA
AS
    SELECT *
    FROM TABLE(extract_raw_data())
    ORDER BY 1;

-- Using a stored procedute to clean up table
USE SCHEMA francisDB.PRESENT;

CREATE OR REPLACE PROCEDURE clean_base_table()
    RETURNS TABLE(date_of_interest DATE, case_count NUMBER(10,2), probable_case_count NUMBER(10,2), hospitalized_count NUMBER(10,2), death_count NUMBER(10,2),
            bx_case_count NUMBER(10,2), bx_probable_case_count NUMBER(10,2), bx_hospitalized_count NUMBER(10,2), bx_death_count NUMBER(10,2),
            bk_case_count NUMBER(10,2), bk_probable_case_count NUMBER(10,2), bk_hospitalized_count NUMBER(10,2), bk_death_count NUMBER(10,2),
            mn_case_count NUMBER(10,2), mn_probable_case_count NUMBER(10,2), mn_hospitalized_count NUMBER(10,2), mn_death_count NUMBER(10,2),
            qn_case_count NUMBER(10,2), qn_probable_case_count NUMBER(10,2), qn_hospitalized_count NUMBER(10,2), qn_death_count NUMBER(10,2),
            si_case_count NUMBER(10,2), si_probable_case_count NUMBER(10,2), si_hospitalized_count NUMBER(10,2), si_death_count NUMBER(10,2),
            total_borough_case_count  NUMBER(10,2), total_borough_probable_count  NUMBER(10,2))
LANGUAGE SQL
AS
BEGIN
    ALTER TABLE NYC_COVID_DATA ALTER date_of_interest SET DATA TYPE DATE;
    RETURN 'THIS IS THE OUTPUT FOR THE CLEAN BASE TABLE STORED PROCEDURE';
END;


CALL clean_base_table();


