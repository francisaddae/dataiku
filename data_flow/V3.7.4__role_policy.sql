
-- Role for SI data tables
CREATE SCHEMA IF NOT EXISTS SECURITY;

CREATE TABLE SECURITY.MANAGER_ROW_CALLS(
    analyst_title VARCHAR,
    role_borough VARCHAR
);

INSERT INTO SECURITY.MANAGER_ROW_CALLS(analyst_title, role_borough) VALUES
    ('Brooklyn_Data_Manager', 'Brooklyn'),
    ('Bronx_Data_Manager', 'Bronx'),
    ('Manhattan_Data_Manager', 'Manhattan'),
    ('Queens_Data_Manager', 'Queens'),
    ('StatenIsland_Data_Manager', 'Staten Island');


CREATE OR REPLACE ROW ACCESS POLICY covid_data_policy
AS ( role_borough varchar) RETURNS BOOLEAN ->
    'ACCOUNTADMIN' = CURRENT_ROLE()
    OR EXISTS (
      SELECT 1 FROM MANAGER_ROW_CALLS
        WHERE analyst_title = CURRENT_ROLE()
        AND borough = role_borough
    )
;

--ROW ACCESS POLICY
ALTER TABLE public.all_nyc_covid_reports  security.covid_data_policy
ADD ROW ACCESS POLICY ON(borough);


--- CREATING ROLES
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE ROLE Brooklyn_Data_Manager;
CREATE OR REPLACE ROLE Bronx_Data_Manager;
CREATE OR REPLACE ROLE Manhattan_Data_Manager;
CREATE OR REPLACE ROLE Queens_Data_Manager;
CREATE OR REPLACE ROLE StatenIsland_Data_Manager;

GRANT SELECT ON TABLE SECURITY.MANAGER_ROW_CALLS TO ROLE Brooklyn_Data_Manager;
GRANT SELECT ON TABLE SECURITY.MANAGER_ROW_CALLS TO ROLE Bronx_Data_Manager;
GRANT SELECT ON TABLE SECURITY.MANAGER_ROW_CALLS TO ROLE Manhattan_Data_Manager;
GRANT SELECT ON TABLE SECURITY.MANAGER_ROW_CALLS TO ROLE Queens_Data_Manager;
GRANT SELECT ON TABLE SECURITY.MANAGER_ROW_CALLS TO ROLE StatenIsland_Data_Manager;

GRANT USAGE ON WAREHOUSE CHALLENGE_WH TO Brooklyn_Data_Manager;
GRANT USAGE ON WAREHOUSE CHALLENGE_WH TO Bronx_Data_Manager;
GRANT USAGE ON WAREHOUSE CHALLENGE_WH TO Manhattan_Data_Manager;
GRANT USAGE ON WAREHOUSE CHALLENGE_WH TO Queens_Data_Manager;
GRANT USAGE ON WAREHOUSE CHALLENGE_WH TO  StatenIsland_Data_Manager;

GRANT USAGE ON DATABASE francisDB TO Brooklyn_Data_Manager;
GRANT USAGE ON DATABASE francisDB TO Bronx_Data_Manager;
GRANT USAGE ON DATABASE francisDB TO Manhattan_Data_Manager;
GRANT USAGE ON DATABASE francisDB TO Queens_Data_Manager;
GRANT USAGE ON DATABASE francisDB TO StatenIsland_Data_Manager;

GRANT USAGE ON SCHEMA PUBLIC TO Brooklyn_Data_Manager;
GRANT USAGE ON SCHEMA PUBLIC TO Bronx_Data_Manager;
GRANT USAGE ON SCHEMA PUBLIC TO Manhattan_Data_Manager;
GRANT USAGE ON SCHEMA PUBLIC TO Queens_Data_Manager;
GRANT USAGE ON SCHEMA PUBLIC TO StatenIsland_Data_Manager;


--test
USE  ROLE Brooklyn_Data_Manager;
SELECT *
FROM ALL_NYC_COVID_DATA;

