-- 1. Create the database
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;

-- 2. Create the schema to store your agent objects
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

-- 3. Grant the ability to create agents to your target role
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE accountadmin;  --Change to role used for training user

-- 4. Grant access to your target role so users can interact with agents
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE accountadmin;  --Change to role used for training user

GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE accountadmin;  --Change to role used for training user


ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
