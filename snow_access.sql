--  GRANT DATABASE, SCHEMA & TABLE ACCESS
--  Switch to SYSADMIN for all object-level grants

USE ROLE SYSADMIN;

-- Database
GRANT USAGE ON DATABASE my_db TO ROLE analyst;

-- Schema
GRANT USAGE ON SCHEMA my_db.public TO ROLE analyst;

-- Existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA my_db.public TO ROLE analyst;

-- Future tables (auto-grants as new tables are created)
GRANT SELECT ON FUTURE TABLES IN SCHEMA my_db.public TO ROLE analyst;


--  GRANT WAREHOUSE ACCESS

USE ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE analyst;

--  ASSIGN ROLE TO USER
--  Back to SECURITYADMIN for user-role assignment

USE ROLE SECURITYADMIN;

GRANT ROLE analyst TO USER john_doe;

--  VERIFY EVERYTHING

USE ROLE SECURITYADMIN;

-- Check user was created
DESC USER john_doe;

-- Check roles assigned to user
SHOW GRANTS TO USER john_doe;

-- Check all privileges granted to the role
SHOW GRANTS TO ROLE analyst;

-- Check warehouse grants
SHOW GRANTS ON WAREHOUSE compute_wh;








USE ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL WAREHOUSES IN ACCOUNT TO ROLE ORGADMIN;

-- or if above doesn't work, try granting on each warehouse

GRANT ALL PRIVILEGES ON ALL WAREHOUSES TO ROLE ORGADMIN;



-- Databases privileges
USE ROLE SYSADMIN;
-- All Databases
GRANT ALL PRIVILEGES ON ALL DATABASES TO ROLE ORGADMIN;

-- All Schemas (current)
GRANT ALL PRIVILEGES ON ALL SCHEMAS TO ROLE ORGADMIN;

-- All Tables (current)
GRANT ALL PRIVILEGES ON ALL TABLES TO ROLE ORGADMIN;

-- All Views (current)
GRANT ALL PRIVILEGES ON ALL VIEWS TO ROLE ORGADMIN;


-- Future Databases, Schemas, Tables & Views (auto-grant)
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS   IN ACCOUNT TO ROLE ORGADMIN;
GRANT ALL PRIVILEGES ON FUTURE TABLES    IN ACCOUNT TO ROLE ORGADMIN;
GRANT ALL PRIVILEGES ON FUTURE VIEWS     IN ACCOUNT TO ROLE ORGADMIN;