--  STEP 1: CREATE ROLE
--  Always use SECURITYADMIN for user & role management
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS data_analyst;

-- Keep role in hierarchy so SYSADMIN can manage it
GRANT ROLE analyst TO ROLE SYSADMIN;

--  STEP 2: CREATE USER

USE ROLE SECURITYADMIN;

CREATE USER IF NOT EXISTS sereti
  PASSWORD          = 'StrongPassword123!'
  LOGIN_NAME        = 'sereti'
  DISPLAY_NAME      = 'Sereti'
  EMAIL             = 'analystsereti@gmail.com'
  DEFAULT_ROLE      = 'data_analyst'
  DEFAULT_WAREHOUSE = 'compute_wh'
  DEFAULT_NAMESPACE = 'my_db.public'
  MUST_CHANGE_PASSWORD = TRUE;