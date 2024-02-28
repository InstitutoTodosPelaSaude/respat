\c itps;
CREATE SCHEMA respiratorios;
GRANT ALL PRIVILEGES ON DATABASE itps TO itps;

CREATE EXTENSION unaccent WITH SCHEMA respiratorios;

CREATE DATABASE itps_dev;
\c itps_dev;

CREATE USER itps_dev WITH PASSWORD '<ITPS_DEV_PASS>';
GRANT ALL PRIVILEGES ON DATABASE itps_dev TO itps_dev;

CREATE SCHEMA respiratorios;
GRANT ALL PRIVILEGES ON SCHEMA respiratorios TO itps_dev;
CREATE EXTENSION unaccent WITH SCHEMA respiratorios;

CREATE DATABASE dagster;
\c dagster;
CREATE SCHEMA dagster;
CREATE USER dagster WITH PASSWORD '<DAGSTER_PASS>';
ALTER DATABASE dagster OWNER TO dagster;
GRANT ALL PRIVILEGES ON DATABASE dagster TO dagster;
GRANT ALL PRIVILEGES ON SCHEMA public TO dagster;
GRANT ALL PRIVILEGES ON SCHEMA dagster TO dagster;

ALTER DATABASE dagster SET search_path TO dagster;
ALTER USER dagster SET search_path TO dagster;