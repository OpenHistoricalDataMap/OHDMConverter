-- PostgreSQL script that drop old verison of intermediate database and
-- rename updatedb to inter
--
-- NOTE: this script must run with access rights on the database
-- author: SteSad

\timing
-- SET client_min_messages TO WARNING;
DO $$
DECLARE 
    t TIMESTAMP := clock_timestamp();
BEGIN
    DROP SCHEMA IF EXISTS inter CASCADE;
    ALTER SCHEMA updatedb RENAME TO inter;
    RAISE NOTICE E'DROP old "inter" schema and ALTER "updatedb" schema to "inter",\tlasted= %', clock_timestamp() - t;
END $$;