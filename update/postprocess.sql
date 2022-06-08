-- PostgreSQL script that must be run after osm2pgsql
-- add current timestamp on creationinformation and
-- add real primary keys on other tables
--
-- NOTE: this script must run with access rights on the database
-- author: SteSad

-- \timing
-- SET client_min_messages TO WARNING;
DO $$
DECLARE 
    t TIMESTAMP := clock_timestamp();
BEGIN
    ALTER TABLE updatedb.creationinformation ADD PRIMARY KEY (id);
    INSERT INTO updatedb.creationinformation(timestampstring) VALUES (t);
    ALTER TABLE updatedb.nodes ADD PRIMARY KEY (id);
    ALTER TABLE updatedb.ways ADD PRIMARY KEY (id);
    ALTER TABLE updatedb.waynodes ADD PRIMARY KEY (id);
    ALTER TABLE updatedb.relations ADD PRIMARY KEY (id);
    ALTER TABLE updatedb.relationmembers ADD PRIMARY KEY (id);
END $$;
\echo '\n'