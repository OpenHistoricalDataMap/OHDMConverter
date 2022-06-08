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
    ALTER TABLE inter.creationinformation ADD PRIMARY KEY (id);
    INSERT INTO inter.creationinformation(timestampstring) VALUES (t);
    ALTER TABLE inter.nodes ADD PRIMARY KEY (id);
    ALTER TABLE inter.ways ADD PRIMARY KEY (id);
    ALTER TABLE inter.waynodes ADD PRIMARY KEY (id);
    ALTER TABLE inter.relations ADD PRIMARY KEY (id);
    ALTER TABLE inter.relationmembers ADD PRIMARY KEY (id);
END $$;
\echo 'Postprocess done\n\n'
