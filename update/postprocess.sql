-- PostgreSQL script that must be run after osm2pgsql
-- add current timestamp on creationinformation and
-- add real primary keys on other tables
--
-- NOTE: this script must run with access rights on the database
-- author: SteSad

-- \timing
SET client_min_messages TO WARNING;
DO $$
DECLARE t TIMESTAMP := clock_timestamp();
BEGIN
    ALTER TABLE updatedb.creationinformation ADD PRIMARY KEY (id);
    RAISE NOTICE E'ALTER TABLE creationinformation WITH real primary key\nTime spent=%', clock_timestamp() -t;

    SELECT clock_timestamp() INTO t;
    --set timestampstring in creationinformation
    INSERT INTO updatedb.creationinformation(timestampstring) VALUES (t);
    RAISE NOTICE E'INSERT current timestamp into creationinformation\nTime spent=%', clock_timestamp() -t;

    SELECT clock_timestamp() INTO t;
    -- alter tables with primary keys
    ALTER TABLE updatedb.nodes ADD PRIMARY KEY (id);
    ALTER TABLE updatedb.ways ADD PRIMARY KEY (id);
    ALTER TABLE updatedb.waynodes ADD PRIMARY KEY (id);
    ALTER TABLE updatedb.relations ADD PRIMARY KEY (id);
    ALTER TABLE updatedb.relationmembers ADD PRIMARY KEY (id);
    RAISE NOTICE E'ALTER all other tables WITH real primary key\nTime spent=%', clock_timestamp() -t;
END;
$$