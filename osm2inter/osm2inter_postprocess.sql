DO $$
DECLARE t TIMESTAMP := clock_timestamp();
BEGIN
    ALTER TABLE inter.creationinformation ADD PRIMARY KEY (id);
    RAISE NOTICE E'\nALTER TABLE creationinformation WITH real primary key\nTime spent=%\n', clock_timestamp() -t;

    SELECT clock_timestamp() INTO t;
    --set timestampstring in creationinformation
    INSERT INTO inter.creationinformation(timestampstring) VALUES (t);
    RAISE NOTICE E'\nINSERT current timestamp into creationinformation\nTime spent=%\n\n', clock_timestamp() -t;

    SELECT clock_timestamp() INTO t;
    -- alter tables with primary keys
    ALTER TABLE inter.nodes ADD PRIMARY KEY (id);
    ALTER TABLE inter.ways ADD PRIMARY KEY (id);
    ALTER TABLE inter.waynodes ADD PRIMARY KEY (id);
    ALTER TABLE inter.relations ADD PRIMARY KEY (id);
    ALTER TABLE inter.relationmembers ADD PRIMARY KEY (id);
    RAISE NOTICE E'\nALTER all other tables WITH real primary key\nTime spent=%\n\n', clock_timestamp() -t;
END;
$$