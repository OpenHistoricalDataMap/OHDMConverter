DO $$
DECLARE t TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE E'\nALTER TABLE creationinformation WITH real primary key\n\n';
    ALTER TABLE inter.creationinformation ADD PRIMARY KEY (id);
    RAISE NOTICE E'\nTime spent=%\n', clock_timestamp() -t;

    SELECT clock_timestamp() INTO t;
    RAISE NOTICE E'\nINSERT current timestamp into creationinformation\n\n';
    --set timestampstring in creationinformation
    INSERT INTO inter.creationinformation(timestampstring) VALUES (t);
    RAISE NOTICE E'\nTime spent=%\n\n', clock_timestamp() -t;

    SELECT clock_timestamp() INTO t;
    RAISE NOTICE E'\nALTER all other tables WITH real primary key\n\n';
    -- alter tables with primary keys
    ALTER TABLE inter.nodes ADD PRIMARY KEY (id);
    ALTER TABLE inter.ways ADD PRIMARY KEY (id);
    ALTER TABLE inter.waynodes ADD PRIMARY KEY (id);
    ALTER TABLE inter.relations ADD PRIMARY KEY (id);
    ALTER TABLE inter.relationmembers ADD PRIMARY KEY (id);
    RAISE NOTICE E'\nTime spent=%\n\n', clock_timestamp() -t;

    SELECT clock_timestamp() INTO t;
    RAISE NOTICE E'\nUPDATE osm_object_mapfeatures with feature_value exists\n\n';
    UPDATE inter.osm_object_mapfeatures AS f 
    SET feature_id = (SELECT cla.id FROM misc.classification AS cla WHERE cla."class" = f.feature_key AND cla.subclassname = f.feature_value)
    WHERE f.feature_value IN (SELECT cla.subclassname FROM misc.classification AS cla WHERE cla."class" = f.feature_key);
    RAISE NOTICE E'\nTime spent=%\n\n', clock_timestamp() -t;

    SELECT clock_timestamp() INTO t;
    RAISE NOTICE E'\nUPDATE osm_object_mapfeatures with undefined subclass\n\n';
    UPDATE inter.osm_object_mapfeatures AS f 
    SET feature_id = (SELECT cla.id FROM misc.classification AS cla WHERE cla."class" = f.feature_key AND cla.subclassname = 'undefined') 
    WHERE f.feature_value NOT IN (SELECT cla.subclassname FROM misc.classification AS cla WHERE cla."class" = f.feature_key);
    RAISE NOTICE E'\nTime spent=%\n\n', clock_timestamp() -t;
END;