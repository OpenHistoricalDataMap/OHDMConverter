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


    SELECT clock_timestamp() INTO t;
    -- admin_level BETWEEN 1 - 12
    UPDATE inter.osm_object_mapfeatures AS features SET feature_id =
    (
        SELECT cla.id FROM misc.classification AS cla
        WHERE
	        cla.classname LIKE 'ohdm%' AND
	        features.feature_key LIKE 'admin%' AND
            features.feature_value ~ '[1-9]?[0-2]?' AND
            cla.subclassname = CONCAT('adminlevel_', features.feature_value)
    );

    -- admin_level undefined
    UPDATE inter.osm_object_mapfeatures AS features SET feature_id =
    (
        SELECT cla.id FROM misc.classification AS cla
        WHERE
	        cla.classname LIKE 'ohdm%' AND
	        features.feature_key LIKE 'admin%' AND
            cla.subclassname = 'undefined'
    );

    -- mapfeature and subfeature exists
    UPDATE inter.osm_object_mapfeatures AS features SET feature_id =
    (
        SELECT cla.id FROM misc.classification AS cla
        WHERE
            cla.classname = features.feature_key AND
	        cla.subclassname = features.feature_value
    );
    
    -- mapfeature and subfeature is undefined
    UPDATE inter.osm_object_mapfeatures AS features SET feature_id =
    (   SELECT cla.id FROM misc.classification AS cla
        WHERE
            cla.classname = features.feature_key AND
            features.feature_value NOTNULL AND
	        cla.subclassname = 'undefined'
    )
    WHERE features.feature_value NOT IN (SELECT subclassname FROM misc.classification);
    
    RAISE NOTICE E'\nUPDATE osm_object_mapfeatures\nTime spent=%\n\n', clock_timestamp() -t;
    
    SELECT clock_timestamp() INTO t;

    -- node.osm_id exist in cross-reference table
    UPDATE inter.nodes AS nodes SET mapfeatures_ids =
    (
        SELECT string_agg(oom.feature_id::VARCHAR, '; ') FROM inter.osm_object_mapfeatures AS oom
        WHERE nodes.osm_id = oom.osm_id
    );


    SELECT clock_timestamp() INTO t;
    -- node.osm_id exist in cross-reference table
    UPDATE inter.nodes AS nodes SET mapfeatures_ids = '-1'
    WHERE nodes.osm_id NOT IN (SELECT osm_id FROM inter.osm_object_mapfeatures);
    RAISE NOTICE E'\nUPDATE nodes.mapfeatures_ids\nTime spent=%\n\n', clock_timestamp() -t;

    SELECT clock_timestamp() INTO t;
    -- way.osm_id exist in cross-reference table
    UPDATE inter.ways AS ways SET mapfeatures_ids =
    (
        SELECT string_agg(oom.feature_id::VARCHAR, '; ') FROM inter.osm_object_mapfeatures AS oom
        WHERE
            ways.osm_id = oom.osm_id
    );

    -- way.osm_id exist in cross-reference table
    UPDATE inter.ways AS ways SET mapfeatures_ids = '-1'
    WHERE ways.osm_id NOT IN (SELECT osm_id FROM inter.osm_object_mapfeatures);
    RAISE NOTICE E'\nUPDATE ways.mapfeatures_ids\nTime spent=%\n\n', clock_timestamp() -t;

    SELECT clock_timestamp() INTO t;
    -- relation.osm_id exist in cross-reference table
    UPDATE inter.relations AS relations SET mapfeatures_ids =
    (
        SELECT string_agg(oom.feature_id::VARCHAR, '; ') FROM inter.osm_object_mapfeatures AS oom
        WHERE
            relations.osm_id = oom.osm_id
    );

    -- relation.osm_id exist in cross-reference table
    UPDATE inter.relations AS relations SET mapfeatures_ids = '-1'
    WHERE relations.osm_id NOT IN (SELECT osm_id FROM inter.osm_object_mapfeatures);

    RAISE NOTICE E'\nUPDATE relations.mapfeatures_ids\nTime spent=%\n\n', clock_timestamp() -t;


END;
$$