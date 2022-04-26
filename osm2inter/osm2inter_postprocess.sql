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
        SELECT cla.id FROM inter.osm_object_mapfeatures AS oom, misc.classification AS cla
        WHERE
            oom.osm_id = features.osm_id AND
	        cla.classname LIKE 'ohdm%' AND 
	        oom.feature_key LIKE 'admin%' AND
            oom.feature_value ~ '[1-9]?[0-2]?' AND
            cla.subclassname = CONCAT('adminlevel_', feature_value)
    );

    -- admin_level undefined
    UPDATE inter.osm_object_mapfeatures AS features SET feature_id =
    (
        SELECT cla.id FROM inter.osm_object_mapfeatures AS oom, misc.classification AS cla
        WHERE
            oom.osm_id = features.osm_id AND
	        cla.classname LIKE 'ohdm%' AND 
	        oom.feature_key LIKE 'admin%' AND
            cla.subclassname = 'undefined'
    );

    -- mapfeature and subfeature exists
    UPDATE inter.osm_object_mapfeatures AS features SET feature_id =
    (
        SELECT cla.id FROM inter.osm_object_mapfeatures AS oom, misc.classification AS cla
        WHERE
            oom.osm_id = features.osm_id AND
            cla.classname = oom.feature_key AND
	        cla.subclassname = oom.feature_value
    );
    
    -- mapfeature and subfeature is undefined
    UPDATE inter.osm_object_mapfeatures AS features SET feature_id =
    (   SELECT cla.id FROM inter.osm_object_mapfeatures AS oom, misc.classification AS cla
        WHERE
            oom.osm_id = features.osm_id AND
            cla.classname = oom.feature_key AND
            oom.feature_value NOTNULL AND
	        cla.subclassname = 'undefined'
    );
    
    RAISE NOTICE E'\nUPDATE osm_object_mapfeatures\nTime spent=%\n\n', clock_timestamp() -t;
    
    SELECT clock_timestamp() INTO t;

    -- node.osm_id exist in cross-reference table
    UPDATE inter.nodes AS nodes SET mapfeatures_ids = 
    (
        SELECT oom.feature_id FROM inter.nodes AS n ,inter.osm_object_mapfeatures AS oom
        WHERE nodes.osm_id = n.osm_id = oom.osm_id
    );

    -- node.osm_id exist in cross-reference table
    UPDATE inter.nodes AS n SET mapfeatures_ids = 
    (
        CASE WHEN n.osm_id NOT IN (SELECT osm_id FROM inter.osm_object_mapfeatures) THEN '-1' END
    );


    RAISE NOTICE E'\nUPDATE mapfeatures_ids\nTime spent=%\n\n', clock_timestamp() -t;


END;
$$