-- PostgreSQL script thaht contains the update process
-- Compare 2 db with the intermediate schema
--
-- NOTE: this script must run with access rights on the database
-- author: SteSad

\timing
-- SET client_min_messages TO WARNING;
-- DECLARE t TIMESTAMP := clock_timestamp();
DO $$
BEGIN
    RAISE NOTICE E'Reset Flags in intermediate db';
    UPDATE inter.nodes
    SET 
        geom_changed = false, 
        object_changed = false, 
        deleted = false, 
        object_new = false, 
        valid = false
    ;

    UPDATE inter.ways
    SET 
        geom_changed = false, 
        object_changed = false, 
        deleted = false, 
        object_new = false, 
        valid = false
    ;

    UPDATE inter.relations
    SET 
        geom_changed = false, 
        object_changed = false, 
        deleted = false, 
        object_new = false, 
        valid = false
    ;

END;
$$;

DO $$
BEGIN
    RAISE NOTICE E'Search for unchanged entries'; 
    UPDATE updatedb.nodes AS updatenodes
    SET "valid" = true
    FROM inter.nodes AS nodes 
    WHERE nodes.osm_id = updatenodes.osm_id 
    AND nodes.tstamp = updatenodes.tstamp;

    UPDATE updatedb.ways AS updateways
    SET "valid" = true
    FROM inter.ways AS ways
    WHERE ways.osm_id = updateways.osm_id 
    AND ways.tstamp = updateways.tstamp;

    UPDATE updatedb.relations AS updaterelations
    SET "valid" = true
    FROM inter.relations AS relations
    WHERE relations.osm_id = updaterelations.osm_id 
    AND relations.tstamp = updaterelations.tstamp;

END;
$$;

DO $$
BEGIN
    RAISE NOTICE E'Mark geometry changed'; 

    -- geom column changed?
    UPDATE updatedb.nodes as updatenodes
    SET geom_changed = true
    FROM inter.nodes AS nodes
    WHERE nodes.osm_id = updatenodes.osm_id 
    AND NOT nodes.geom = updatenodes.geom;

    UPDATE updatedb.ways as updateways
    SET geom_changed = true
    FROM inter.ways AS ways
    WHERE ways.osm_id = updateways.osm_id 
    AND NOT ways.geom = updateways.geom;

    UPDATE updatedb.relations as updaterelations
    SET geom_changed = true
    FROM inter.relations AS relations 
    WHERE relations.osm_id = updaterelations.osm_id 
    AND NOT relations.geom = updaterelations.geom;

    -- member column changed?
    UPDATE updatedb.ways as updateways
    SET geom_changed = true
    FROM inter.ways AS ways
    WHERE ways.osm_id = updateways.osm_id
    AND NOT ways.member = updateways.member;

    UPDATE updatedb.relations as updaterelations
    SET geom_changed = true
    FROM inter.relations AS relations
    WHERE relations.osm_id = updaterelations.osm_id
    AND NOT relations.member = updaterelations.member;

    

END;
$$;

DO $$
BEGIN
    RAISE NOTICE E'Mark object changed'; 

    UPDATE updatedb.nodes AS updatenodes
    SET object_changed = true
    FROM inter.nodes AS nodes
    WHERE updatenodes.osm_id = nodes.osm_id 
    AND 
    (
        NOT updatenodes.serializedtags = nodes.serializedtags OR
        NOT updatenodes.tstamp = nodes.tstamp OR
        similarity(updatenodes.mapfeature_ids,nodes.mapfeature_ids) < 1 OR
        NOT updatenodes.uid = nodes.uid OR
        NOT updatenodes.username = nodes.username OR
        NOT updatenodes.name = nodes.name OR
        NOT updatenodes.url = nodes.url
    );

    UPDATE updatedb.ways as updateways
    SET object_changed = true
    FROM inter.ways AS ways
    WHERE updateways.osm_id = ways.osm_id 
    AND 
    (
        NOT updateways.serializedtags = ways.serializedtags OR
        NOT updateways.tstamp = ways.tstamp OR
        similarity(updateways.mapfeature_ids,ways.mapfeature_ids) < 1 OR
        NOT updateways.uid = ways.uid OR
        NOT updateways.username = ways.username OR
        NOT updateways.name = ways.name OR
        NOT updateways.url = ways.url
    );

    UPDATE updatedb.relations as updaterelations
    SET object_changed = true
    FROM inter.relations AS relations
    WHERE updaterelations.osm_id = relations.osm_id 
    AND 
    (
        NOT updaterelations.serializedtags = relations.serializedtags OR
        NOT updaterelations.tstamp = relations.tstamp OR
        similarity(updaterelations.mapfeature_ids,relations.mapfeature_ids) < 1 OR
        NOT updaterelations.uid = relations.uid OR
        NOT updaterelations.username = relations.username OR
        NOT updaterelations.name = relations.name OR
        NOT updaterelations.url = relations.url
    );

END;
$$;

DO $$
BEGIN
    RAISE NOTICE E'Mark new object'; 

    UPDATE updatedb.nodes
    SET object_new = true
    WHERE osm_id NOT IN (SELECT osm_id FROM inter.nodes);

    UPDATE updatedb.ways
    SET object_new = true
    WHERE osm_id NOT IN (SELECT osm_id FROM inter.ways);

    UPDATE updatedb.relations
    SET object_new = true
    WHERE osm_id NOT IN (SELECT osm_id FROM inter.relations);

END;
$$;

DO $$
BEGIN
    RAISE NOTICE E'Insert delete objects in updatedb'; 
    INSERT INTO updatedb.nodes (
        osm_id,
        tstamp,
        mapfeature_ids,
        serializedtags,
        geom,
        uid,
        username,
        name,
        url,
        geom_changed,
        object_changed,
        deleted,
        object_new,
        has_name,
        valid 
    )
    (
        SELECT osm_id,tstamp,mapfeature_ids,serializedtags,geom,uid,username,name,url,geom_changed,object_changed,true,object_new,has_name,valid 
        FROM inter.nodes AS nodes
        WHERE nodes.osm_id NOT IN (
            SELECT osm_id FROM updatedb.nodes
        )
    );

    INSERT INTO updatedb.ways (
        osm_id,
        tstamp,
        mapfeature_ids,
        serializedtags,
        geom,
        uid,
        username,
        name,
        url,
        member,
        geom_changed,
        object_changed,
        deleted,
        object_new,
        has_name,
        valid 
    )
    (
        SELECT osm_id,tstamp,mapfeature_ids,serializedtags,geom,uid,username,name,url,member,geom_changed,object_changed,true,object_new,has_name,valid 
        FROM inter.ways AS ways
        WHERE ways.osm_id NOT IN (
            SELECT osm_id FROM updatedb.ways
        )
    );

    INSERT INTO updatedb.relations (
        osm_id,
        tstamp,
        mapfeature_ids,
        serializedtags,
        geom,
        uid,
        username,
        name,
        url,
        member,
        geom_changed,
        object_changed,
        deleted,
        object_new,
        has_name,
        valid 
    )
    (
        SELECT osm_id,tstamp,mapfeature_ids,serializedtags,geom,uid,username,name,url,member,geom_changed,object_changed,true,object_new,has_name,valid 
        FROM inter.relations AS relations
        WHERE relations.osm_id NOT IN (
            SELECT osm_id FROM updatedb.relations
        )
    );

END;
$$;

DO $$
BEGIN
    RAISE NOTICE E'Mark all changed or new objects as not valid'; 

    UPDATE updatedb.nodes
    SET valid = false
    WHERE geom_changed = true or object_changed = true or object_new = true;

    UPDATE updatedb.ways
    SET valid = false
    WHERE geom_changed = true or object_changed = true or object_new = true;

    UPDATE updatedb.relations
    SET valid = false
    WHERE geom_changed = true or object_changed = true or object_new = true;

END;
$$;

-- after full update replace updatedb with intermediate