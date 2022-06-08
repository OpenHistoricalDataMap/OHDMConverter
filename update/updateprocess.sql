-- PostgreSQL script thaht contains the update process
-- Compare 2 db with the intermediate schema
--
-- NOTE: this script must run with access rights on the database
-- author: SteSad

\timing
-- SET client_min_messages TO WARNING;
-- DECLARE t TIMESTAMP := clock_timestamp();
DO $$
DECLARE
    t TIMESTAMP := clock_timestamp();
    d1 int := 0;
    d2 int := 0;
BEGIN
    RAISE NOTICE E'Process on reseting flags'
    UPDATE inter.nodes
    SET 
        geom_changed = false, 
        object_changed = false, 
        url_changed = false,
        deleted = false, 
        object_new = false, 
        valid = false
    ;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE inter.ways
    SET 
        geom_changed = false, 
        object_changed = false, 
        url_changed = false,
        deleted = false, 
        object_new = false, 
        valid = false
    ;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE inter.relations
    SET 
        geom_changed = false, 
        object_changed = false, 
        url_changed = false,
        deleted = false, 
        object_new = false, 
        valid = false
    ;
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'Reset Flags in: % rows in intermediate database; \tTime spent=%', d2, clock_timestamp() - t;
    d2 = 0;
--------------------------------------------------------------------------------
    RAISE NOTICE E'Process on unchanged entries'
    SELECT clock_timestamp() INTO t;
    UPDATE updatedb.nodes AS updatenodes
    SET "valid" = true
    FROM inter.nodes AS nodes 
    WHERE nodes.osm_id = updatenodes.osm_id 
    AND nodes.tstamp = updatenodes.tstamp;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.ways AS updateways
    SET "valid" = true
    FROM inter.ways AS ways
    WHERE ways.osm_id = updateways.osm_id 
    AND ways.tstamp = updateways.tstamp;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.relations AS updaterelations
    SET "valid" = true
    FROM inter.relations AS relations
    WHERE relations.osm_id = updaterelations.osm_id 
    AND relations.tstamp = updaterelations.tstamp;
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'UPDATE % rows, there are unchanged entries; \tTime spent=%', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
    RAISE NOTICE E'Process on mark changed geometries'
    SELECT clock_timestamp() INTO t;d2 = 0;
    -- geom column changed?
    UPDATE updatedb.nodes as updatenodes
    SET geom_changed = true
    FROM inter.nodes AS nodes
    WHERE nodes.osm_id = updatenodes.osm_id 
    AND NOT nodes.geom = updatenodes.geom;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.ways as updateways
    SET geom_changed = true
    FROM inter.ways AS ways
    WHERE ways.osm_id = updateways.osm_id 
    AND NOT ways.geom = updateways.geom;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.relations as updaterelations
    SET geom_changed = true
    FROM inter.relations AS relations 
    WHERE relations.osm_id = updaterelations.osm_id 
    AND NOT relations.geom = updaterelations.geom;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    -- member column changed?
    UPDATE updatedb.ways as updateways
    SET geom_changed = true
    FROM inter.ways AS ways
    WHERE ways.osm_id = updateways.osm_id
    AND SIMILARITY(ways.member,updateways.member) < 1;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.relations as updaterelations
    SET geom_changed = true
    FROM inter.relations AS relations
    WHERE relations.osm_id = updaterelations.osm_id
    AND SIMILARITY(relations.member,updaterelations.member) < 1;
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'UPDATE % rows, there have changed geometries; \tTime spent=%', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
    RAISE NOTICE E'Process on mark changed objects'; 
    SELECT clock_timestamp() INTO t;d2 = 0;
    UPDATE updatedb.nodes AS updatenodes
    SET object_changed = true
    FROM inter.nodes AS nodes
    WHERE updatenodes.osm_id = nodes.osm_id 
    AND 
    (
        NOT COALESCE(updatenodes.serializedtags,'') = COALESCE(nodes.serializedtags,'') OR
        NOT updatenodes.tstamp = nodes.tstamp OR
        similarity(updatenodes.mapfeature_ids,nodes.mapfeature_ids) < 1 OR
        NOT COALESCE(updatenodes.uid,'') = COALESCE(nodes.uid,'') OR
        NOT COALESCE(updatenodes.username,'') = COALESCE(nodes.username,'')  OR
        NOT COALESCE(updatenodes.name,'') = COALESCE(nodes.name, '')
    );
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.ways as updateways
    SET object_changed = true
    FROM inter.ways AS ways
    WHERE updateways.osm_id = ways.osm_id 
    AND 
    (
        NOT COALESCE(updateways.serializedtags,'') = COALESCE(ways.serializedtags,'')  OR
        NOT updateways.tstamp = ways.tstamp OR
        similarity(updateways.mapfeature_ids,ways.mapfeature_ids) < 1 OR
        NOT COALESCE(updateways.uid,'') = COALESCE(ways.uid,'')  OR
        NOT COALESCE(updateways.username,'') = COALESCE(ways.username,'')  OR
        NOT updateways.name = COALESCE(ways.name, '')
    );
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.relations as updaterelations
    SET object_changed = true
    FROM inter.relations AS relations
    WHERE updaterelations.osm_id = relations.osm_id 
    AND 
    (
        NOT COALESCE(updaterelations.serializedtags,'') = COALESCE(relations.serializedtags,'')  OR
        NOT updaterelations.tstamp = relations.tstamp OR
        similarity(updaterelations.mapfeature_ids,relations.mapfeature_ids) < 1 OR
        NOT COALESCE(updaterelations.uid,'') = COALESCE(relations.uid,'')  OR
        NOT COALESCE(updaterelations.username,'') = COALESCE(relations.username,'')  OR
        NOT COALESCE(updaterelations.name,'') = COALESCE(relations.name, '')
    );
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'UPDATE % rows, there have changed objects; \tTime spent=%', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
    RAISE NOTICE E'Process on mark changed urls';
    SELECT clock_timestamp() INTO t;d2 = 0;
    UPDATE updatedb.nodes AS updatenodes
    SET url_changed = true
    FROM inter.nodes AS nodes
    WHERE updatenodes.osm_id = nodes.osm_id 
    AND NOT COALESCE(updatenodes.url,'') = COALESCE(nodes.url,'') ;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.ways as updateways
    SET url_changed = true
    FROM inter.ways AS ways
    WHERE updateways.osm_id = ways.osm_id 
    AND NOT COALESCE(updateways.url,'') = COALESCE(ways.url,'') ;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.relations as updaterelations
    SET url_changed = true
    FROM inter.relations AS relations
    WHERE updaterelations.osm_id = relations.osm_id 
    AND NOT COALESCE(updaterelations.url,'') = COALESCE(relations.url,'') ;
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'UPDATE % rows, there have changed urls; \tTime spent=%', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
    RAISE NOTICE E'Process on mark new objects';
    SELECT clock_timestamp() INTO t;d2 = 0;
    UPDATE updatedb.nodes
    SET object_new = true
    WHERE osm_id NOT IN (SELECT osm_id FROM inter.nodes);
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.ways
    SET object_new = true
    WHERE osm_id NOT IN (SELECT osm_id FROM inter.ways);
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.relations
    SET object_new = true
    WHERE osm_id NOT IN (SELECT osm_id FROM inter.relations);
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'UPDATE % rows, there are new; \tTime spent=%', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
    RAISE NOTICE E'Process on INSERT old, delete objects from intermediate to updatedb'; 
    SELECT clock_timestamp() INTO t;d2 = 0;
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
        url_changed,
        deleted,
        object_new,
        has_name,
        valid 
    )
    (
        SELECT 
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
            url_changed,
            true,
            object_new,
            has_name,
            valid 
        FROM inter.nodes AS nodes
        WHERE nodes.osm_id NOT IN (
            SELECT osm_id FROM updatedb.nodes
        )
    );
    GET diagnostics d1 = row_count; d2 = d2 + d1;

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
        url_changed,
        deleted,
        object_new,
        has_name,
        valid 
    )
    (
        SELECT 
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
            url_changed,
            true,object_new,
            has_name,
            valid 
        FROM inter.ways AS ways
        WHERE ways.osm_id NOT IN (
            SELECT osm_id FROM updatedb.ways
        )
    );
    GET diagnostics d1 = row_count; d2 = d2 + d1;

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
        url_changed,
        deleted,
        object_new,
        has_name,
        valid 
    )
    (
        SELECT 
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
            url_changed,
            true,
            object_new,
            has_name,
            valid 
        FROM inter.relations AS relations
        WHERE relations.osm_id NOT IN (
            SELECT osm_id FROM updatedb.relations
        )
    );
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'INSERT % rows, there are marked as deleted; \tTime spent=%', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
    RAISE NOTICE E'Process on Mark all changed or new objects as not valid'; 
    SELECT clock_timestamp() INTO t;d2 = 0;
    UPDATE updatedb.nodes
    SET valid = false
    WHERE geom_changed = true or object_changed = true or object_new = true or url_changed = true;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.ways
    SET valid = false
    WHERE geom_changed = true or object_changed = true or object_new = true or url_changed = true;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.relations
    SET valid = false
    WHERE geom_changed = true or object_changed = true or object_new = true or url_changed = true;
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'INSERT % rows, there are marked as not valid; \tTime spent=%', d2, clock_timestamp() - t;
END $$;

\echo 'Process for udatedb Done\n'

-- after full update replace updatedb with intermediate