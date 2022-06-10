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
    RAISE NOTICE E'UPDATE % row(s) in intermediate database to reset flags,\tlasted= %', d2, clock_timestamp() - t;
    d2 = 0;
--------------------------------------------------------------------------------
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
    RAISE NOTICE E'UPDATE % row(s), there have changed geometries,\tlasted= %', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
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
    RAISE NOTICE E'UPDATE % row(s), there have changed objects,\tlasted= %', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
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
    RAISE NOTICE E'UPDATE % row(s), there have changed urls,\tlasted= %', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
    SELECT clock_timestamp() INTO t;d2 = 0;
    UPDATE updatedb.nodes AS nodes
    SET object_new = true
    FROM (
        SELECT u.osm_id, u.tstamp FROM updatedb.nodes AS u
        LEFT JOIN inter.nodes AS i ON i.osm_id = u.osm_id
        WHERE i.osm_id ISNULL
    ) AS new_objects
    WHERE nodes.osm_id = new_objects.osm_id;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.ways AS ways
    SET object_new = true
    FROM (
        SELECT u.osm_id, u.tstamp FROM updatedb.ways AS u
        LEFT JOIN inter.ways AS i ON i.osm_id = u.osm_id
        WHERE i.osm_id ISNULL
    ) AS new_objects
    WHERE ways.osm_id = new_objects.osm_id;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.relations AS relations
    SET object_new = true
    FROM (
        SELECT u.osm_id, u.tstamp FROM updatedb.relations AS u
        LEFT JOIN inter.relations AS i ON i.osm_id = u.osm_id
        WHERE i.osm_id ISNULL
    ) AS new_objects
    WHERE relations.osm_id = new_objects.osm_id;
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'UPDATE % row(s), there are new,\tlasted= %', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
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
            nodes.osm_id,
            nodes.tstamp,
            nodes.mapfeature_ids,
            nodes.serializedtags,
            nodes.geom,
            nodes.uid,
            nodes.username,
            nodes.name,
            nodes.url,
            nodes.geom_changed,
            nodes.object_changed,
            nodes.url_changed,
            true,
            nodes.object_new,
            nodes.has_name,
            nodes.valid 
        FROM inter.nodes AS nodes
        LEFT JOIN updatedb.nodes AS updatenodes ON updatenodes.osm_id = nodes.osm_id
        WHERE nodes.osm_id ISNULL
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
            ways.osm_id,
            ways.tstamp,
            ways.mapfeature_ids,
            ways.serializedtags,
            ways.geom,
            ways.uid,
            ways.username,
            ways.name,
            ways.url,
            ways.member,
            ways.geom_changed,
            ways.object_changed,
            ways.url_changed,
            true,
            ways.object_new,
            ways.has_name,
            ways.valid 
        FROM inter.ways AS ways
        LEFT JOIN updatedb.ways AS updateways ON updateways.osm_id = ways.osm_id
        WHERE updateways.osm_id ISNULL
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
            relations.osm_id,
            relations.tstamp,
            relations.mapfeature_ids,
            relations.serializedtags,
            relations.geom,
            relations.uid,
            relations.username,
            relations.name,
            relations.url,
            relations.member,
            relations.geom_changed,
            relations.object_changed,
            relations.url_changed,
            true,
            relations.object_new,
            relations.has_name,
            relations.valid             
        FROM inter.relations AS relations
        LEFT JOIN updatedb.relations AS updaterelations ON updaterelations.osm_id = relations.osm_id
        WHERE updaterelations.osm_id ISNULL
    );
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'INSERT % row(s), there are marked as deleted,\tlasted= %', d2, clock_timestamp() - t;
--------------------------------------------------------------------------------
    SELECT clock_timestamp() INTO t;d2 = 0;    
    UPDATE updatedb.nodes
    SET valid = true
    WHERE geom_changed = false 
        AND object_changed = false 
        AND object_new = false 
        AND url_changed = false 
        AND deleted = false;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.ways
    SET valid = true
    WHERE geom_changed = false 
        AND object_changed = false 
        AND object_new = false 
        AND url_changed = false 
        AND deleted = false;
    GET diagnostics d1 = row_count; d2 = d2 + d1;

    UPDATE updatedb.relations
    SET valid = true
    WHERE geom_changed = false 
        AND object_changed = false 
        AND object_new = false 
        AND url_changed = false 
        AND deleted = false;
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'UPDATE % row(s), there are unchanged entries,\tlasted= %', d2, clock_timestamp() - t;
END $$;

\echo 'Process for updatedb Done\n'