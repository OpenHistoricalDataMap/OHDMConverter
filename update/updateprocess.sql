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

    UPDATE inter.ways AS ways
    SET "valid" = true
    FROM updatedb.ways AS updateways
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
    AND NOT updatenodes.serializedtags = nodes.serializedtags;

    UPDATE updatedb.ways as updateways
    SET object_changed = true
    FROM inter.ways AS ways
    WHERE updateways.osm_id = ways.osm_id 
    AND NOT updateways.serializedtags = ways.serializedtags;

    UPDATE updatedb.relations as updaterelations
    SET object_changed = true
    FROM inter.relations AS relations
    WHERE updaterelations.osm_id = relations.osm_id 
    AND NOT updaterelations.serializedtags = relations.serializedtags;

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
    WHERE osm_id NOT IN (SELECT osm_id FROM inter.nodes);

    UPDATE updatedb.relations
    SET object_new = true
    WHERE osm_id NOT IN (SELECT osm_id FROM inter.nodes);

END;
$$;

-- after full update replace updatedb with intermediate