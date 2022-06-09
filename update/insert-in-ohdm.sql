-- PostgreSQL script that update and insert entries in the OHDM db
--
-- NOTE: this script must run with access rights on the database
-- author: SteSad

\timing
-- SET client_min_messages TO WARNING;
-- DECLARE t TIMESTAMP := clock_timestamp();
DO $$
DECLARE 
    current timestamp := CURRENT_TIMESTAMP + interval '1 day';
    t TIMESTAMP := clock_timestamp();
    d1 int := 0;
    d2 int := 0;
BEGIN
-- Insert new osm users
    INSERT INTO ohdm.external_users(userid, username)
    (
        SELECT uid, username FROM (
            SELECT uid::BIGINT,username 
            FROM updatedb.nodes 
            WHERE valid = false AND (object_changed = true or object_new = true or geom_changed = true)
                UNION
            SELECT uid::BIGINT,username 
            FROM updatedb.ways 
            WHERE valid = false AND (object_changed = true or object_new = true)
                UNION
            SELECT uid::BIGINT,username 
            FROM updatedb.relations 
            WHERE valid = false AND (object_changed = true or object_new = true)
        ) AS users
        WHERE users.uid NOT IN (SELECT userid FROM ohdm.external_users)
    );
    GET diagnostics d1 = row_count;


-- Update external_systems_id from inserted osm users
    UPDATE ohdm.external_users SET external_systems_id =
    (
        SELECT id
        FROM ohdm.external_systems
        WHERE source_name = 'osm'
    )
    WHERE external_systems_id ISNULL;
    RAISE NOTICE E'INSERT % row(s) in "external_users",\tlasted= %', d1, clock_timestamp() - t;

-- Insert new point geometries
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.points(point, source_user_id)
    (
        SELECT nodes.geom, users.id 
        FROM updatedb.nodes AS nodes
        JOIN ohdm.external_users AS users ON nodes.uid::BIGINT = users.userid
        WHERE nodes.valid = false 
        AND nodes.deleted = false 
        AND (nodes.object_new = true or nodes.geom_changed = true)
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.points as points
            WHERE points.point = nodes.geom
            AND points.source_user_id = users.id
        )
    );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "points",\tlasted= %', d1, clock_timestamp() - t; 

-- Insert new line geometries
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.lines(line, source_user_id)
    (
        SELECT ways.geom, users.id 
        FROM updatedb.ways AS ways
        JOIN ohdm.external_users AS users ON ways.uid::BIGINT = users.userid
        WHERE ways.valid = false 
        AND ways.deleted = false 
        AND (ways.object_new = true or ways.geom_changed = true)
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.lines as lines
            WHERE lines.line = ways.geom
            AND lines.source_user_id = users.id
        )

    );   
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "lines",\tlasted= %', d1, clock_timestamp() - t; 
    
-- Insert new polygone geometries
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.polygons(polygon, source_user_id)
    (
        SELECT relations.geom, users.id 
        FROM updatedb.relations AS relations
        JOIN ohdm.external_users AS users ON relations.uid::BIGINT = users.userid
        WHERE relations.valid = false 
        AND relations.deleted = false 
        AND (relations.object_new = true or relations.geom_changed = true)
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.polygons as polygons
            WHERE polygons.polygon = relations.geom
            AND polygons.source_user_id = users.id
        )
    ); 
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "polygons",\tlasted= %', d1, clock_timestamp() - t; 

-- Insert new geoobjects
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.geoobject(geoobject_name, mapfeature_ids, source_user_id) 
    (
        SELECT name, mapfeature_ids, users.id FROM (
            SELECT uid::BIGINT, name, mapfeature_ids
            FROM updatedb.nodes AS nodes
            WHERE nodes.valid = false 
            AND nodes.deleted = false 
            AND (nodes.object_new = true or nodes.object_changed = true)
                UNION
            SELECT uid::BIGINT, name, mapfeature_ids
            FROM updatedb.ways AS ways
            WHERE ways.valid = false 
            AND ways.deleted = false 
            AND (ways.object_new = true or ways.object_changed = true)
                UNION
            SELECT uid::BIGINT, name, mapfeature_ids
            FROM updatedb.relations AS relations
            WHERE relations.valid = false 
            AND relations.deleted = false 
            AND (relations.object_new = true or relations.object_changed = true)
        ) AS geoob
        JOIN ohdm.external_users AS users ON geoob.uid = users.userid
        WHERE geoob.name NOTNULL
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.geoobject AS geoobject
            WHERE geoobject.geoobject_name = geoob.name
            AND geoobject.mapfeature_ids = geoob.mapfeature_ids
            AND geoobject.source_user_id = users.id
        )
    );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "geoobject",\tlasted= %', d1, clock_timestamp() - t;

-- Insert new content
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.content(content_key_value, mime_type) 
    (
        SELECT serializedtags, 'hstore' FROM (
            SELECT serializedtags
            FROM updatedb.nodes AS nodes
            WHERE nodes.valid = false 
            AND nodes.deleted = false 
            AND (nodes.object_new = true or nodes.object_changed = true)
                UNION
            SELECT serializedtags
            FROM updatedb.ways AS ways
            WHERE ways.valid = false 
            AND ways.deleted = false 
            AND (ways.object_new = true or ways.object_changed = true)                
                UNION
            SELECT serializedtags
            FROM updatedb.relations AS relations
            WHERE relations.valid = false 
            AND relations.deleted = false 
            AND (relations.object_new = true or relations.object_changed = true)
        ) AS content
        WHERE content.serializedtags NOTNULL
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.content AS c
            WHERE c.content_key_value = content.serializedtags
            AND c.mime_type = 'hstore'
        )
    );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "content",\tlasted= %', d1, clock_timestamp() - t;

-- Insert new urls
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.url(url) 
    (
        SELECT url FROM (
            SELECT url 
            FROM updatedb.nodes AS nodes
            WHERE nodes.url_changed = true OR nodes.object_new = true
                UNION
            SELECT url
            FROM updatedb.ways AS ways
            WHERE ways.url_changed = true OR ways.object_new = true                
                UNION
            SELECT url
            FROM updatedb.relations AS relations
            WHERE relations.url_changed = true OR relations.object_new = true
        ) as urls
        WHERE urls.url NOTNULL
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.url AS u
            WHERE u.url = urls.url
        )
    );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "url",\tlasted= %', d1, clock_timestamp() - t;

-- Insert new geoobject_content
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.geoobject_content(valid_since, valid_until, geoobject_id, content_id)
    (
        SELECT geoob.tstamp, current, g.id, c.id FROM (
            SELECT nodes.tstamp, nodes.serializedtags, nodes.name 
            FROM updatedb.nodes AS nodes
            WHERE nodes.valid = false AND nodes.deleted = false AND 
            (nodes.object_new = true or nodes.object_changed = true)
                UNION
            SELECT ways.tstamp, ways.serializedtags, ways.name 
            FROM updatedb.ways AS ways
            WHERE ways.valid = false AND ways.deleted = false AND 
            (ways.object_new = true or ways.object_changed = true)                
                UNION
            SELECT relations.tstamp, relations.serializedtags, relations.name 
            FROM updatedb.relations AS relations
            WHERE relations.valid = false AND relations.deleted = false AND 
            (relations.object_new = true or relations.object_changed = true)
        ) AS geoob
        JOIN ohdm.content AS c ON geoob.serializedtags = c.content_key_value
        JOIN ohdm.geoobject AS g ON geoob.name = g.geoobject_name  
        WHERE NOT EXISTS (
            SELECT 1 FROM ohdm.geoobject_content
            WHERE valid_since = geoob.tstamp::date
            AND geoobject_id = g.id
            AND content_id = c.id
        )     
    );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "geoobject_content",\tlasted= %', d1, clock_timestamp() - t;

-- Updated geoobject_content WHERE is valid
    SELECT clock_timestamp() INTO t;
    UPDATE ohdm.geoobject_content SET valid_until = current
    FROM 
    (
        SELECT nodes.tstamp, nodes.serializedtags, nodes.name
        FROM updatedb.nodes AS nodes
        WHERE nodes.valid = true
            UNION
        SELECT ways.tstamp, ways.serializedtags, ways.name
        FROM updatedb.ways AS ways
        WHERE ways.valid = true            
            UNION
        SELECT relations.tstamp, relations.serializedtags, relations.name
        FROM updatedb.relations AS relations
        WHERE relations.valid = true
    ) AS geoob
    JOIN ohdm.content AS c ON geoob.serializedtags = c.content_key_value
    JOIN ohdm.geoobject AS g ON geoob.name = g.geoobject_name
    WHERE geoobject_id = g.id AND content_id = c.id;
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'UPDATE % row(s) in "geoobject_content",\tlasted= %', d1, clock_timestamp() - t;

-- Insert new url_content
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.url_content(valid_since, valid_until, geoobject_id, url_id)
    (
        SELECT geoob.tstamp, current, g.id, u.id
        FROM 
        (
            SELECT nodes.tstamp, nodes.url, nodes.name 
            FROM updatedb.nodes AS nodes
            WHERE nodes.url_changed = true OR nodes.object_new = true
                UNION
            SELECT ways.tstamp, ways.url, ways.name 
            FROM updatedb.ways AS ways
            WHERE ways.url_changed = true OR ways.object_new = true                
                UNION
            SELECT relations.tstamp, relations.url, relations.name 
            FROM updatedb.relations AS relations
            WHERE relations.url_changed = true OR relations.object_new = true
        ) AS geoob
        JOIN ohdm.url AS u ON geoob.url = u.url
        JOIN ohdm.geoobject AS g ON geoob.name = g.geoobject_name       
        WHERE NOT EXISTS (
            SELECT 1 FROM ohdm.url_content
            WHERE valid_since = geoob.tstamp::date
            AND geoobject_id = g.id
            AND url_id = u.id 
        )
    );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "url_content",\tlasted= %', d1, clock_timestamp() - t;

-- Update url_content
    SELECT clock_timestamp() INTO t;
    UPDATE ohdm.url_content SET valid_until = current
    FROM 
    (
        SELECT nodes.tstamp, nodes.url, nodes.name
        FROM updatedb.nodes AS nodes
        WHERE nodes.valid = true
            UNION
        SELECT ways.tstamp, ways.url, ways.name
        FROM updatedb.ways AS ways
        WHERE ways.valid = true            
            UNION
        SELECT relations.tstamp, relations.url, relations.name
        FROM updatedb.relations AS relations
        WHERE relations.valid = true
    ) AS geoob
    JOIN ohdm.url AS u ON geoob.url = u.url
    JOIN ohdm.geoobject AS g ON geoob.name = g.geoobject_name
    WHERE url_id = u.id AND geoobject_id = g.id;
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'UPDATE % row(s) in "url_content",\tlasted= %', d1, clock_timestamp() - t;

-- Insert new geoobject_geometry points
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.geoobject_geometry
        (id_geoobject_source,id_point,valid_since,valid_until)
    (
        SELECT g.id,p.id,n.tstamp,current
        FROM updatedb.nodes AS n
        JOIN ohdm.geoobject AS g ON n.name = g.geoobject_name
        JOIN ohdm.points AS p ON n.geom = p.point
        WHERE n.valid = false 
        AND n.deleted = false 
        AND (n.geom_changed = true OR n.object_new = true)
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.geoobject_geometry
            WHERE id_geoobject_source = g.id
            AND id_point = p.id
            AND valid_since = n.tstamp::date
        )
    );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % point object(s) in "geoobject_geometry",\tlasted= %', d1, clock_timestamp() - t;

-- Insert new geoobject_geometry lines
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.geoobject_geometry
        (id_geoobject_source,id_line,id_geoobject_target,valid_since,valid_until)
    (
        SELECT source.id,l.id,target.id,w.tstamp,current
        FROM updatedb.ways AS w
        JOIN ohdm.geoobject AS source ON w.name = source.geoobject_name
        JOIN ohdm.lines AS l ON w.geom = l.line
        JOIN updatedb.waynodes AS wn ON w.osm_id = wn.way_id
        JOIN updatedb.nodes AS n ON n.osm_id = wn.node_id
        JOIN ohdm.geoobject AS target ON n.name = target.geoobject_name
        WHERE w.valid = false 
        AND w.deleted = false 
        AND (w.geom_changed = true OR w.object_new = true)
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.geoobject_geometry
            WHERE id_geoobject_source = source.id
            AND id_line = l.id
            AND valid_since = w.tstamp::date
        )
    );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % line object(s) in "geoobject_geometry",\tlasted= %', d1, clock_timestamp() - t;

-- Insert new geoobject_geometry polygons with membernodes
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.geoobject_geometry
        (id_geoobject_source,id_polygon,id_geoobject_target,valid_since,valid_until)
    (
        SELECT source.id,p.id,target.id,r.tstamp,current
        FROM updatedb.relations AS r
        JOIN ohdm.geoobject AS source ON r.name = source.geoobject_name
        JOIN ohdm.polygons AS p ON r.geom = p.polygon
        JOIN updatedb.relationmembers AS rm ON r.osm_id = rm.relation_id
        JOIN updatedb.nodes AS n ON n.osm_id = rm.node_id
        JOIN ohdm.geoobject AS target ON n.name = target.geoobject_name
        WHERE r.valid = false 
        AND r.deleted = false 
        AND (r.geom_changed = true OR r.object_new = true)
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.geoobject_geometry
            WHERE id_geoobject_source = source.id
            AND id_polygon = p.id
            AND valid_since = r.tstamp::date
        )
    );
    GET diagnostics d1 = row_count; d2 = d2 + d1;

-- Insert new geoobject_geometry polygons with memberways
    INSERT INTO ohdm.geoobject_geometry
        (id_geoobject_source,id_polygon,id_geoobject_target,valid_since,valid_until)
    (
        SELECT source.id,p.id,target.id,r.tstamp,current
        FROM updatedb.relations AS r
        JOIN ohdm.geoobject AS source ON r.name = source.geoobject_name
        JOIN ohdm.polygons AS p ON r.geom = p.polygon
        JOIN updatedb.relationmembers AS rm ON r.osm_id = rm.relation_id
        JOIN updatedb.ways AS w ON w.osm_id = rm.way_id
        JOIN ohdm.geoobject AS target ON w.name = target.geoobject_name
        WHERE r.valid = false 
        AND r.deleted = false 
        AND (r.geom_changed = true OR r.object_new = true)
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.geoobject_geometry
            WHERE id_geoobject_source = source.id
            AND id_polygon = p.id
            AND valid_since = r.tstamp::date
        )
    );
    GET diagnostics d1 = row_count; d2 = d2 + d1;

-- Insert new geoobject_geometry polygons with memberrelations
    INSERT INTO ohdm.geoobject_geometry
        (id_geoobject_source,id_polygon,id_geoobject_target,valid_since,valid_until)
    (
        SELECT source.id,p.id,target.id,r.tstamp,current
        FROM updatedb.relations AS r
        JOIN ohdm.geoobject AS source ON r.name = source.geoobject_name
        JOIN ohdm.polygons AS p ON r.geom = p.polygon
        JOIN updatedb.relationmembers AS rm ON r.osm_id = rm.relation_id
        JOIN updatedb.relations AS re ON re.osm_id = rm.way_id
        JOIN ohdm.geoobject AS target ON re.name = target.geoobject_name
        WHERE r.valid = false 
        AND r.deleted = false 
        AND (r.geom_changed = true OR r.object_new = true)
        AND NOT EXISTS (
            SELECT 1 FROM ohdm.geoobject_geometry
            WHERE id_geoobject_source = source.id
            AND id_polygon = p.id
            AND valid_since = r.tstamp::date
        )
    );
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'INSERT % polygone object(s) in "geoobject_geometry",\tlasted= %', d2, clock_timestamp() - t;

-- Update geoobject_geometry
    SELECT clock_timestamp() INTO t;
    UPDATE ohdm.geoobject_geometry SET valid_until = current
    FROM
    (
        SELECT name FROM updatedb.nodes
        WHERE valid = true
            UNION
        SELECT name FROM updatedb.ways
        WHERE valid = true
            UNION
        SELECT name FROM updatedb.relations
        WHERE valid = true
    ) AS objects
    JOIN ohdm.geoobject AS g ON objects.name = g.geoobject_name
    WHERE g.id = id_geoobject_source;
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'UPDATE % row(s) in "geoobject_geometry",\tlasted= %', d1, clock_timestamp() - t;
END $$;

\echo 'Import and update for ohdm, DONE\n'

