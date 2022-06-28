-- Convertion PostgreSQL script, to convert 
-- intermediate db into ohdm db
--
-- NOTE: this script must run with access rights on the database
-- author: SteSad
\timing
DROP SCHEMA IF EXISTS ohdm CASCADE;
DO $$
DECLARE
    t TIMESTAMP := clock_timestamp();
    d1 int := 0;
    d2 int := 0;
BEGIN
-- create ohdm database in postgresql
    CREATE SCHEMA IF NOT EXISTS ohdm;
-- create nessarry tables
    CREATE TABLE IF NOT EXISTS ohdm.geoobject
    (
        id             BIGSERIAL NOT NULL,
        geoobject_name VARCHAR,
        classification_ids VARCHAR,
        source_user_id BIGINT,
        CONSTRAINT geoobject_pkey PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ohdm.content
    (
        id                BIGSERIAL NOT NULL,
        content_key       VARCHAR,
        content_value     BYTEA,
        content_key_value HSTORE,
        mime_type         VARCHAR,
        CONSTRAINT content_pkey PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ohdm.geoobject_content
    (
        id                 BIGSERIAL NOT NULL,
        valid_since        DATE,
        valid_until        DATE,
        valid_since_offset BIGINT,
        valid_until_offset BIGINT,
        geoobject_id       BIGINT,
        content_id         BIGINT,
        CONSTRAINT geoobject_content_pkey PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ohdm.url
    (
        id  BIGSERIAL NOT NULL,
        url VARCHAR,
        CONSTRAINT url_pkey PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ohdm.url_content
    (
        id                 BIGSERIAL NOT NULL,
        valid_since        DATE,
        valid_until        DATE,
        valid_since_offset BIGINT,
        valid_until_offset BIGINT,
        geoobject_id       BIGINT,
        url_id             BIGINT,
        CONSTRAINT url_content_pkey PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ohdm.external_systems
    (
        id          BIGSERIAL NOT NULL,
        source_name VARCHAR,
        description VARCHAR,
        CONSTRAINT external_systems_pkey PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ohdm.external_users
    (
        id                  BIGSERIAL NOT NULL,
        userid              BIGINT,
        username            VARCHAR,
        external_systems_id BIGINT,
        CONSTRAINT external_users_pkey PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ohdm.points
    (
        id             BIGSERIAL NOT NULL,
        point          GEOMETRY,
        source_user_id BIGINT,
        CONSTRAINT points_pkey PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ohdm.lines
    (
        id             BIGSERIAL NOT NULL,
        line           GEOMETRY,
        source_user_id BIGINT,
        CONSTRAINT lines_pkey PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ohdm.polygons
    (
        id             BIGSERIAL NOT NULL,
        polygon        GEOMETRY,
        source_user_id BIGINT,
        CONSTRAINT polygons_pkey PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ohdm.geoobject_geometry
    (
        id                  BIGSERIAL NOT NULL,
        id_geoobject_source BIGINT,
        id_point            BIGINT,
        id_line             BIGINT,
        id_polygon          BIGINT,
        id_geoobject_target BIGINT,
        role                VARCHAR,
        valid_since         DATE,
        valid_until         DATE,
        valid_since_offset  BIGINT,
        valid_until_offset  BIGINT,
        CONSTRAINT geoobject_geometry_pkey PRIMARY KEY (id)
    );
    RAISE NOTICE E'CREATE OHDM database, \tlasted=%', clock_timestamp() - t;
    
-- manually insert source of data
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.external_systems(source_name, description)
    VALUES ('osm', 'Open Street Map');
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "external_systems" \tlasted=%', d1, clock_timestamp() - t;

-- insert external_users
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.external_users(userid, username) 
        (
            SELECT uid::BIGINT, username
            FROM inter.nodes
                UNION
            SELECT uid::BIGINT, username
            FROM inter.ways
                UNION
            SELECT uid::BIGINT, username
            FROM inter.relations
        );
    GET diagnostics d1 = row_count;

    SELECT clock_timestamp() INTO t;
    UPDATE ohdm.external_users
    SET external_systems_id = 
        (
            SELECT id
            FROM ohdm.external_systems
            WHERE source_name = 'osm'
        );
    RAISE NOTICE E'INSERT % row(s) in "external_users",\tlasted= %', d1, clock_timestamp() - t;

-- insert points
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.points(point, source_user_id) 
        (
            SELECT n.geom, e.id
            FROM inter.nodes AS n
            JOIN ohdm.external_users AS e ON n.uid::BIGINT = e.userid
        );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "points",\tlasted= %', d1, clock_timestamp() - t;

-- insert lines
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.lines(line, source_user_id) 
        (
            SELECT w.geom, e.id
            FROM inter.ways AS w
            JOIN ohdm.external_users AS e ON w.uid::BIGINT = e.userid
        );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "lines",\tlasted= %', d1, clock_timestamp() - t;

-- insert polygons
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.polygons(polygon, source_user_id) 
        (
            SELECT r.geom, e.id
            FROM inter.relations AS r
            JOIN ohdm.external_users AS e ON r.uid::BIGINT = e.userid
        );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "polygons",\tlasted= %', d1, clock_timestamp() - t;

-- insert geoobject
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.geoobject(geoobject_name, classification_ids, source_user_id) 
        (
            SELECT name, classification_ids, e.id
            FROM (
                SELECT uid::BIGINT, name, classification_ids
                FROM inter.nodes
                    UNION
                SELECT uid::BIGINT, name, classification_ids
                FROM inter.ways
                    UNION
                SELECT uid::BIGINT, name, classification_ids
                FROM inter.relations
            ) AS geoob
            JOIN ohdm.external_users AS e ON geoob.uid = e.userid);
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "geoobject",\tlasted= %', d1, clock_timestamp() - t;

-- insert content
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.content(content_key_value, mime_type) 
        (
            SELECT serializedtags, 'hstore'
            FROM inter.nodes
                UNION
            SELECT serializedtags, 'hstore'
            FROM inter.ways
                UNION
            SELECT serializedtags, 'hstore'
            FROM inter.relations
        );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "content",\tlasted= %', d1, clock_timestamp() - t;

-- insert url
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.url(url) 
        (
            SELECT url
            FROM inter.nodes
                UNION
            SELECT url
            FROM inter.ways
                UNION
            SELECT url
            FROM inter.relations
        );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "url",\tlasted= %', d1, clock_timestamp() - t;

-- insert geoobject_content
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.geoobject_content
        (valid_since, valid_until, geoobject_id, content_id) 
        (
            SELECT geoob.tstamp, CURRENT_TIMESTAMP, g.id, c.id
            FROM (
                SELECT tstamp, serializedtags, name
                FROM inter.nodes
                    UNION
                SELECT tstamp, serializedtags, name
                FROM inter.ways
                    UNION
                SELECT tstamp, serializedtags, name
                FROM inter.relations
            ) AS geoob
            JOIN ohdm.content AS c ON geoob.serializedtags = c.content_key_value
            JOIN ohdm.geoobject AS g ON geoob.name = g.geoobject_name
        );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "geoobject_content",\tlasted= %', d1, clock_timestamp() - t;

-- insert url_content
    SELECT clock_timestamp() INTO t;
    INSERT INTO ohdm.url_content
        (valid_since, valid_until, geoobject_id, url_id) 
        (
            SELECT geoob.tstamp, CURRENT_TIMESTAMP, g.id, u.id
            FROM (
                SELECT tstamp, url, name
                FROM inter.nodes
                    UNION
                SELECT tstamp, url, name
                FROM inter.ways
                    UNION
                SELECT tstamp, url, name
                FROM inter.relations
            ) AS geoob
            JOIN ohdm.geoobject AS g ON geoob.name = g.geoobject_name
            JOIN ohdm.url AS u ON geoob.url = u.url
        );
    GET diagnostics d1 = row_count;
    RAISE NOTICE E'INSERT % row(s) in "url_content",\tlasted= %', d1, clock_timestamp() - t;

-- insert geoobject_geometry
    SELECT clock_timestamp() INTO t;
-- from nodes
    INSERT INTO ohdm.geoobject_geometry
        (id_geoobject_source, id_point, valid_since, valid_until) 
        (
            SELECT g.id, p.id, n.tstamp, CURRENT_TIMESTAMP
            FROM inter.nodes AS n
            JOIN ohdm.geoobject AS g ON n.name = g.geoobject_name
            JOIN ohdm.points AS p ON n.geom = p.point
        );
    GET diagnostics d1 = row_count; d2 = d2 + d1;

-- from ways with waynodes
    INSERT INTO ohdm.geoobject_geometry
        (id_geoobject_source, id_line, id_geoobject_target, valid_since, valid_until) 
        (
            SELECT source.id, l.id, target.id, w.tstamp, CURRENT_TIMESTAMP
            FROM inter.ways AS w
            JOIN inter.waynodes AS wn ON w.osm_id = wn.way_id
            JOIN ohdm.geoobject AS source ON w.name = source.geoobject_name
            JOIN ohdm.lines AS l ON w.geom = l.line
            JOIN inter.nodes AS n ON n.osm_id = wn.node_id
            JOIN ohdm.geoobject AS target ON n.name = target.geoobject_name
        );
    GET diagnostics d1 = row_count; d2 = d2 + d1;

-- from relations with membernodes
    INSERT INTO ohdm.geoobject_geometry
        (id_geoobject_source, id_polygon, id_geoobject_target, role, valid_since, valid_until) 
        (
            SELECT source.id, p.id, target.id, rm.role, r.tstamp, CURRENT_TIMESTAMP
            FROM inter.relations AS r
            JOIN inter.relationmembers AS rm ON r.osm_id = rm.relation_id
            JOIN ohdm.geoobject AS source ON r.name = source.geoobject_name
            JOIN ohdm.polygons AS p ON r.geom = p.polygon
            JOIN inter.nodes AS n ON n.osm_id = rm.node_id
            JOIN ohdm.geoobject AS target ON n.name = target.geoobject_name
        );
    GET diagnostics d1 = row_count; d2 = d2 + d1;

-- from relations with memberways
    INSERT INTO ohdm.geoobject_geometry
        (id_geoobject_source, id_polygon, id_geoobject_target, role, valid_since, valid_until)
        (
            SELECT source.id, p.id, target.id, rm.role, r.tstamp, CURRENT_TIMESTAMP
            FROM inter.relations AS r
            JOIN inter.relationmembers AS rm ON r.osm_id = rm.relation_id
            JOIN ohdm.geoobject AS source ON r.name = source.geoobject_name
            JOIN ohdm.polygons AS p ON geom = p.polygon
            JOIN inter.ways AS w ON w.osm_id = rm.way_id
            JOIN ohdm.geoobject AS target ON w.name = target.geoobject_name
        );
    GET diagnostics d1 = row_count; d2 = d2 + d1;

-- from relations with memberrelations
    INSERT INTO ohdm.geoobject_geometry
        (id_geoobject_source, id_polygon, id_geoobject_target, role, valid_since, valid_until)
        (
            SELECT source.id, p.id, target.id, rm.role, r.tstamp, CURRENT_TIMESTAMP
            FROM inter.relations AS r
            JOIN inter.relationmembers AS rm ON r.osm_id = rm.relation_id
            JOIN ohdm.geoobject AS source ON r.name = source.geoobject_name
            JOIN ohdm.polygons AS p ON geom = p.polygon
            JOIN inter.relations AS re ON re.osm_id = rm.way_id
            JOIN ohdm.geoobject AS target ON re.name = target.geoobject_name
        );
    GET diagnostics d1 = row_count; d2 = d2 + d1;
    RAISE NOTICE E'INSERT % row(s) in "geoobject_geometry",\tlasted= %', d2, clock_timestamp() - t;
END $$;
\echo '\n\n'
