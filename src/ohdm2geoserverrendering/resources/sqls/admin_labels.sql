/* Nur fuer administrative boundaries */ 

/* Table erstellen */

DROP TABLE IF EXISTS my_test_schema.my_admin_labels;

CREATE TABLE my_test_schema.my_admin_labels (
geometry geometry,
object_id bigint,
geom_id bigint,
classid bigint,
type character varying,
name character varying,
valid_since date,
valid_until date,
tags hstore,
user_id bigint,
admin_level integer,
area real);

/* Daten hinzufügen */
/* POLYGON */
INSERT INTO

my_test_schema.my_admin_labels(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, admin_level, area)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, 
/* Falls Klasse = adminlevel_x dann klasse = administrative*/
CASE
    WHEN c.subclassname = 'adminlevel_1' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_2' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_3' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_4' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_5' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_6' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_7' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_8' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_9' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_10' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_11' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_12' THEN 'administrative'
    ELSE c.subclassname
END
	, o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, CAST(
    CASE
        WHEN c.admin_level = 'adminlevel_1' THEN 1
        WHEN c.admin_level = 'adminlevel_2' THEN 2
        WHEN c.admin_level = 'adminlevel_3' THEN 3
        WHEN c.admin_level = 'adminlevel_4' THEN 4
        WHEN c.admin_level = 'adminlevel_5' THEN 5
        WHEN c.admin_level = 'adminlevel_6' THEN 6
        WHEN c.admin_level = 'adminlevel_7' THEN 7
        WHEN c.admin_level = 'adminlevel_8' THEN 8
        WHEN c.admin_level = 'adminlevel_9' THEN 9
        WHEN c.admin_level = 'adminlevel_10' THEN 10
        WHEN c.admin_level = 'adminlevel_11' THEN 11
        WHEN c.admin_level = 'adminlevel_12' THEN 12                                     
    ELSE 0
END AS integer), st_area(g.geometry, true)

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id FROM ohdm.geoobject_geometry) as gg,
 
 (SELECT id, polygon as geometry FROM ohdm.polygons) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname as admin_level, subclassname FROM ohdm.classification where class = 'ohdm_boundary' OR class = 'boundary' AND subclassname = 'administrative') as c
 
 WHERE gg.type_target = 3 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 /* LINE */
INSERT INTO

my_test_schema.my_admin_labels(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, admin_level)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, 
/* Falls Klasse = adminlevel_x dann klasse = administrative*/
CASE
    WHEN c.subclassname = 'adminlevel_1' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_2' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_3' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_4' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_5' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_6' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_7' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_8' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_9' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_10' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_11' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_12' THEN 'administrative'
    ELSE c.subclassname
END
    , o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, CAST(
    CASE
        WHEN c.admin_level = 'adminlevel_1' THEN 1
        WHEN c.admin_level = 'adminlevel_2' THEN 2
        WHEN c.admin_level = 'adminlevel_3' THEN 3
        WHEN c.admin_level = 'adminlevel_4' THEN 4
        WHEN c.admin_level = 'adminlevel_5' THEN 5
        WHEN c.admin_level = 'adminlevel_6' THEN 6
        WHEN c.admin_level = 'adminlevel_7' THEN 7
        WHEN c.admin_level = 'adminlevel_8' THEN 8
        WHEN c.admin_level = 'adminlevel_9' THEN 9
        WHEN c.admin_level = 'adminlevel_10' THEN 10
        WHEN c.admin_level = 'adminlevel_11' THEN 11
        WHEN c.admin_level = 'adminlevel_12' THEN 12                                     
    ELSE 0
END AS integer)

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id FROM ohdm.geoobject_geometry) as gg,
 
 (SELECT id, line as geometry FROM ohdm.lines) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname as admin_level, subclassname FROM ohdm.classification where class = 'ohdm_boundary' OR class = 'boundary' AND subclassname = 'administrative') as c
 
 WHERE gg.type_target = 2 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 
 /* POINT */
 
 INSERT INTO

my_test_schema.my_admin_labels(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, admin_level)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, 
/* Falls Klasse = adminlevel_x dann klasse = administrative*/
CASE
    WHEN c.subclassname = 'adminlevel_1' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_2' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_3' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_4' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_5' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_6' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_7' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_8' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_9' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_10' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_11' THEN 'administrative'
    WHEN c.subclassname = 'adminlevel_12' THEN 'administrative'
    ELSE c.subclassname
END
    , o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, CAST(
    CASE
        WHEN c.admin_level = 'adminlevel_1' THEN 1
        WHEN c.admin_level = 'adminlevel_2' THEN 2
        WHEN c.admin_level = 'adminlevel_3' THEN 3
        WHEN c.admin_level = 'adminlevel_4' THEN 4
        WHEN c.admin_level = 'adminlevel_5' THEN 5
        WHEN c.admin_level = 'adminlevel_6' THEN 6
        WHEN c.admin_level = 'adminlevel_7' THEN 7
        WHEN c.admin_level = 'adminlevel_8' THEN 8
        WHEN c.admin_level = 'adminlevel_9' THEN 9
        WHEN c.admin_level = 'adminlevel_10' THEN 10
        WHEN c.admin_level = 'adminlevel_11' THEN 11
        WHEN c.admin_level = 'adminlevel_12' THEN 12                                     
    ELSE 0
END AS integer)

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id FROM ohdm.geoobject_geometry) as gg,
 
 (SELECT id, point as geometry FROM ohdm.points) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname as admin_level, subclassname FROM ohdm.classification where class = 'ohdm_boundary' OR class = 'boundary' AND subclassname = 'administrative') as c
 
 WHERE gg.type_target = 1 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;