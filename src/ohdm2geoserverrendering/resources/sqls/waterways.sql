/* Schema erstellen */

DROP TABLE IF EXISTS target_schema_to_be_replaced.my_waterways;

CREATE TABLE target_schema_to_be_replaced.my_waterways (
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
tunnel VARCHAR(50),
intermittent VARCHAR(50),
bridge VARCHAR(50),
lock VARCHAR(50));

/* Daten hinzufügen */
/* POLYGON */
INSERT INTO

target_schema_to_be_replaced.my_waterways(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, tunnel, intermittent, bridge, lock)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id, gg.tunnel, gg.intermittent, gg.bridge, gg.lock

FROM

 (SELECT id, name from source_schema_to_be_replaced.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, tags -> 'tunnel' as tunnel, tags -> 'intermittent' as intermittent, tags -> 'bridge' as bridge, 
tags->'lock' as lock FROM source_schema_to_be_replaced.geoobject_geometry) as gg,
 
 (SELECT id, polygon as geometry FROM source_schema_to_be_replaced.polygons) as g,
 
 /* hier jeweils source_schema_to_be_replaced.polygons, lines, points*/
 
 (SELECT id, subclassname FROM source_schema_to_be_replaced.classification) as c
 
 WHERE gg.type_target = 3 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 
 /* LINE */
INSERT INTO

target_schema_to_be_replaced.my_waterways(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, tunnel, intermittent, bridge, lock)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id, gg.tunnel, gg.intermittent, gg.bridge, gg.lock

FROM

 (SELECT id, name from source_schema_to_be_replaced.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, tags -> 'tunnel' as tunnel, tags -> 'intermittent' as intermittent, tags -> 'bridge' as bridge, 
tags->'lock' as lock FROM source_schema_to_be_replaced.geoobject_geometry) as gg,
 
 (SELECT id, line as geometry FROM source_schema_to_be_replaced.lines) as g,
 
 /* hier jeweils source_schema_to_be_replaced.polygons, lines, points*/
 
 (SELECT id, subclassname FROM source_schema_to_be_replaced.classification) as c
 
 WHERE gg.type_target = 2 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 /* gg.type_target = 3 AND  aus WHERE entnommen */
 
 
 /* POINT */
INSERT INTO

target_schema_to_be_replaced.my_waterways(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, tunnel, intermittent, bridge, lock)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id, gg.tunnel, gg.intermittent, gg.bridge, gg.lock

FROM

 (SELECT id, name from source_schema_to_be_replaced.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, tags -> 'tunnel' as tunnel, tags -> 'intermittent' as intermittent, tags -> 'bridge' as bridge, 
tags->'lock' as lock FROM source_schema_to_be_replaced.geoobject_geometry) as gg,
 
 (SELECT id, point as geometry FROM source_schema_to_be_replaced.points) as g,
 
 /* hier jeweils source_schema_to_be_replaced.polygons, lines, points*/
 
 (SELECT id, subclassname FROM source_schema_to_be_replaced.classification) as c
 
 WHERE gg.type_target = 1 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 /* gg.type_target = 3 AND  aus WHERE entnommen */
 
