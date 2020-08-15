/* Table erstellen */

DROP TABLE IF EXISTS target_schema_to_be_replaced.my_transport_points;

CREATE TABLE target_schema_to_be_replaced.my_transport_points (
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
ref character varying);

/* Daten hinzufügen */
/* POLYGON */
INSERT INTO

target_schema_to_be_replaced.my_transport_points(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, ref)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid,  c.subclassname, o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, gg.ref

FROM

 (SELECT id, name from source_schema_to_be_replaced.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, 
  tags -> 'ref' as ref FROM source_schema_to_be_replaced.geoobject_geometry) as gg,
 
 (SELECT id, polygon as geometry FROM source_schema_to_be_replaced.polygons) as g,
 
 /* hier jeweils source_schema_to_be_replaced.polygons, lines, points*/
 
 (SELECT id, subclassname FROM source_schema_to_be_replaced.classification) as c
 
 WHERE gg.type_target = 3 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 
 /* LINES */
INSERT INTO

target_schema_to_be_replaced.my_transport_points(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, ref)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid,  c.subclassname, o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, gg.ref

FROM

 (SELECT id, name from source_schema_to_be_replaced.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, 
  tags -> 'ref' as ref FROM source_schema_to_be_replaced.geoobject_geometry) as gg,
 
 (SELECT id, line as geometry FROM source_schema_to_be_replaced.lines) as g,
 
 /* hier jeweils source_schema_to_be_replaced.polygons, lines, points*/
 
 (SELECT id, subclassname FROM source_schema_to_be_replaced.classification) as c
 
 WHERE gg.type_target = 2 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 /* POINTS */
INSERT INTO

target_schema_to_be_replaced.my_transport_points(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, ref)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid,  c.subclassname, o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, gg.ref

FROM

 (SELECT id, name from source_schema_to_be_replaced.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, 
  tags -> 'ref' as ref FROM source_schema_to_be_replaced.geoobject_geometry) as gg,
 
 (SELECT id, point as geometry FROM source_schema_to_be_replaced.points) as g,
 
 /* hier jeweils source_schema_to_be_replaced.polygons, lines, points*/
 
 (SELECT id, subclassname FROM source_schema_to_be_replaced.classification) as c
 
 WHERE gg.type_target = 1 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;