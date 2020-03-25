/* Table erstellen */

DROP TABLE IF EXISTS my_test_schema.my_amenities;

CREATE TABLE my_test_schema.my_amenities (

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
religion character varying,
denomination character varying,
power_source character varying,
score integer,
access character varying);

/* Daten hinzufügen */
/* POLYGON */
INSERT INTO

my_test_schema.my_amenities(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, religion, denomination, power_source, score, access)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid,  c.subclassname, o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, gg.religion, gg.denomination, gg.power_source, CAST(score as integer), gg.access

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, 
  tags -> 'religion' as religion,
  tags -> 'denomination' as denomination,
  tags -> 'generator:source' as power_source,
  tags -> 'score' as score,
  tags -> 'access' as access FROM ohdm.geoobject_geometry) as gg,
 
 (SELECT id, polygon as geometry FROM ohdm.polygons) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname FROM ohdm.classification) as c
 
 WHERE gg.type_target = 3 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 /* LINES */
INSERT INTO

my_test_schema.my_amenities(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, religion, denomination, power_source, score, access)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid,  c.subclassname, o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, gg.religion, gg.denomination, gg.power_source, CAST(score as integer), access

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, 
  tags -> 'religion' as religion,
  tags -> 'denomination' as denomination,
  tags -> 'generator:source' as power_source,
  tags -> 'score' as score,
  tags -> 'access' as access FROM ohdm.geoobject_geometry) as gg,
 
 (SELECT id, line as geometry FROM ohdm.lines) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname FROM ohdm.classification) as c
 
 WHERE gg.type_target = 2 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 
 /* POINTS */
INSERT INTO

my_test_schema.my_amenities(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, religion, denomination, power_source, score, access)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid,  c.subclassname, o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, gg.religion, gg.denomination, gg.power_source, CAST(score as integer), access

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, 
  tags -> 'religion' as religion,
  tags -> 'denomination' as denomination,
  tags -> 'generator:source' as power_source,
  tags -> 'score' as score,
  tags -> 'access' as access FROM ohdm.geoobject_geometry) as gg,
 
 (SELECT id, point as geometry FROM ohdm.points) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname FROM ohdm.classification) as c
 
 WHERE gg.type_target = 1 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
