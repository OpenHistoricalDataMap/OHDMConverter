/* Schema erstellen */

DROP TABLE IF EXISTS my_test_schema.my_buildings;

CREATE TABLE my_test_schema.my_buildings (

geometry geometry,
object_id bigint,
geom_id bigint,
classid bigint,
type character varying,
name character varying,
valid_since date,
valid_until date,
tags hstore,
user_id bigint);

/* Daten hinzufügen */
/* POLYGON */
INSERT INTO

my_test_schema.my_buildings(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id FROM ohdm.geoobject_geometry where classification_id between 159 and 261 or 
  /* ... between (SELECT id from ohdm.classification where class = 'amenity') ...*/
 classification_id between 911 and 922 or
 classification_id between 400 and 448 ) as gg,
 
 /* KLASSENID */
 /* amenity: 159 bis 261    as camenity
    aeroway: 911 bis 922    as caeroway
    building: 400 bis 448   as cbuilding */
 
 (SELECT id, polygon as geometry FROM ohdm.polygons) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname FROM ohdm.classification where 
  id between 159 and 261 or 
  id between 911 and 922 or
  id between 400 and 448) as c
 
 WHERE gg.type_target = 3 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 
 /* LINES */
 
INSERT INTO

my_test_schema.my_buildings(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id FROM ohdm.geoobject_geometry where classification_id between 159 and 261 or 
 classification_id between 911 and 922 or
 classification_id between 400 and 448 ) as gg,
 
 /* KLASSENID */
 /* amenity: 159 bis 261    as camenity
    aeroway: 911 bis 922    as caeroway
    building: 400 bis 448   as cbuilding */
 
 (SELECT id, line as geometry FROM ohdm.lines) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname FROM ohdm.classification where 
  id between 159 and 261 or 
  id between 911 and 922 or
  id between 400 and 448) as c
 
 WHERE gg.type_target = 2 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND  c.id = gg.classification_id;
 
 /* POINTS */
 
INSERT INTO

my_test_schema.my_buildings(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id FROM ohdm.geoobject_geometry where classification_id between 159 and 261 or 
 classification_id between 911 and 922 or
 classification_id between 400 and 448 ) as gg,
 
 /* KLASSENID */
 /* amenity: 159 bis 261    as camenity
    aeroway: 911 bis 922    as caeroway
    building: 400 bis 448   as cbuilding */
 
 (SELECT id, point as geometry FROM ohdm.points) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname FROM ohdm.classification where 
  id between 159 and 261 or 
  id between 911 and 922 or
  id between 400 and 448) as c
 
 WHERE gg.type_target = 1 AND  g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
