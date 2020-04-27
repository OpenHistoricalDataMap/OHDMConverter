/* Schema erstellen */

DROP TABLE IF EXISTS my_test_schema.my_housenumbers;

CREATE TABLE my_test_schema.my_housenumbers (

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
addr_street character varying,
addr_postcode character varying,
addr_city character varying,
addr_unit character varying default '',
addr_housename character varying);

/* Daten hinzufügen */
/* POLYGON */
INSERT INTO

my_test_schema.my_housenumbers(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, addr_street, addr_postcode, addr_city, addr_unit, addr_housename)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, gg.housenumber, o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, gg.street, gg.postcode, gg.city, gg.unit, gg.housename

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, 
  valid_since, valid_until, tags, source_user_id as user_id, 
  tags -> 'addr:housenumber' as housenumber,
  tags -> 'addr:street' as street, tags -> 'addr:postcode' as postcode, 
  tags -> 'addr:city' as city, tags->'addr:unit' as unit, 
  tags -> 'addr:housename' as housename FROM ohdm.geoobject_geometry) as gg,
 
 (SELECT id, polygon as geometry FROM ohdm.polygons) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname FROM ohdm.classification) as c
 
 WHERE gg.type_target = 3 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
DELETE FROM my_test_schema.my_housenumbers WHERE addr_street IS NULL
AND addr_postcode IS NULL AND addr_city IS NULL AND addr_unit IS NULL AND addr_housename IS NULL;

DELETE FROM my_test_schema.my_housenumbers WHERE type IS NULL;

/* LINES */
INSERT INTO

my_test_schema.my_housenumbers(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, addr_street, addr_postcode, addr_city, addr_unit, addr_housename)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, gg.housenumber, o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, gg.street, gg.postcode, gg.city, gg.unit, gg.housename

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, 
  valid_since, valid_until, tags, source_user_id as user_id, 
  tags -> 'addr:housenumber' as housenumber,
  tags -> 'addr:street' as street, tags -> 'addr:postcode' as postcode, 
  tags -> 'addr:city' as city, tags->'addr:unit' as unit, 
  tags -> 'addr:housename' as housename FROM ohdm.geoobject_geometry) as gg,
 
 (SELECT id, line as geometry FROM ohdm.lines) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname FROM ohdm.classification) as c
 
 WHERE gg.type_target = 2 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
DELETE FROM my_test_schema.my_housenumbers WHERE addr_street IS NULL
AND addr_postcode IS NULL AND addr_city IS NULL AND addr_unit IS NULL AND addr_housename IS NULL;

DELETE FROM my_test_schema.my_housenumbers WHERE type IS NULL;

/* POINTS */
INSERT INTO

my_test_schema.my_housenumbers(geometry, object_id, geom_id, classid, type, name, 
							   valid_since, valid_until, tags, user_id, addr_street, 
							   addr_postcode, addr_city, addr_unit, addr_housename)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, gg.housenumber, o.name, gg.valid_since, 
gg.valid_until, gg.tags, gg.user_id, gg.street, gg.postcode, gg.city, gg.unit, gg.housename

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, 
  valid_since, valid_until, tags, source_user_id as user_id, 
  tags -> 'addr:housenumber' as housenumber,
  tags -> 'addr:street' as street, tags -> 'addr:postcode' as postcode, 
  tags -> 'addr:city' as city, tags->'addr:unit' as unit, 
  tags -> 'addr:housename' as housename FROM ohdm.geoobject_geometry) as gg,
 
 (SELECT id, point as geometry FROM ohdm.points) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, subclassname FROM ohdm.classification) as c
 
 WHERE gg.type_target = 1 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
DELETE FROM my_test_schema.my_housenumbers WHERE addr_street IS NULL
AND addr_postcode IS NULL AND addr_city IS NULL AND addr_unit IS NULL AND addr_housename IS NULL;

DELETE FROM my_test_schema.my_housenumbers WHERE type IS NULL;