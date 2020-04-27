/* Schema erstellen */

DROP TABLE IF EXISTS my_test_schema.my_roads;

CREATE TABLE my_test_schema.my_roads (
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
highway character varying,
tunnel smallint,
bridge smallint,
oneway smallint,
ref character varying,
layer character varying,
access character varying,
service character varying,
horse character varying,
bicycle character varying,
construction character varying,
surface character varying,
tracktype character varying,
z_order character varying,
highspeed character varying,
usage character varying,
class character varying);

/* Daten hinzufügen */
/* POLYGON */
INSERT INTO

my_test_schema.my_roads(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, highway, tunnel, bridge, oneway, ref, layer, access, service, horse, bicycle, construction, surface, tracktype, z_order, highspeed, usage, class)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id, c.subclassname, 
CAST(
    CASE
        WHEN gg.tunnel = 'yes'
    THEN 1
    ELSE 0
END AS smallint), 

CAST(
    CASE
        WHEN gg.bridge = 'yes'
    THEN 1
    ELSE 0
END AS smallint), 

CAST(
    CASE
        WHEN gg.oneway = 'yes'
    THEN 1
    ELSE 0
END AS smallint), gg.ref, gg.layer, gg.access, gg.service, gg.horse, gg.bicycle, gg.construction, gg.surface, gg.tracktype, gg.z_order, gg.highspeed, gg.usage, c.type

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, 
  tags->'tunnel' as tunnel,
  tags->'bridge' as bridge,
  tags->'oneway' as oneway,
  tags->'ref' as ref,
  tags->'layer' as layer,
  tags->'access' as access,
  tags->'service' as service,
  tags->'horse' as horse,
  tags->'bicycle' as bicycle,
  tags->'construction' as construction,
  tags->'surface' as surface,
  tags->'tracktype' as tracktype,
  tags->'z_order' as z_order,
  tags->'maxspeed' as highspeed,
  tags->'usage' as usage
  FROM ohdm.geoobject_geometry) as gg,
 
 (SELECT id, polygon as geometry FROM ohdm.polygons) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, class as type, subclassname FROM ohdm.classification) as c
 
 WHERE gg.type_target = 3 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 /* LINES */
INSERT INTO

my_test_schema.my_roads(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, highway, tunnel, bridge, oneway, ref, layer, access, service, horse, bicycle, construction, surface, tracktype, z_order, highspeed, usage, class)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id, c.subclassname, 
CAST(
    CASE
        WHEN gg.tunnel = 'yes'
    THEN 1
    ELSE 0
END AS smallint), 

CAST(
    CASE
        WHEN gg.bridge = 'yes'
    THEN 1
    ELSE 0
END AS smallint), 

CAST(
    CASE
        WHEN gg.oneway = 'yes'
    THEN 1
    ELSE 0
END AS smallint), gg.ref, gg.layer, gg.access, gg.service, gg.horse, gg.bicycle, gg.construction, gg.surface, gg.tracktype, gg.z_order, gg.highspeed, gg.usage, c.type

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, 
  tags->'tunnel' as tunnel,
  tags->'bridge' as bridge,
  tags->'oneway' as oneway,
  tags->'ref' as ref,
  tags->'layer' as layer,
  tags->'access' as access,
  tags->'service' as service,
  tags->'horse' as horse,
  tags->'bicycle' as bicycle,
  tags->'construction' as construction,
  tags->'surface' as surface,
  tags->'tracktype' as tracktype,
  tags->'z_order' as z_order,
  tags->'maxspeed' as highspeed,
  tags->'usage' as usage
  FROM ohdm.geoobject_geometry) as gg,

 (SELECT id, line as geometry FROM ohdm.lines) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, class as type, subclassname FROM ohdm.classification) as c
 
 WHERE gg.type_target = 2 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 
 /* POINTS */
INSERT INTO

my_test_schema.my_roads(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id, highway, tunnel, bridge, oneway, ref, layer, access, service, horse, bicycle, construction, surface, tracktype, z_order, highspeed, usage, class)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id, c.subclassname, 
CAST(
    CASE
        WHEN gg.tunnel = 'yes'
    THEN 1
    ELSE 0
END AS smallint), 

CAST(
    CASE
        WHEN gg.bridge = 'yes'
    THEN 1
    ELSE 0
END AS smallint), 

CAST(
    CASE
        WHEN gg.oneway = 'yes'
    THEN 1
    ELSE 0
END AS smallint), gg.ref, gg.layer, gg.access, gg.service, gg.horse, gg.bicycle, gg.construction, gg.surface, gg.tracktype, gg.z_order, gg.highspeed, gg.usage, c.type

FROM

 (SELECT id, name from ohdm.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id, 
  tags->'tunnel' as tunnel,
  tags->'bridge' as bridge,
  tags->'oneway' as oneway,
  tags->'ref' as ref,
  tags->'layer' as layer,
  tags->'access' as access,
  tags->'service' as service,
  tags->'horse' as horse,
  tags->'bicycle' as bicycle,
  tags->'construction' as construction,
  tags->'surface' as surface,
  tags->'tracktype' as tracktype,
  tags->'z_order' as z_order,
  tags->'maxspeed' as highspeed,
  tags->'usage' as usage
  FROM ohdm.geoobject_geometry) as gg,

 
 (SELECT id, point as geometry FROM ohdm.points) as g,
 
 /* hier jeweils ohdm.polygons, lines, points*/
 
 (SELECT id, class as type, subclassname FROM ohdm.classification) as c
 
 WHERE gg.type_target = 1 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
