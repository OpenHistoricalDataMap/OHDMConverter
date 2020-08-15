
/* Daten hinzufügen */
/* POLYGON */
INSERT INTO

target_schema_to_be_replaced.my_buildings(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id

FROM

 (SELECT id, name from source_schema_to_be_replaced.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id FROM source_schema_to_be_replaced.geoobject_geometry) as gg,
 

 
 (SELECT id, polygon as geometry FROM source_schema_to_be_replaced.polygons) as g,
 
 /* hier jeweils source_schema_to_be_replaced.polygons, lines, points*/
 
 (SELECT id, subclassname FROM source_schema_to_be_replaced.classification where 
  subclassname = 'apartments'
OR subclassname = 'bungalow'
OR subclassname = 'cabin'
OR subclassname = 'detached'
OR subclassname = 'dormitory'
OR subclassname = 'farm'
OR subclassname = 'ger'
OR subclassname = 'hotel'
OR subclassname = 'house'
OR subclassname = 'houseboat'
OR subclassname = 'residential'
OR subclassname = 'semidetached_house'
OR subclassname = 'static_caravan'
OR subclassname = 'terrace'
OR subclassname = 'commercial'
OR subclassname = 'industrial'
OR subclassname = 'kiosk'
OR subclassname = 'office'
OR subclassname = 'retail'
OR subclassname = 'supermarket'
OR subclassname = 'warehouse'
OR subclassname = 'cathedral'
OR subclassname = 'chapel'
OR subclassname = 'church'
OR subclassname = 'mosque'
OR subclassname = 'religious'
OR subclassname = 'shrine'
OR subclassname = 'synagogue'
OR subclassname = 'temple'
OR subclassname = 'bakehouse'
OR subclassname = 'civic'
OR subclassname = 'fire_station'
OR subclassname = 'government'
OR subclassname = 'hospital'
OR subclassname = 'kindergarten'
OR subclassname = 'public'
OR subclassname = 'school'
OR subclassname = 'toilets'
OR subclassname = 'train_station'
OR subclassname = 'transportation'
OR subclassname = 'conservatory'
OR subclassname = 'university'
OR subclassname = 'barn'
OR subclassname = 'conservatory'
OR subclassname = 'cowshed'
OR subclassname = 'farm_auxiliary'
OR subclassname = 'greenhouse'
OR subclassname = 'slurry_tank'
OR subclassname = 'stable'
OR subclassname = 'sty'
OR subclassname = 'grandstand'
OR subclassname = 'pavilion'
OR subclassname = 'riding_hall'
OR subclassname = 'sports_hall'
OR subclassname = 'stadium'
OR subclassname = 'hangar'
OR subclassname = 'hut'
OR subclassname = 'shed'
OR subclassname = 'carport'
OR subclassname = 'garage'
OR subclassname = 'garages'
OR subclassname = 'parking'
OR subclassname = 'digester'
OR subclassname = 'service'
OR subclassname = 'transformer_tower'
OR subclassname = 'water_tower'
OR subclassname = 'bunker'
OR subclassname = 'bridge'
OR subclassname = 'construction'
OR subclassname = 'gatehouse'
OR subclassname = 'roof'
OR subclassname = 'ruins'
OR subclassname = 'tree_house'
OR subclassname = 'yes'
OR subclassname = 'user defined'

  ) as c
 
 WHERE gg.type_target = 3 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
 
 
 /* LINES */
 
INSERT INTO

target_schema_to_be_replaced.my_buildings(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id

FROM

 (SELECT id, name from source_schema_to_be_replaced.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id FROM source_schema_to_be_replaced.geoobject_geometry) as gg,
 

 
 (SELECT id, line as geometry FROM source_schema_to_be_replaced.lines) as g,
 
 /* hier jeweils source_schema_to_be_replaced.polygons, lines, points*/
 
 (SELECT id, subclassname FROM source_schema_to_be_replaced.classification where 
  subclassname = 'apartments'
OR subclassname = 'bungalow'
OR subclassname = 'cabin'
OR subclassname = 'detached'
OR subclassname = 'dormitory'
OR subclassname = 'farm'
OR subclassname = 'ger'
OR subclassname = 'hotel'
OR subclassname = 'house'
OR subclassname = 'houseboat'
OR subclassname = 'residential'
OR subclassname = 'semidetached_house'
OR subclassname = 'static_caravan'
OR subclassname = 'terrace'
OR subclassname = 'commercial'
OR subclassname = 'industrial'
OR subclassname = 'kiosk'
OR subclassname = 'office'
OR subclassname = 'retail'
OR subclassname = 'supermarket'
OR subclassname = 'warehouse'
OR subclassname = 'cathedral'
OR subclassname = 'chapel'
OR subclassname = 'church'
OR subclassname = 'mosque'
OR subclassname = 'religious'
OR subclassname = 'shrine'
OR subclassname = 'synagogue'
OR subclassname = 'temple'
OR subclassname = 'bakehouse'
OR subclassname = 'civic'
OR subclassname = 'fire_station'
OR subclassname = 'government'
OR subclassname = 'hospital'
OR subclassname = 'kindergarten'
OR subclassname = 'public'
OR subclassname = 'school'
OR subclassname = 'toilets'
OR subclassname = 'train_station'
OR subclassname = 'transportation'
OR subclassname = 'conservatory'
OR subclassname = 'university'
OR subclassname = 'barn'
OR subclassname = 'conservatory'
OR subclassname = 'cowshed'
OR subclassname = 'farm_auxiliary'
OR subclassname = 'greenhouse'
OR subclassname = 'slurry_tank'
OR subclassname = 'stable'
OR subclassname = 'sty'
OR subclassname = 'grandstand'
OR subclassname = 'pavilion'
OR subclassname = 'riding_hall'
OR subclassname = 'sports_hall'
OR subclassname = 'stadium'
OR subclassname = 'hangar'
OR subclassname = 'hut'
OR subclassname = 'shed'
OR subclassname = 'carport'
OR subclassname = 'garage'
OR subclassname = 'garages'
OR subclassname = 'parking'
OR subclassname = 'digester'
OR subclassname = 'service'
OR subclassname = 'transformer_tower'
OR subclassname = 'water_tower'
OR subclassname = 'bunker'
OR subclassname = 'bridge'
OR subclassname = 'construction'
OR subclassname = 'gatehouse'
OR subclassname = 'roof'
OR subclassname = 'ruins'
OR subclassname = 'tree_house'
OR subclassname = 'yes'
OR subclassname = 'user defined'
) as c
 
 WHERE gg.type_target = 2 AND g.id = gg.id_target AND o.id = gg.id_geoobject_source AND  c.id = gg.classification_id;
 
 /* POINTS */
 
INSERT INTO

target_schema_to_be_replaced.my_buildings(geometry, object_id, geom_id, classid, type, name, valid_since, valid_until, tags, user_id)

SELECT 

g.geometry, o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.tags, gg.user_id

FROM

 (SELECT id, name from source_schema_to_be_replaced.geoobject) as o, 
 
 (SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, valid_until, tags, source_user_id as user_id FROM source_schema_to_be_replaced.geoobject_geometry) as gg,
 
 /* KLASSENID */
 /* amenity: 159 bis 261    as camenity
    aeroway: 911 bis 922    as caeroway
    building: 400 bis 448   as cbuilding */
 
 (SELECT id, point as geometry FROM source_schema_to_be_replaced.points) as g,
 
 /* hier jeweils source_schema_to_be_replaced.polygons, lines, points*/
 
 (SELECT id, subclassname FROM source_schema_to_be_replaced.classification where 
  subclassname = 'apartments'
OR subclassname = 'bungalow'
OR subclassname = 'cabin'
OR subclassname = 'detached'
OR subclassname = 'dormitory'
OR subclassname = 'farm'
OR subclassname = 'ger'
OR subclassname = 'hotel'
OR subclassname = 'house'
OR subclassname = 'houseboat'
OR subclassname = 'residential'
OR subclassname = 'semidetached_house'
OR subclassname = 'static_caravan'
OR subclassname = 'terrace'
OR subclassname = 'commercial'
OR subclassname = 'industrial'
OR subclassname = 'kiosk'
OR subclassname = 'office'
OR subclassname = 'retail'
OR subclassname = 'supermarket'
OR subclassname = 'warehouse'
OR subclassname = 'cathedral'
OR subclassname = 'chapel'
OR subclassname = 'church'
OR subclassname = 'mosque'
OR subclassname = 'religious'
OR subclassname = 'shrine'
OR subclassname = 'synagogue'
OR subclassname = 'temple'
OR subclassname = 'bakehouse'
OR subclassname = 'civic'
OR subclassname = 'fire_station'
OR subclassname = 'government'
OR subclassname = 'hospital'
OR subclassname = 'kindergarten'
OR subclassname = 'public'
OR subclassname = 'school'
OR subclassname = 'toilets'
OR subclassname = 'train_station'
OR subclassname = 'transportation'
OR subclassname = 'conservatory'
OR subclassname = 'university'
OR subclassname = 'barn'
OR subclassname = 'conservatory'
OR subclassname = 'cowshed'
OR subclassname = 'farm_auxiliary'
OR subclassname = 'greenhouse'
OR subclassname = 'slurry_tank'
OR subclassname = 'stable'
OR subclassname = 'sty'
OR subclassname = 'grandstand'
OR subclassname = 'pavilion'
OR subclassname = 'riding_hall'
OR subclassname = 'sports_hall'
OR subclassname = 'stadium'
OR subclassname = 'hangar'
OR subclassname = 'hut'
OR subclassname = 'shed'
OR subclassname = 'carport'
OR subclassname = 'garage'
OR subclassname = 'garages'
OR subclassname = 'parking'
OR subclassname = 'digester'
OR subclassname = 'service'
OR subclassname = 'transformer_tower'
OR subclassname = 'water_tower'
OR subclassname = 'bunker'
OR subclassname = 'bridge'
OR subclassname = 'construction'
OR subclassname = 'gatehouse'
OR subclassname = 'roof'
OR subclassname = 'ruins'
OR subclassname = 'tree_house'
OR subclassname = 'yes'
OR subclassname = 'user defined'
) as c
 
 WHERE gg.type_target = 1 AND  g.id = gg.id_target AND o.id = gg.id_geoobject_source AND c.id = gg.classification_id;
