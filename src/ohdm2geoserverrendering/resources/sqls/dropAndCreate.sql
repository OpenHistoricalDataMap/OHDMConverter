/* Admin labels */
DROP TABLE IF EXISTS target_schema_to_be_replaced.my_admin_labels;
CREATE TABLE target_schema_to_be_replaced.my_admin_labels (
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

/* Table Amenities */
DROP TABLE IF EXISTS target_schema_to_be_replaced.my_amenities;
CREATE TABLE target_schema_to_be_replaced.my_amenities (
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

/* Table Boundaries */
DROP TABLE IF EXISTS target_schema_to_be_replaced.my_boundaries;
CREATE TABLE target_schema_to_be_replaced.my_boundaries (
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
admin_level integer);

/* Buldings */
DROP TABLE IF EXISTS target_schema_to_be_replaced.my_buildings;
CREATE TABLE target_schema_to_be_replaced.my_buildings (
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

/* House numbers */
DROP TABLE IF EXISTS target_schema_to_be_replaced.my_housenumbers;
CREATE TABLE target_schema_to_be_replaced.my_housenumbers (
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

/* Landusages */
DROP TABLE IF EXISTS target_schema_to_be_replaced.my_landusages;
CREATE TABLE target_schema_to_be_replaced.my_landusages (
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
area real,
layer integer,
z_order integer);

/* Places */
DROP TABLE IF EXISTS target_schema_to_be_replaced.my_places;
CREATE TABLE target_schema_to_be_replaced.my_places (
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
capital character varying,
z_order INTEGER,
population INTEGER);

/* Roads */
DROP TABLE IF EXISTS target_schema_to_be_replaced.my_roads;
CREATE TABLE target_schema_to_be_replaced.my_roads (
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

/* Transport areas */
DROP TABLE IF EXISTS target_schema_to_be_replaced.my_transport_areas;
CREATE TABLE target_schema_to_be_replaced.my_transport_areas (
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

/* Transport points */
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

/* Water areas */
DROP TABLE IF EXISTS target_schema_to_be_replaced.my_waterarea;
CREATE TABLE target_schema_to_be_replaced.my_waterarea (
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
area real);

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
