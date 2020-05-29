package ohdm2mapnik.GeoObject;

import java.util.Date;

public class Point extends GeoObject {

    public static String[] fileds = {
            "access",
            "addr:housename",
            "addr:housenumber",
            "admin_level",
            "aerialway",
            "aeroway",
            "amenity",
            "barrier",
            "boundary",
            "building",
            "highway",
            "historic",
            "junction",
            "landuse",
            "leisure",
            "lock",
            "man_made",
            "military",
            "name",
            "natural",
            "oneway",
            "place",
            "power",
            "railway",
            "ref",
            "religion",
            "shop",
            "tourism",
            "water",
            "waterway"
    };

    public Point(long wayId, long geoobjectId, String name, String classificationClass, String classificationSubclassname, String tags, Date validSince, Date validUntil, String way) {
        super(wayId, geoobjectId, name, classificationClass, classificationSubclassname, tags, validSince, validUntil, way);
    }

    @Override
    public String getMapnikQuery(String targetSchema) {
        /*
        INSERT INTO public.planet_osm_point(
        id, osm_id, version, visible, geoobject, access, "addr:housename", "addr:housenumber", admin_level, aerialway, aeroway, amenity, barrier, boundary, building, highway, historic, junction, landuse, layer, leisure, lock, man_made, military, name, "natural", oneway, place, power, railway, ref, religion, shop, tourism, water, waterway, tags, way, valid_since, valid_until)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
         */
        StringBuilder query = new StringBuilder("INSERT INTO " + targetSchema + ".planet_osm_point( ");
        query.append("geoobject, layer, tags, way, valid_since, valid_until");
        for (int i = 0; i < fileds.length; i++) {
            query.append(", \"" + fileds[i] + "\"");
        }
        query.append(") VALUES (");

        // geoobject id
        query.append(super.geoobjectId);
        query.append(", ");

        // layer
        try {
            query.append(Long.parseLong(super.tags.get("layer")));
            query.append(", ");
        } catch (NumberFormatException e) {
            query.append("NULL, ");
        }

        // tags
        super.tags.cleanupTags();
        query.append("'");
        query.append(super.tags.getHstoreTags());
        query.append("', ");

        // geometry
        query.append("'" + super.way + "', ");

        // valid range
        query.append("'");
        query.append(super.validSince.toString());
        query.append("', '");
        query.append(super.validUntil.toString());
        query.append("'");

        // fields
        for (int i = 0; i < fileds.length; i++) {
            String value = super.tags.get(fileds[i]);
            if (value.equals("NULL")) {
                query.append(", NULL");
            } else {
                query.append(", '" + value + "'");
            }
        }

        // end
        query.append(");");

        return query.toString();
    }
}