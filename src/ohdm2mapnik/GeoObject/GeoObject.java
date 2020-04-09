package ohdm2mapnik.GeoObject;

import ohdm2mapnik.Tags;

import java.util.Date;

public class GeoObject {

    public static String[] fileds = {
            "name",
    };

    protected long wayId;
    protected long geoobjectId;
    protected String name;
    protected Tags tags;
    protected Date validSince;
    protected Date validUntil;
    protected String way;

    public GeoObject(long wayId, long geoobjectId, String name, String classificationClass, String classificationSubclassname, String tags, Date validSince, Date validUntil, String way) {
        this.wayId = wayId;
        this.geoobjectId = geoobjectId;
        this.name = name;
        this.tags = new Tags(tags);
        this.tags.put(classificationClass, classificationSubclassname);
        this.validSince = validSince;
        this.validUntil = validUntil;
        this.way = way;
    }

    public String getMapnikQuery(String targetSchema) {
        return "";
    }
}
