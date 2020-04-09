package ohdm2mapnik;

import java.util.HashMap;
import java.util.Map;

public class Tags {

    private Map<String, String> tags;
    private int zOrder = 0;
    private boolean roads = false;

    // The following keys will be deleted
    public static String[] deleteTags = {
            "note",
            "source",
            "source_ref",
            "attribution",
            "comment",
            "fixme",
            // Tags generally dropped by editors, not otherwise covered
            "created_by",
            "odbl",
            // Lots of import tags
            // EUROSHA (Various countries)
            "project:eurosha_2012",
            // UrbIS (Brussels, BE)
            "ref:UrbIS",
            // NHN (CA)
            "accuracy:meters",
            "waterway:type",
            // StatsCan (CA)
            "statscan:rbuid",
            // RUIAN (CZ)
            "ref:ruian:addr",
            "ref:ruian",
            "building:ruian:type",
            // DIBAVOD (CZ)
            "dibavod:id",
            // UIR-ADR (CZ)
            "uir_adr:ADRESA_KOD",
            // GST (DK)
            "gst:feat_id",
            // osak (DK)
            "osak:identifier",
            // Maa-amet (EE)
            "maaamet:ETAK",
            // FANTOIR (FR)
            "ref:FR:FANTOIR",
            // OPPDATERIN (NO)
            "OPPDATERIN",
            // Various imports (PL)
            "addr:city:simc",
            "addr:street:sym_ul",
            "building:usage:pl",
            "building:use:pl",
            // TERYT (PL)
            "teryt:simc",
            // RABA (SK)
            "raba:id",
            // LINZ (NZ)
            "linz2osm:objectid",
            // DCGIS (Washington DC, US)
            "dcgis:gis_id",
            // Building Identification Number (New York, US)
            "nycdoitt:bin",
            // Chicago Building Inport (US)
            "chicago:building_id",
            // Louisville, Kentucky/Building Outlines Import (US)
            "lojic:bgnum",
            // MassGIS (Massachusetts, US)
            "massgis:way_id",
            // misc
            "import",
            "import_uuid",
            "OBJTYPE",
            "SK53_bulk:load",
    };
    public static String[] deletePrefixes = {
            "note:",
            "source:",
            // Corine (CLC) (Europe)
            "CLC:",
            // Geobase (CA)
            "geobase:",
            // CanVec (CA)
            "canvec:",
            // Geobase (CA)
            "geobase:",
            // kms (DK)
            "kms:",
            // ngbe (ES)
            // See also note:es and source:file above
            "ngbe:",
            // Friuli Venezia Giulia (IT)
            "it:fvg:",
            // KSJ2 (JA)
            // See also note:ja and source_ref above
            "KSJ2:",
            // Yahoo/ALPS (JA)
            "yh:",
            // LINZ (NZ)
            "LINZ2OSM:",
            "LINZ:",
            // WroclawGIS (PL)
            "WroclawGIS:",
            // Naptan (UK)
            "naptan:",
            // TIGER (US)
            "tiger:",
            // GNIS (US)
            "gnis:",
            // National Hydrography Dataset (US)
            "NHD:",
            "nhd:",
            // mvdgis (Montevideo, UY)
            "mvdgis:",
    };

    public Tags(String tags) {
        this.tags = new HashMap<String, String>();

        try {
            if (!tags.equals("null") && !tags.equals("")) {
                this.string2HashMap(tags);
            }
        } catch (NullPointerException e) {
            //  Block of code to handle errors
        }
    }

    public void put(String key, String value) {
        if (!value.equals("undefined")) {
            this.tags.put(key, value);
        }
    }

    public String get(String key) {
        return this.tags.getOrDefault(key, "NULL");
    }

    /**
     * convert hstore string into hash map
     */
    private void string2HashMap(String tags) {
        // cut first & last char from tags
        tags = tags.substring(1);
        tags = tags.substring(0, tags.length() - 1);
        String[] tagsPart = tags.split("\", \"");

        for (String tagPart : tagsPart) {
            String[] tag = tagPart.split("\"=>\"");
            this.tags.put(tag[0], tag[1]);
        }
    }

    public void cleanupTags() {
        Map<String, String> cleanTags = new HashMap<String, String>();

        for (String key : this.tags.keySet()) {
            String value = this.tags.get(key);

            // remove unused prefix
            for(int i = 0; i< deletePrefixes.length; i++){
                if (key.startsWith(deletePrefixes[i])) {
                    key = key.replace(deletePrefixes[i], "");
                    continue;
                }
            }

            // remove unused tags
            for(int i = 0; i< deleteTags.length; i++){
                if (key.equals(deleteTags[i])) {
                    key = "";
                    break;
                }
            }

            // add useful tag
            if (!key.equals("")) {
                cleanTags.put(key, value);
            }
        }

        // update self.tags with clean tags
        this.tags = cleanTags;
    }

    public void setZorderRoads() {
        this.zOrder = 0;
        this.roads = false;

        for (String key : this.tags.keySet()) {
            String value = this.tags.get(key);
            switch (key) {
                case "highway":
                    switch (value) {
                        case "motorway":
                            this.zOrder += 380;
                            this.roads = true;
                            break;
                        case "trunk":
                            this.zOrder += 370;
                            this.roads = true;
                            break;
                        case "primary":
                            this.zOrder += 360;
                            this.roads = true;
                            break;
                        case "secondary":
                            this.zOrder += 350;
                            this.roads = true;
                            break;
                        case "tertiary":
                            this.zOrder += 340;
                            break;
                        case "residential":
                            this.zOrder += 330;
                            break;
                        case "unclassified":
                            this.zOrder += 330;
                            break;
                        case "road":
                            this.zOrder += 330;
                            break;
                        case "living_street":
                            this.zOrder += 320;
                            break;
                        case "pedestrian":
                            this.zOrder += 310;
                            break;
                        case "raceway":
                            this.zOrder += 300;
                            break;
                        case "motorway_link":
                            this.zOrder += 240;
                            this.roads = true;
                            break;
                        case "trunk_link":
                            this.zOrder += 230;
                            this.roads = true;
                            break;
                        case "primary_link":
                            this.zOrder += 220;
                            this.roads = true;
                            break;
                        case "secondary_link":
                            this.zOrder += 210;
                            this.roads = true;
                            break;
                        case "tertiary_link":
                            this.zOrder += 200;
                            break;
                        case "service":
                            this.zOrder += 150;
                            break;
                        case "track":
                            this.zOrder += 110;
                            break;
                        case "path":
                            this.zOrder += 100;
                            break;
                        case "footway":
                            this.zOrder += 100;
                            break;
                        case "bridleway":
                            this.zOrder += 100;
                            break;
                        case "cycleway":
                            this.zOrder += 100;
                            break;
                        case "steps":
                            this.zOrder += 90;
                            break;
                        case "platform":
                            this.zOrder += 90;
                            break;
                    }
                    break;
                case "railway":
                    switch (value) {
                        case "rail":
                            this.zOrder += 440;
                            this.roads = true;
                            break;
                        case "subway":
                            this.zOrder += 420;
                            this.roads = true;
                            break;
                        case "narrow_gauge":
                            this.zOrder += 420;
                            this.roads = true;
                            break;
                        case "light_rail":
                            this.zOrder += 420;
                            this.roads = true;
                            break;
                        case "funicular":
                            this.zOrder += 420;
                            this.roads = true;
                            break;
                        case "preserved":
                            this.zOrder += 420;
                            break;
                        case "monorail":
                            this.zOrder += 420;
                            break;
                        case "miniature":
                            this.zOrder += 420;
                            break;
                        case "turntable":
                            this.zOrder += 420;
                            break;
                        case "tram":
                            this.zOrder += 410;
                            break;
                        case "disused":
                            this.zOrder += 400;
                            break;
                        case "construction":
                            this.zOrder += 400;
                            break;
                        case "platform":
                            this.zOrder += 90;
                            break;
                    }
                    break;
                case "aeroway":
                    switch (value) {
                        case "runway":
                            this.zOrder += 60;
                            break;
                        case "taxiway":
                            this.zOrder += 50;
                            break;
                    }
                    break;
                case "boundary":
                    switch (value) {
                        case "administrative":
                            this.roads = true;
                            break;
                    }
                    break;
            }
        }
    }

    /**
     * convert tags to sql hstore value
     *
     * @return hstore String
     */
    public String getHstoreTags() {

        if (this.tags.size() == 0) {
            return "";
        }

        StringBuilder hstoreTags = new StringBuilder("");
        for (String key : this.tags.keySet()) {
            hstoreTags.append("\"" + key + "\"=>\"" + this.tags.get(key) + "\", ");
        }
        hstoreTags.delete(hstoreTags.length() - 2, hstoreTags.length());
        return hstoreTags.toString();
    }

    public int getzOrder() {
        return zOrder;
    }

    public boolean isRoads() {
        return roads;
    }
}
