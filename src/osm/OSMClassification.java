package osm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import osm2inter.AbstractElement;
import osm2inter.SQLStatementQueue;
import osm2inter.TagElement;

/**
 *
 * @author thsc
 */
public class OSMClassification {
    public HashMap<String, List<String>> osmFeatureClasses = new HashMap();
    
    public static final String UNDEFINED = "undefined";
    private static OSMClassification osmClassification = null;
    
    public static OSMClassification getOSMClassification() {
        if(OSMClassification.osmClassification == null) {
            OSMClassification.osmClassification = new OSMClassification();
        }
        
        return OSMClassification.osmClassification;
    }
    
    private OSMClassification() {
        // that's not really good style.. anyway create that map here in code
        List<String> subClasses = new ArrayList<>();
        
        // Aerialway
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("cable_car");// lines
        subClasses.add("gondola");// lines
        subClasses.add("chair_lift");// lines
        subClasses.add("mixed_lift");// lines
        subClasses.add("drag_lift");// lines
        subClasses.add("t-bar");// lines
        subClasses.add("j-bar");// lines
        subClasses.add("platter");// lines
        subClasses.add("rope_tow");// lines
        subClasses.add("magic_carpet"); // lines
        subClasses.add("zip_line");
        
        subClasses.add("pylon"); // point
        subClasses.add("station"); // point and polygone
        
        subClasses.add("canopy"); // lines
        subClasses.add("goods"); // lines
        
        osmFeatureClasses.put("aerialway", subClasses);
        
        // Aeroway
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("apron");
        subClasses.add("gate");
        subClasses.add("hangar");
        subClasses.add("helipad");
        subClasses.add("navigationaid");
        subClasses.add("runway");
        subClasses.add("taxilane");
        subClasses.add("taxiway");
        subClasses.add("apron");
        subClasses.add("terminal");
        subClasses.add("windsock");
        
        osmFeatureClasses.put("aeroway", subClasses);        
        
        // Amenity
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("bar");
        subClasses.add("bbq");
        subClasses.add("biergarten");
        subClasses.add("cafe");
        subClasses.add("drinking_water");
        subClasses.add("fast_food");
        subClasses.add("food_court");
        subClasses.add("ice_cream");
        subClasses.add("pub");
        subClasses.add("restaurant");
        subClasses.add("college");
        subClasses.add("kindergarten");
        subClasses.add("library");
        subClasses.add("public_bookcase");
        subClasses.add("school");
        subClasses.add("music_school");
        subClasses.add("driving_school");
        subClasses.add("language_school");
        subClasses.add("university");
        subClasses.add("bicycle_parking");
        subClasses.add("bicycle_repair_station");
        subClasses.add("biycycle_rental");
        subClasses.add("boat_sharing");
        subClasses.add("bus_station");
        subClasses.add("car_rental");
        subClasses.add("car_sharing");
        subClasses.add("car_wash");
        subClasses.add("charging_station");
        subClasses.add("ferry_terminal");
        subClasses.add("fuel");
        subClasses.add("grit_bin");
        subClasses.add("motorcycle_parking");
        subClasses.add("parking");
        subClasses.add("parking_entrance");
        subClasses.add("parking_space");
        subClasses.add("taxi");
        subClasses.add("atm");
        subClasses.add("bureau_de_change");
        subClasses.add("baby_hatch");
        subClasses.add("clinic");
        subClasses.add("dentist");
        subClasses.add("doctors");
        subClasses.add("hospital");
        subClasses.add("nursing_home");
        subClasses.add("pharmacy");
        subClasses.add("social_facility");
        subClasses.add("veterinary");
        subClasses.add("blood_donation");
        subClasses.add("arts_centre");
        subClasses.add("brothel");
        subClasses.add("casino");
        subClasses.add("cinema");
        subClasses.add("community_centre");
        subClasses.add("fountain");
        subClasses.add("gambling");
        subClasses.add("nightclub");
        subClasses.add("planetarium");
        subClasses.add("social_centre");
        subClasses.add("stripclub");
        subClasses.add("studio");
        subClasses.add("swingerclub");
        subClasses.add("theatre");
        subClasses.add("animal_boarding");
        subClasses.add("animal_shelter");
        subClasses.add("bench");
        subClasses.add("clock");
        subClasses.add("courthouse");
        subClasses.add("coworking_space");
        subClasses.add("crematorium");
        subClasses.add("crypt");
        subClasses.add("dive_centre");
        subClasses.add("dojo");
        subClasses.add("embassy");
        subClasses.add("fire_station");
        subClasses.add("game_feeding");
        subClasses.add("grave_yard");
        subClasses.add("hunting_stand");
        subClasses.add("internet_cafe");
        subClasses.add("kneipp_water_cure");
        subClasses.add("marketplace");
        subClasses.add("photo_booth");
        subClasses.add("place_of_worship");
        subClasses.add("police");
        subClasses.add("post_box");
        subClasses.add("post_office");
        subClasses.add("prison");
        subClasses.add("ranger_station");
        subClasses.add("recycling");
        subClasses.add("rescue_station");
        subClasses.add("sauna");
        subClasses.add("shelter");
        subClasses.add("shower");
        subClasses.add("table");
        subClasses.add("telephone");
        subClasses.add("toilets");
        subClasses.add("townhall");
        subClasses.add("vending_machine");
        subClasses.add("waste_basket");
        subClasses.add("waste_disposal");
        subClasses.add("waste_transfer_station");
        subClasses.add("watering_place");
        subClasses.add("water_point");
        
        osmFeatureClasses.put("amenity", subClasses);                
        
        // Barrier (but only cable_barrier, city_wall, ditch, fence, retaining_wall, tank_trap, wall)
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("unknown");
        subClasses.add("cable_barrier");
        subClasses.add("city_wall");
        subClasses.add("ditch");
        subClasses.add("fence");
        subClasses.add("retaining_wall");
        subClasses.add("tank_trap");
        subClasses.add("wall");
        
        osmFeatureClasses.put("barrier", subClasses);                
        
        // Boundary
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("administrative");
        subClasses.add("historic");
        subClasses.add("maritime");
        subClasses.add("national_park");
        subClasses.add("political");
        subClasses.add("postal_code");
        subClasses.add("religious_administration");
        subClasses.add("protected_area");
        
        osmFeatureClasses.put("boundary", subClasses);        
        
        // Building
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("apartments");
        subClasses.add("farm");
        subClasses.add("hotel");
        subClasses.add("house");
        subClasses.add("detached");
        subClasses.add("residential");
        subClasses.add("dormitory");
        subClasses.add("terrace");
        subClasses.add("houseboat");
        subClasses.add("bungalow");
        subClasses.add("static_caravan");
        subClasses.add("commercial");
        subClasses.add("industrial");
        subClasses.add("retail");
        subClasses.add("cathedral");
        subClasses.add("church");
        subClasses.add("chapel");
        subClasses.add("mosque");
        subClasses.add("temple");
        subClasses.add("synagogue");
        subClasses.add("shrine");
        subClasses.add("civic");
        subClasses.add("hospital");
        subClasses.add("school");
        subClasses.add("stadium");
        subClasses.add("train_station");
        subClasses.add("transportation");
        subClasses.add("university");
        subClasses.add("public");
        
        // other buildings
        subClasses.add("barn");
        subClasses.add("bridge");
        subClasses.add("bunker");
        subClasses.add("cabin");
        subClasses.add("construction");
        subClasses.add("cowshed");
        subClasses.add("farm_auxiliary");
        subClasses.add("garage");
        subClasses.add("garages");
        subClasses.add("greenhouse");
        subClasses.add("hangar");
        subClasses.add("roof");
        subClasses.add("hut");
        subClasses.add("shed");
        subClasses.add("stable");
        subClasses.add("transformer_tower");
        subClasses.add("service");
        subClasses.add("kiosk");
        subClasses.add("ruins");
        
        osmFeatureClasses.put("building", subClasses);        
        
        // Craft
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("agricultural_engines");
        subClasses.add("bakery");
        subClasses.add("basket_maker");
        subClasses.add("beekeeper");
        subClasses.add("blacksmith");
        subClasses.add("boatbuilder");
        subClasses.add("bookbinder");
        subClasses.add("brewery");
        subClasses.add("builder");
        subClasses.add("carpenter");
        subClasses.add("carpet_layer");
        subClasses.add("caterer");
        subClasses.add("chimney_sweeper");
        subClasses.add("clockmaker");
        subClasses.add("confectionery");
        subClasses.add("distillery");
        subClasses.add("dressmaker");
        subClasses.add("electrician");
        subClasses.add("floorer");
        subClasses.add("gardener");
        subClasses.add("glaziery");
        subClasses.add("handicraft");
        subClasses.add("hvac");
        subClasses.add("insulation");
        subClasses.add("jeweller");
        subClasses.add("locksmith");
        subClasses.add("metal_construction");
        subClasses.add("optician");
        subClasses.add("painter");
        subClasses.add("parquet_layer");
        subClasses.add("optician");
        subClasses.add("photographer");
        subClasses.add("photographic_laboratory");
        subClasses.add("piano_tuner");
        subClasses.add("plasterer");
        subClasses.add("plumber");
        subClasses.add("pottery");
        subClasses.add("rigger");
        subClasses.add("roofer");
        subClasses.add("saddler");
        subClasses.add("sailmaker");
        subClasses.add("sawmill");
        subClasses.add("scaffolder");
        subClasses.add("sculptor");
        subClasses.add("shoemaker");
        subClasses.add("stand_builder");
        subClasses.add("stonemason");
        subClasses.add("sun_protection");
        subClasses.add("tailor");
        subClasses.add("tiler");
        subClasses.add("tinsmith");
        subClasses.add("turner");
        subClasses.add("upholsterer");
        subClasses.add("watchmaker");
        subClasses.add("window_construction");
        subClasses.add("winery");
        
        osmFeatureClasses.put("craft", subClasses);        
        
        // Emergency
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("ambulance_station");
        subClasses.add("defibrillator");
        
        // TODO: continue with Firefighters
        // http://wiki.openstreetmap.org/wiki/Map_Features
        
        osmFeatureClasses.put("emergency", subClasses);        
        

        
        // Geological
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("moraine");
        subClasses.add("outcrop");
        subClasses.add("palaeontological_site");
        
        osmFeatureClasses.put("geological", subClasses);        
        
        // Highway
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("motorway");
        subClasses.add("trunk");
        subClasses.add("primary");
        subClasses.add("secondary");
        subClasses.add("tertiary");
        subClasses.add("unclassified");
        subClasses.add("service");
        subClasses.add("motorway_link");
        subClasses.add("trunk_link");
        subClasses.add("primary_link");
        subClasses.add("secondary_link");
        subClasses.add("tertiary_link");
        subClasses.add("living_street");
        subClasses.add("pedestrian");
        subClasses.add("track");
        subClasses.add("bus_guideway");
        subClasses.add("escape");
        subClasses.add("raceway");
        subClasses.add("road");
        subClasses.add("footway");
        subClasses.add("bridleway");
        subClasses.add("steps");
        subClasses.add("path");
        subClasses.add("cycleway");
        subClasses.add("proposed");
        subClasses.add("construction");
        subClasses.add("bus_stop");
        
        // TODO add others
        osmFeatureClasses.put("highway", subClasses);        
        
        // Historic (of course :) )
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("aircraft");
        subClasses.add("archaeological_site");
        subClasses.add("battlefield");
        subClasses.add("boundary_stone");
        subClasses.add("building");
        subClasses.add("cannon");
        subClasses.add("castle");
        subClasses.add("city_gate");
        subClasses.add("citywalls");
        subClasses.add("farm");
        subClasses.add("fort");
        subClasses.add("gallows");
        subClasses.add("highwater_mark");
        subClasses.add("locomotive");
        subClasses.add("manor");
        subClasses.add("memorial");
        subClasses.add("milestone");
        subClasses.add("monastery");
        subClasses.add("monument");
        subClasses.add("optical_telegraph");
        subClasses.add("pillory");
        subClasses.add("ruins");
        subClasses.add("rune_stone");
        subClasses.add("ship");
        subClasses.add("wayside_cross");
        subClasses.add("wayside_shrine");
        subClasses.add("wreck");
        
        osmFeatureClasses.put("historic", subClasses);        
        
        // Landuse
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("allotments");
        
        // TODO add others
        osmFeatureClasses.put("landuse", subClasses);        

        // Man Made
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("adit");
        
        // TODO add others
        osmFeatureClasses.put("man_made", subClasses);        
        
        // Military
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("airfield");
             
        // TODO add others
        osmFeatureClasses.put("military", subClasses);        

        // Natural
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("wood");
             
        // TODO add others
        osmFeatureClasses.put("natural", subClasses);        

        // Office
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("accountant");
             
        // TODO add others
        osmFeatureClasses.put("office", subClasses);        

        // Places
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("country");
             
        // TODO add others
        osmFeatureClasses.put("places", subClasses);        

        // Power
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("plant");
             
        // TODO add others
        osmFeatureClasses.put("power", subClasses);        

        // Public Transport
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("stop_position");
             
        // TODO add others
        osmFeatureClasses.put("public_transport", subClasses);        

        // Railway
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("abandoned");
             
        // TODO add others
        osmFeatureClasses.put("railway", subClasses);        
        
        // Route
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("bicycle");
             
        // TODO add others
        osmFeatureClasses.put("route", subClasses);        

        // Waterway
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("river");
             
        // TODO add others
        osmFeatureClasses.put("waterway", subClasses);   
        
        // Historic person            
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
             
        // TODO add others
        osmFeatureClasses.put("historic_person", subClasses);  
        
        this.setupClassIDs_Names();
    }
    
    private HashMap<String, Integer> classIDs = new HashMap<>();
    private ArrayList<String> fullClassNames = new ArrayList<>();
    
    private void setupClassIDs_Names() {
        // no append real data
        int id = 1; // start with 1 as in database

        // create classification table
        // iterate classes
        Iterator<String> classIter = this.osmFeatureClasses.keySet().iterator();

        while(classIter.hasNext()) {
            String className = classIter.next();
            List<String> subClasses = this.osmFeatureClasses.get(className);
            Iterator<String> subClassIter = subClasses.iterator();

            while(subClassIter.hasNext()) {
                String subClassName = subClassIter.next();
                
                // keep in memory
                String fullClassName = this.createFullClassName(className, subClassName);
                
                this.classIDs.put(fullClassName, id++);
                this.fullClassNames.add(fullClassName);
            }
        }
    }
    
    private String createFullClassName(String className, String subclassname) {
        return className + "_" + subclassname;
    }
    
    public String getFullClassName(String classCodeString) {
        int classCode = Integer.parseInt(classCodeString);
        String nothing = "undefined";
        
        if(classCode > -1 && this.fullClassNames.size() > classCode) {
            return this.fullClassNames.get(classCode);
        }
        
        return nothing;
    }
    
    /**
     * TODO: Add here translation of unused OSM types to OHDM types etc.
     * @param osmElement
     * @return 
     */
    public int getOHDMClassID(AbstractElement osmElement) {
      // a node can have tags which can describe geometrie feature classe
        ArrayList<TagElement> tags = osmElement.getTags();
        if(tags == null) return -1;
        
        Iterator<TagElement> tagIter = tags.iterator();
        if(tagIter == null) return -1;
        
        while(tagIter.hasNext()) {
            TagElement tag = tagIter.next();

            // get attributes of that tag
            Iterator<String> keyIter = tag.attributes.keySet().iterator();
            while(keyIter.hasNext()) {
                String key = keyIter.next();

                // is this key name of a feature class?
                if(this.isClassName(key)) {
                    String value = tag.attributes.get(key);

                    // find id of class / subclass
                    return this.getOHDMClassID(key, value);
                }
            }
        }

        // there is no class description - sorry
        return -1;
        
    }
    
    /**
     * @return -1 if no known class and sub class name, a non-negative number 
     * otherwise
     */
    public int getOHDMClassID(String className, String subClassName) {
        String fullClassName = this.createFullClassName(className, subClassName);
        
        // find entry
        Integer id = this.classIDs.get(fullClassName);
        if(id != null) {
            return id;
        }
        
        // try undefined
        fullClassName = this.createFullClassName(className, OSMClassification.UNDEFINED);
        id = this.classIDs.get(fullClassName);
        if(id != null) {
            return id;
        }
        
//        System.out.println("not found: " + this.createFullClassName(className, subClassName));
        
        // else
        return -1;
    }
    
    private boolean isClassName(String key) {
        return this.getOSMClassification().osmFeatureClasses.keySet().contains(key);
    }
    
    public void write2Table(Connection targetConnection, String classificationTableName) throws SQLException {
        
        SQLStatementQueue sq = new SQLStatementQueue(targetConnection);
        
        // init first line: unknown classification
        sq.append("INSERT INTO ");
        sq.append(classificationTableName);
        sq.append(" VALUES (-1, 'no_class', 'no_subclass');");

        sq.forceExecute();
        
        // now append real data
        int n = 0;

        // create classification table
        // iterate classes
        Iterator<String> classIter = this.osmClassification.osmFeatureClasses.keySet().iterator();

        while(classIter.hasNext()) {
            String className = classIter.next();
            List<String> subClasses = this.osmClassification.osmFeatureClasses.get(className);
            Iterator<String> subClassIter = subClasses.iterator();

            while(subClassIter.hasNext()) {
                String subClassName = subClassIter.next();
                
                // add to database
                sq.append("INSERT INTO ");
                sq.append(classificationTableName);
                sq.append(" (class, subclass) VALUES ('");
                sq.append(className);
                sq.append("', '");
                sq.append(subClassName);
                sq.append("') RETURNING id;");
                
                ResultSet insertResult = sq.executeWithResult();
                insertResult.next();
                int classID = insertResult.getInt(1);
                
                // keep in memory
                String fullClassName = this.createFullClassName(className, subClassName);
                
                this.classIDs.put(fullClassName, classID);
            }
        }
    }
}
