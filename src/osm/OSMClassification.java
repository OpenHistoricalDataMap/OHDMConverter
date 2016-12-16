package osm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import osmupdatewizard.AbstractElement;
import osmupdatewizard.SQLStatementQueue;
import osmupdatewizard.TagElement;

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
        
        // TODO: add others
        
        osmFeatureClasses.put("craft", subClasses);        
        
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
    
    /**
     * Returns iterator of string which are valid ID for
     * this classname. Each class has subclasses.. there are more
     * than one id
     * @param classname
     * @return 
     */
    public Iterator<String> getClassIDs(String classname) {
        List<String> ids = new ArrayList<>();
        for(String fullClassName : this.classIDs.keySet()) {
            if(fullClassName.startsWith(classname)) {
                ids.add(Integer.toString(this.classIDs.get(fullClassName)));
            }
        }
        
        return ids.iterator();
    }
    
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
