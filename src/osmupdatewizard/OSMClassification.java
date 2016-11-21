package osmupdatewizard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author thsc
 */
class OSMClassification {
    HashMap<String, List<String>> osmFeatureClasses = new HashMap();
    
    static final String UNDEFINED = "undefined";
    
    OSMClassification() {
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
        
        osmFeatureClasses.put("Aerialway", subClasses);
        
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
        
        osmFeatureClasses.put("Aeroway", subClasses);        
        
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
        
        osmFeatureClasses.put("Barrier", subClasses);                
        
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
        
        osmFeatureClasses.put("Boundary", subClasses);        
        
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
        
        osmFeatureClasses.put("Building", subClasses);        
        
        // Craft
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("agricultural_engines");
        
        // TODO: add others
        
        osmFeatureClasses.put("Craft", subClasses);        
        
        // Geological
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("moraine");
        subClasses.add("outcrop");
        subClasses.add("palaeontological_site");
        
        osmFeatureClasses.put("Geological", subClasses);        
        
        // Highway
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("motorway");
        
        // TODO add others
        osmFeatureClasses.put("Highway", subClasses);        
        
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
        
        osmFeatureClasses.put("Historic", subClasses);        
        
        // Landuse
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("allotments");
        
        // TODO add others
        osmFeatureClasses.put("Landuse", subClasses);        

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
        osmFeatureClasses.put("Military", subClasses);        

        // Natural
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("wood");
             
        // TODO add others
        osmFeatureClasses.put("Natural", subClasses);        

        // Office
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("accountant");
             
        // TODO add others
        osmFeatureClasses.put("Office", subClasses);        

        // Places
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("country");
             
        // TODO add others
        osmFeatureClasses.put("Places", subClasses);        

        // Power
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("plant");
             
        // TODO add others
        osmFeatureClasses.put("Power", subClasses);        

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
        osmFeatureClasses.put("Waterway", subClasses);   
        
        // Historic person            
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
             
        // TODO add others
        osmFeatureClasses.put("historic_person", subClasses);   
    }
}
