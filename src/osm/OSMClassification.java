package osm;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import util.OHDM_DB;
import util.SQLStatementQueue;
import util.DB;

/**
 *
 * @author thsc
 */
public class OSMClassification {
    // key: class (like highway, value: list of subclasses (like primary, secondary)
    public HashMap<String, List<String>> osmFeatureClasses = new HashMap();
    private final HashMap<String, Integer> classIDs = new HashMap<>();
    private final ArrayList<String> fullClassNames = new ArrayList<>();

    public static final String UNDEFINED = "undefined";
    private static OSMClassification osmClassification = null;
    
    public static OSMClassification getOSMClassification() {
        if(OSMClassification.osmClassification == null) {
            OSMClassification.osmClassification = new OSMClassification();
        }
        
        return OSMClassification.osmClassification;
    }
    
    private OSMClassification() {
        List<String> subClasses = new ArrayList<>();
        
        // Aerialway
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("cable_car");// lines
        subClasses.add("gondola");// lines
        subClasses.add("mixed_lift");// lines
        subClasses.add("chair_lift");// lines
        subClasses.add("drag_lift");// lines
        subClasses.add("t-bar");// lines
        subClasses.add("j-bar");// lines
        subClasses.add("platter");// lines
        subClasses.add("rope_tow");// lines
        subClasses.add("magic_carpet"); // lines
        subClasses.add("zip_line");

        subClasses.add("goods"); // lines
        subClasses.add("pylon"); // point
        subClasses.add("station"); // point and polygone

        subClasses.add("canopy"); // lines // doesn't exist?

        osmFeatureClasses.put("aerialway", subClasses);
        
        // Aeroway
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("aerodrome"); //added
        subClasses.add("apron");
        subClasses.add("gate");
        subClasses.add("hangar");
        subClasses.add("helipad");
        subClasses.add("heliport"); //added
        subClasses.add("navigationaid");
        subClasses.add("runway");
        subClasses.add("spaceport"); //added
        subClasses.add("taxilane"); // doesn't exist?
        subClasses.add("taxiway");
        // subClasses.add("apron"); // double
        subClasses.add("terminal");
        subClasses.add("windsock");
        
        osmFeatureClasses.put("aeroway", subClasses);        
        
        // Amenity
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        // sustenance
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
        // education
        subClasses.add("college");
        subClasses.add("driving_school");
        subClasses.add("kindergarten");
        subClasses.add("language_school");
        subClasses.add("library");
        subClasses.add("toy_library"); //added
        subClasses.add("music_school");
        subClasses.add("school");
        subClasses.add("university");
        // transportation
        subClasses.add("bicycle_parking");
        subClasses.add("bicycle_repair_station");
        subClasses.add("biycycle_rental");
        subClasses.add("boat_rental"); //added
        subClasses.add("boat_sharing");
        subClasses.add("bus_station");
        subClasses.add("car_rental");
        subClasses.add("car_sharing");
        subClasses.add("car_wash");
        subClasses.add("vehicle_inspection"); //added
        subClasses.add("charging_station");
        subClasses.add("ferry_terminal");
        subClasses.add("fuel");
        subClasses.add("grit_bin");
        subClasses.add("motorcycle_parking");
        subClasses.add("parking");
        subClasses.add("parking_entrance");
        subClasses.add("parking_space");
        subClasses.add("taxi");
        // financial
        subClasses.add("atm");
        subClasses.add("bank"); //added
        subClasses.add("bureau_de_change");
        // healthcare
        subClasses.add("baby_hatch");
        subClasses.add("clinic");
        subClasses.add("dentist");
        subClasses.add("doctors");
        subClasses.add("hospital");
        subClasses.add("nursing_home");
        subClasses.add("pharmacy");
        subClasses.add("social_facility");
        subClasses.add("veterinary");
        subClasses.add("blood_donation"); // doesn't exist?
        // entertainment, arts and culture
        subClasses.add("arts_centre");
        subClasses.add("brothel");
        subClasses.add("casino");
        subClasses.add("cinema");
        subClasses.add("community_centre");
        subClasses.add("fountain");
        subClasses.add("gambling");
        subClasses.add("nightclub");
        subClasses.add("planetarium");
        subClasses.add("public_bookcase");
        subClasses.add("social_centre");
        subClasses.add("stripclub");
        subClasses.add("studio");
        subClasses.add("swingerclub");
        subClasses.add("theatre");
        // others
        subClasses.add("animal_boarding");
        subClasses.add("animal_shelter");
        subClasses.add("baking_oven"); // added
        subClasses.add("bench");
        subClasses.add("childcare"); // added
        subClasses.add("clock");
        subClasses.add("conference_centre"); // added
        subClasses.add("courthouse");
        subClasses.add("crematorium");
        subClasses.add("coworking_space"); // doesn't exist?
        subClasses.add("crypt"); // doesn't exist?
        subClasses.add("dive_centre");
        subClasses.add("dojo"); // doesn't exist?
        subClasses.add("embassy");
        subClasses.add("fire_station");
        subClasses.add("game_feeding"); // doesn't exist?
        subClasses.add("give_box"); // added
        subClasses.add("grave_yard");
        subClasses.add("hunting_stand");
        subClasses.add("internet_cafe");
        subClasses.add("kitchen"); // added
        subClasses.add("kneipp_water_cure");
        subClasses.add("marketplace");
        subClasses.add("monastery"); // added
        subClasses.add("photo_booth");
        subClasses.add("place_of_worship");
        subClasses.add("police");
        subClasses.add("post_box");
        subClasses.add("post_depot"); // added
        subClasses.add("post_office");
        subClasses.add("prison");
        subClasses.add("public_bath"); // added
        subClasses.add("ranger_station");
        subClasses.add("recycling");
        subClasses.add("refugee_site"); // added
        subClasses.add("rescue_station"); // doesn't exist?
        subClasses.add("post_depot"); // added
        subClasses.add("sanitary_dump_station"); // added
        subClasses.add("sauna"); // deprecated -> leisure
        subClasses.add("shelter");
        subClasses.add("shower");
        subClasses.add("table"); // doesn't exist?
        subClasses.add("telephone");
        subClasses.add("toilets");
        subClasses.add("townhall");
        subClasses.add("vending_machine");
        subClasses.add("waste_basket");
        subClasses.add("waste_disposal");
        subClasses.add("waste_transfer_station");
        subClasses.add("watering_place");
        subClasses.add("water_point");
        //user defined - probably not needed
        
        osmFeatureClasses.put("amenity", subClasses);                
        
        // Barrier (but only cable_barrier, city_wall, ditch, fence, retaining_wall, tank_trap, wall) WHY?
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("unknown");
        subClasses.add("cable_barrier");
        subClasses.add("city_wall");
        subClasses.add("ditch");
        subClasses.add("fence");
        //subClasses.add("guard_rail"); // added
        //subClasses.add("handrail"); // added
        //subClasses.add("hedge"); // added
        //subClasses.add("kerb"); // added
        subClasses.add("retaining_wall");
        subClasses.add("tank_trap"); // doesn't exist?
        subClasses.add("wall");

        osmFeatureClasses.put("barrier", subClasses);                
        
        // Boundary
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("aboriginal_lands"); // added
        subClasses.add("administrative");
        subClasses.add("historic"); // doesn't exist?
        subClasses.add("maritime");
        subClasses.add("marker"); // added
        subClasses.add("national_park");
        subClasses.add("political");
        subClasses.add("postal_code");
        subClasses.add("religious_administration"); // doesn't exist?
        subClasses.add("protected_area");
        
        osmFeatureClasses.put("boundary", subClasses);        
        
        // Building
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        //accommodation
        subClasses.add("apartments");
        subClasses.add("bungalow");
        subClasses.add("cabin");
        subClasses.add("detached");
        subClasses.add("dormitory");
        subClasses.add("farm");
        subClasses.add("ger"); // added
        subClasses.add("hotel");
        subClasses.add("house");
        subClasses.add("houseboat");
        subClasses.add("residential");
        subClasses.add("semidetached_house"); // added
        subClasses.add("static_caravan");
        subClasses.add("terrace");
        // commercial
        subClasses.add("commercial");
        subClasses.add("industrial");
        subClasses.add("kiosk");
        subClasses.add("office"); // added
        subClasses.add("retail");
        subClasses.add("supermarket"); // added
        subClasses.add("warehouse"); // added
        // religious
        subClasses.add("cathedral");
        subClasses.add("chapel");
        subClasses.add("church");
        subClasses.add("mosque");
        subClasses.add("religious"); // added
        subClasses.add("shrine");
        subClasses.add("synagogue");
        subClasses.add("temple");
        // civic / amenity
        subClasses.add("bakehouse"); // added
        subClasses.add("civic");
        subClasses.add("fire_station"); // added
        subClasses.add("government"); // added
        subClasses.add("hospital");
        subClasses.add("kindergarten"); // added
        subClasses.add("public");
        subClasses.add("school");
        subClasses.add("toilets"); // added
        subClasses.add("train_station");
        subClasses.add("transportation");
        subClasses.add("university");
        // agricultural / plant production
        subClasses.add("barn");
        subClasses.add("conservatory"); // added
        subClasses.add("cowshed");
        subClasses.add("farm_auxiliary");
        subClasses.add("greenhouse");
        subClasses.add("slurry_tank"); // added
        subClasses.add("stable");
        subClasses.add("sty"); // added
        //sports
        subClasses.add("grandstand"); // added
        subClasses.add("pavilion"); // added
        subClasses.add("riding_hall"); // added
        subClasses.add("sports_hall"); // added
        subClasses.add("stadium");
        //storage
        subClasses.add("hangar");
        subClasses.add("hut");
        subClasses.add("shed");
        // cars
        subClasses.add("carport"); // added
        subClasses.add("garage");
        subClasses.add("garages");
        subClasses.add("parking"); // added
        // power / technical buildings
        subClasses.add("digester"); // added
        subClasses.add("service");
        subClasses.add("transformer_tower");
        subClasses.add("water_tower"); // added
        // other buildings
        subClasses.add("bunker");
        subClasses.add("bridge");
        subClasses.add("construction");
        subClasses.add("gatehouse"); // added
        subClasses.add("roof");
        subClasses.add("ruins");
        subClasses.add("tree_house"); // added
        //subClasses.add("yes"); // added

        osmFeatureClasses.put("building", subClasses);        
        
        // Craft
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("agricultural_engines");
        subClasses.add("atelier"); // added
        subClasses.add("bakery");
        subClasses.add("basket_maker");
        subClasses.add("beekeeper");
        subClasses.add("blacksmith");
        subClasses.add("boatbuilder");
        subClasses.add("bookbinder");
        subClasses.add("brewery");
        subClasses.add("builder");
        subClasses.add("cabinet_maker"); // added
        subClasses.add("car_painter"); // added
        subClasses.add("carpenter");
        subClasses.add("carpet_layer");
        subClasses.add("caterer");
        subClasses.add("chimney_sweeper");
        subClasses.add("clockmaker");
        subClasses.add("confectionery");
        subClasses.add("cooper"); // added
        subClasses.add("dental_technician"); // added
        subClasses.add("distillery");
        subClasses.add("door_construction"); // added
        subClasses.add("dressmaker");
        subClasses.add("electronics_repair"); // added
        subClasses.add("embroiderer"); // added
        subClasses.add("electrician");
        subClasses.add("engraver"); // added
        subClasses.add("floorer");
        subClasses.add("gardener");
        subClasses.add("glaziery");
        subClasses.add("grinding_mill"); // added
        subClasses.add("handicraft");
        subClasses.add("hvac");
        subClasses.add("insulation");
        subClasses.add("jeweller");
        subClasses.add("joiner"); // added
        subClasses.add("key_cutter"); // added
        subClasses.add("locksmith");
        subClasses.add("metal_construction");
        subClasses.add("mint"); // added
        subClasses.add("musical_instrument"); // added
        subClasses.add("oil_mill"); // added
        subClasses.add("optician");
        //subClasses.add("optician"); //double
        subClasses.add("organ_builder"); // added
        subClasses.add("painter");
        subClasses.add("parquet_layer");
        subClasses.add("photographer");
        subClasses.add("photographic_laboratory");
        subClasses.add("piano_tuner");
        subClasses.add("plasterer");
        subClasses.add("plumber");
        subClasses.add("pottery");
        subClasses.add("printer"); // added
        subClasses.add("print_maker"); // added
        subClasses.add("rigger");
        subClasses.add("roofer");
        subClasses.add("saddler");
        subClasses.add("sailmaker");
        subClasses.add("sawmill");
        subClasses.add("scaffolder");
        subClasses.add("sculptor");
        subClasses.add("shoemaker");
        subClasses.add("sign_maker"); // added
        subClasses.add("stand_builder");
        subClasses.add("stonemason");
        subClasses.add("sun_protection");
        subClasses.add("tailor");
        subClasses.add("tiler");
        subClasses.add("tinsmith");
        subClasses.add("toolmaker"); // added
        subClasses.add("turner");
        subClasses.add("upholsterer");
        subClasses.add("watchmaker");
        subClasses.add("water_well_drilling"); // added
        subClasses.add("window_construction");
        subClasses.add("winery");
        
        osmFeatureClasses.put("craft", subClasses);        
        
        // Emergency
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        // medical rescue
        subClasses.add("ambulance_station");
        subClasses.add("defibrillator");
        subClasses.add("landing_site"); // added
        subClasses.add("emergency_ward_entrance"); // added
        // firefighter
        subClasses.add("dry_riser_inlet"); // added
        subClasses.add("fire_alarm_box"); // added
        subClasses.add("fire_extinguisher");
        subClasses.add("fire_flapper"); // doesn't exist?
        subClasses.add("fire_hose");
        subClasses.add("fire_hydrant");
        subClasses.add("water_tank");
        subClasses.add("suction_point"); // added
        // lifeguards
        subClasses.add("lifeguard"); // added
        subClasses.add("lifeguard_base");
        subClasses.add("lifeguard_place"); // doesn't exist?
        subClasses.add("lifeguard_tower"); // added
        subClasses.add("lifeguard_platform"); // added
        subClasses.add("life_ring");
        // assembly point
        subClasses.add("assembly_point");
        // other structure
        subClasses.add("phone");
        subClasses.add("siren");
        subClasses.add("ses_station"); // doesn't exist?
        subClasses.add("drinking_water"); // added

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
        // roads
        subClasses.add("motorway");
        subClasses.add("trunk");
        subClasses.add("primary");
        subClasses.add("secondary");
        subClasses.add("tertiary");
        subClasses.add("unclassified");
        subClasses.add("residential"); // added
        // link roads
        subClasses.add("motorway_link");
        subClasses.add("trunk_link");
        subClasses.add("primary_link");
        subClasses.add("secondary_link");
        subClasses.add("tertiary_link");
        // special road types
        subClasses.add("living_street");
        subClasses.add("service");
        subClasses.add("pedestrian");
        subClasses.add("track");
        subClasses.add("bus_guideway");
        subClasses.add("escape");
        subClasses.add("raceway");
        subClasses.add("road");
        // paths
        subClasses.add("footway");
        subClasses.add("bridleway");
        subClasses.add("steps");
        subClasses.add("corridor"); // added
        subClasses.add("path");
        subClasses.add("cycleway");
        // lifecycle
        subClasses.add("proposed");
        subClasses.add("construction");
        // other highway features
        subClasses.add("bus_stop");
        //subClasses.add("bus_stop"); // double
        subClasses.add("crossing");
        subClasses.add("elevator");
        subClasses.add("emergency_access_point");
        subClasses.add("give_way");
        subClasses.add("phone"); // added
        subClasses.add("milestone"); // added
        subClasses.add("mini_roundabout");
        subClasses.add("motorway_junction");
        subClasses.add("passing_place");
        subClasses.add("platform"); // added
        subClasses.add("rest_area");
        subClasses.add("speed_camera");
        subClasses.add("street_lamp");
        subClasses.add("services");
        subClasses.add("stop");
        subClasses.add("traffic_mirror"); // added
        subClasses.add("traffic_signals");
        subClasses.add("trailhead"); // added
        subClasses.add("turning_circle");
        subClasses.add("turning_loop"); // added
        subClasses.add("toll_gantry"); // added

        // somehow include "highway+cycleway=key" and "highway+sidewalk=key" and "highway+busway=lane" ?

        osmFeatureClasses.put("highway", subClasses);

        // Historic (of course :) )
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("aircraft");
        subClasses.add("aqueduct"); // added
        subClasses.add("archaeological_site");
        subClasses.add("battlefield");
        subClasses.add("boundary_stone");
        subClasses.add("building");
        subClasses.add("cannon");
        subClasses.add("castle");
        subClasses.add("castle_wall"); // added
        subClasses.add("church"); // added
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
        subClasses.add("railway_car"); // added
        subClasses.add("ruins");
        subClasses.add("rune_stone");
        subClasses.add("ship");
        subClasses.add("tank"); // added
        subClasses.add("tomb"); // added
        subClasses.add("tower"); // added
        subClasses.add("wayside_cross");
        subClasses.add("wayside_shrine");
        subClasses.add("wreck");
        //subClasses.add("yes"); // added
        
        osmFeatureClasses.put("historic", subClasses);        
        
        // Landuse
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        // developed land
        subClasses.add("commercial");
        subClasses.add("construction");
        subClasses.add("industrial");
        subClasses.add("residential");
        subClasses.add("retail");
        // rural and agricultural land
        subClasses.add("allotments");
        subClasses.add("farmland");
        subClasses.add("farmyard");
        subClasses.add("forest");
        subClasses.add("meadow");
        subClasses.add("orchard");
        subClasses.add("vineyard");
        // other
        subClasses.add("basin");
        subClasses.add("brownfield");
        subClasses.add("cemetery");
        subClasses.add("conservation"); // deprecated?
        subClasses.add("depot"); // added
        subClasses.add("garages");
        subClasses.add("grass");
        subClasses.add("greenfield");
        subClasses.add("greenhouse_horticulture");
        subClasses.add("landfill");
        subClasses.add("military");
        subClasses.add("pasture"); // doesn't exist?
        subClasses.add("peat_cutting"); // deprecated?
        subClasses.add("plant_nursery");
        subClasses.add("port");
        subClasses.add("quarry"); // added
        subClasses.add("railway");
        subClasses.add("recreation_ground");
        subClasses.add("religious"); // added
        subClasses.add("reservoir");
        subClasses.add("salt_pond");
        subClasses.add("village_green");

        osmFeatureClasses.put("landuse", subClasses);        

        // leisure
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("adult_gaming_centre");
        subClasses.add("amusement_arcade");
        subClasses.add("beach_resort");
        subClasses.add("bandstand");
        subClasses.add("bird_hide");
        subClasses.add("common");
        subClasses.add("dance");
        subClasses.add("disc_golf_course"); // added
        subClasses.add("dog_park");
        subClasses.add("escape_game"); // added
        subClasses.add("firepit");
        subClasses.add("fishing");
        subClasses.add("fitness_centre");
        subClasses.add("fitness_station"); // added
        subClasses.add("garden");
        subClasses.add("golf_course"); // doesn't exist?
        subClasses.add("hackerspace");
        subClasses.add("horse_riding");
        subClasses.add("ice_rink");
        subClasses.add("marina");
        subClasses.add("miniature_golf");
        subClasses.add("nature_reserve");
        subClasses.add("park");
        subClasses.add("picnic_table");
        subClasses.add("pitch");
        subClasses.add("playground");
        subClasses.add("slipway");
        subClasses.add("sports_centre");
        subClasses.add("stadium");
        subClasses.add("summer_camp");
        subClasses.add("swimming_area");
        subClasses.add("swimming_pool"); // added
        subClasses.add("track");
        subClasses.add("water_park");
        subClasses.add("wildlife_hide"); // doesn't exist?
        
        osmFeatureClasses.put("leisure", subClasses);        
        
        // Man Made
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("adit");
        subClasses.add("beacon");
        subClasses.add("breakwater");
        subClasses.add("bridge");
        subClasses.add("bunker_silo");
        subClasses.add("campanile"); // doesn't exist?
        subClasses.add("carpet_hanger"); // added
        subClasses.add("chimney");
        subClasses.add("communications_tower");
        subClasses.add("crane");
        subClasses.add("cross");
        subClasses.add("cutline");
        subClasses.add("clearcut");
        subClasses.add("dovecote");
        subClasses.add("dyke");
        subClasses.add("embankment");
        subClasses.add("flagpole");
        subClasses.add("gasometer");
        subClasses.add("goods_conveyor"); // added
        subClasses.add("groyne");
        subClasses.add("hot_water_tank"); // doesn't exist?
        subClasses.add("kiln");
        subClasses.add("lighthouse");
        subClasses.add("mast");
        subClasses.add("mineshaft");
        subClasses.add("monitoring_station");
        subClasses.add("obelisk");
        subClasses.add("observatory");
        subClasses.add("offshore_platform");
        subClasses.add("petroleum_well");
        subClasses.add("pier");
        subClasses.add("pipeline");
        subClasses.add("pumping_station");
        subClasses.add("reservoir_covered");
        subClasses.add("silo");
        subClasses.add("snow_fence");
        subClasses.add("snow_net");
        subClasses.add("storage_tank");
        subClasses.add("street_cabinet");
        subClasses.add("surveillance");
        subClasses.add("survey_point");
        subClasses.add("telescope");
        subClasses.add("tower");
        subClasses.add("wastewater_plant");
        subClasses.add("watermill");
        subClasses.add("water_tower");
        subClasses.add("water_well");
        subClasses.add("water_tap");
        subClasses.add("water_works");
        subClasses.add("wildlife_crossing");
        subClasses.add("windmill");
        subClasses.add("works");
        //subClasses.add("yes"); // added
        
        osmFeatureClasses.put("man_made", subClasses);        
        
        // Military
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("airfield");
        subClasses.add("ammunition"); // doesn't exist?
        subClasses.add("bunker");
        subClasses.add("barracks");
        subClasses.add("checkpoint");
        subClasses.add("danger_area");
        subClasses.add("naval_base");
        subClasses.add("nuclear_explosion_site");
        subClasses.add("obstacle_course");
        subClasses.add("office");
        subClasses.add("range");
        subClasses.add("training_area");
        subClasses.add("trench");
        //subClasses.add("airfield"); // double

        osmFeatureClasses.put("military", subClasses);        

        // Natural
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        // vegetation or surface related
        subClasses.add("wood");
        subClasses.add("tree_row");
        subClasses.add("tree");
        subClasses.add("scrub");
        subClasses.add("heath");
        subClasses.add("moor");
        subClasses.add("grassland");
        subClasses.add("fell");
        subClasses.add("bare_rock");
        subClasses.add("scree");
        subClasses.add("shingle");
        subClasses.add("sand");
        subClasses.add("mud");
        // water related
        subClasses.add("water");
        subClasses.add("wetland");
        subClasses.add("glacier");
        subClasses.add("bay");
        subClasses.add("strait"); // added
        subClasses.add("cape"); // added
        subClasses.add("beach");
        subClasses.add("coastline"); // ignored by osm2pgsql
        subClasses.add("reef"); // added
        subClasses.add("spring");
        subClasses.add("hot_spring");
        subClasses.add("geyser");
        subClasses.add("blowhole"); // added
        // landform related
        subClasses.add("peak");
        subClasses.add("volcano");
        subClasses.add("valley");
        subClasses.add("peninsula"); // added
        subClasses.add("isthmus"); // added
        subClasses.add("ridge");
        subClasses.add("arete");
        subClasses.add("cliff");
        subClasses.add("saddle");
        subClasses.add("dune"); // added
        subClasses.add("rock");
        subClasses.add("stone");
        subClasses.add("sinkhole");
        subClasses.add("cave_entrance");

        osmFeatureClasses.put("natural", subClasses);        

        // Office
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("accountant");
        subClasses.add("adoption_agency");
        subClasses.add("advertising_agency");
        subClasses.add("architect");
        subClasses.add("association");
        subClasses.add("bail_bond_agent"); // added
        subClasses.add("charity"); // added
        subClasses.add("company");
        subClasses.add("consulting"); // added
        subClasses.add("coworking"); // added
        subClasses.add("diplomatic"); // added
        subClasses.add("educational_institution");
        subClasses.add("employment_agency");
        subClasses.add("energy_supplier");
        subClasses.add("engineer"); // added
        subClasses.add("estate_agent");
        subClasses.add("financial"); // added
        subClasses.add("forestry");
        subClasses.add("foundation");
        subClasses.add("geodesist"); // added
        subClasses.add("government");
        subClasses.add("graphic_design"); // added
        subClasses.add("guide");
        subClasses.add("harbour_master"); // added
        subClasses.add("insurance");
        subClasses.add("it");
        subClasses.add("lawyer");
        subClasses.add("logistics");
        subClasses.add("moving_company");
        subClasses.add("newspaper");
        subClasses.add("ngo");
        subClasses.add("notary");
        subClasses.add("political_party");
        subClasses.add("private_investigator");
        subClasses.add("quango");
        subClasses.add("real_estate_agent"); // doesn't exist?
        subClasses.add("religion");
        subClasses.add("research");
        subClasses.add("security"); // added
        subClasses.add("surveyor"); // added
        subClasses.add("tax");
        subClasses.add("tax_advisor");
        subClasses.add("telecommunication");
        subClasses.add("travel_agent"); // doesn't exist
        subClasses.add("union"); // added
        subClasses.add("visa"); // added
        subClasses.add("water_utility");
        //subClasses.add("yes"); // added

        osmFeatureClasses.put("office", subClasses);        

        // Place
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        // administratively declared places
        subClasses.add("country");
        subClasses.add("state");
        subClasses.add("region");
        subClasses.add("province");
        subClasses.add("district");
        subClasses.add("county");
        subClasses.add("municipality");
        // populated settlements, urban
        subClasses.add("city");
        subClasses.add("borough");
        subClasses.add("suburb");
        subClasses.add("quarter");
        subClasses.add("neighbourhood");
        subClasses.add("city_block");
        subClasses.add("plot");
        // populated settlements, urban and rural
        subClasses.add("town");
        subClasses.add("village");
        subClasses.add("hamlet");
        subClasses.add("isolated_dwelling");
        subClasses.add("farm");
        subClasses.add("allotments");
        // other places
        subClasses.add("continent");
        subClasses.add("archipelago");
        subClasses.add("island");
        subClasses.add("islet");
        subClasses.add("square");
        subClasses.add("locality"); // added
        subClasses.add("sea"); // added
        subClasses.add("ocean"); // added
             
        osmFeatureClasses.put("place", subClasses);

        // Power
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("cable");
        subClasses.add("catenary_mast");
        subClasses.add("compensator");
        subClasses.add("converter");
        subClasses.add("generator");
        subClasses.add("heliostat");
        subClasses.add("insulator");
        subClasses.add("line");
        // somehow include "power+line=busbar" and "power+line=bay" ?
        subClasses.add("minor_line");
        subClasses.add("plant");
        subClasses.add("pole");
        subClasses.add("portal");
        subClasses.add("substation");
        subClasses.add("switch");
        subClasses.add("switchgear"); // added
        subClasses.add("terminal");
        subClasses.add("tower");
        subClasses.add("transformer");

        osmFeatureClasses.put("power", subClasses);        

        // Public Transport
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("stop_position");
        subClasses.add("platform");
        subClasses.add("station");
        subClasses.add("stop_area");

        osmFeatureClasses.put("public_transport", subClasses);        

        // Railway
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        // tracks
        subClasses.add("abandoned");
        subClasses.add("construction");
        subClasses.add("disused");
        subClasses.add("funicular");
        subClasses.add("light_rail");
        subClasses.add("miniature");
        subClasses.add("monorail");
        subClasses.add("narrow_gauge");
        subClasses.add("preserved");
        subClasses.add("rail");
        subClasses.add("subway");
        subClasses.add("tram");
        // stations and stops
        subClasses.add("halt");
        // somehow include "railway+public_transport=stop_position" and "railway+public_transport=platform"
        // and "railway+public_transport=station"?
        subClasses.add("platform");
        subClasses.add("station");
        subClasses.add("subway_entrance");
        subClasses.add("tram_stop");
        // other railways
        subClasses.add("buffer_stop");
        subClasses.add("derail");
        subClasses.add("crossing");
        subClasses.add("level_crossing");
        // somehow include "railway+landuse=railway"?
        subClasses.add("signal");
        subClasses.add("switch");
        subClasses.add("railway_crossing");
        subClasses.add("turntable");
        subClasses.add("roundhouse");
        subClasses.add("traverser");
        subClasses.add("wash"); // added
             
        osmFeatureClasses.put("railway", subClasses);        
        
        // Route
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("bicycle");
        subClasses.add("bus");
        subClasses.add("canoe");
        subClasses.add("detour");
        subClasses.add("ferry");
        subClasses.add("fitness_trail"); // doesn't exist?
        subClasses.add("foot"); // added
        subClasses.add("hiking");
        subClasses.add("horse");
        subClasses.add("inline_skates");
        subClasses.add("light_rail");
        subClasses.add("mtb");
        subClasses.add("nordic_walking"); // doesn't exist?
        subClasses.add("pipeline"); // doesn't exist?
        subClasses.add("piste");
        subClasses.add("power");
        subClasses.add("railway");
        subClasses.add("road");
        subClasses.add("running");
        subClasses.add("ski");
        subClasses.add("subway"); // added
        subClasses.add("train");
        subClasses.add("tracks"); // added
        subClasses.add("tram");
        subClasses.add("trolleybus"); // added

        osmFeatureClasses.put("route", subClasses);

        // shop
        subClasses = new ArrayList<>();

        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        // food, beverages
        subClasses.add("alcohol");
        subClasses.add("bakery");
        subClasses.add("beverages");
        subClasses.add("brewing_supplies");
        subClasses.add("butcher");
        subClasses.add("cheese");
        subClasses.add("chocolate");
        subClasses.add("coffee");
        subClasses.add("confectionery");
        subClasses.add("convenience");
        subClasses.add("deli");
        subClasses.add("dairy");
        subClasses.add("farm");
        subClasses.add("frozen_food"); // added
        subClasses.add("greengrocer");
        subClasses.add("health_food"); // added
        subClasses.add("ice_cream");
        subClasses.add("organic"); // deprecated?
        subClasses.add("pasta");
        subClasses.add("pastry");
        subClasses.add("seafood");
        subClasses.add("spices");
        subClasses.add("tea");
        subClasses.add("wine"); // deprecated
        subClasses.add("water"); // added
        // general store, department store, mall
        subClasses.add("department_store");
        subClasses.add("general");
        subClasses.add("kiosk");
        subClasses.add("mall");
        subClasses.add("supermarket");
        subClasses.add("wholesale"); // added
        // clothing, shoes, accessories
        subClasses.add("baby_goods");
        subClasses.add("bag");
        subClasses.add("boutique");
        subClasses.add("clothes");
        subClasses.add("fabric");
        subClasses.add("fashion"); // deprecated
        subClasses.add("fashion_accessories"); // added
        subClasses.add("jewelry");
        subClasses.add("leather");
        subClasses.add("sewing"); // added
        subClasses.add("shoes");
        subClasses.add("tailor");
        subClasses.add("watches");
        subClasses.add("wool"); // added
        // discount store, charity
        subClasses.add("charity");
        subClasses.add("second_hand");
        subClasses.add("variety_store");
        // health and beauty
        subClasses.add("beauty");
        subClasses.add("chemist");
        subClasses.add("cosmetics");
        subClasses.add("drugstore"); // deprecated (discouraged)
        subClasses.add("erotic");
        subClasses.add("hairdresser");
        subClasses.add("hairdresser_supply"); // added
        subClasses.add("hearing_aids");
        subClasses.add("herbalist");
        subClasses.add("massage");
        subClasses.add("medical_supply");
        subClasses.add("nutrition_supplements");
        subClasses.add("optician");
        subClasses.add("perfumery");
        subClasses.add("tattoo");
        // Do-it-yourself, household, building materials, gardening
        subClasses.add("agrarian"); // added
        subClasses.add("appliance"); // added
        subClasses.add("bathroom_furnishing");
        subClasses.add("doityourself");
        subClasses.add("electrical");
        subClasses.add("energy");
        subClasses.add("fireplace");
        subClasses.add("florist");
        subClasses.add("garden_centre");
        subClasses.add("garden_furniture");
        subClasses.add("gas");
        subClasses.add("glaziery");
        subClasses.add("hardware");
        subClasses.add("houseware");
        subClasses.add("locksmith");
        subClasses.add("paint");
        subClasses.add("security"); // added
        subClasses.add("trade");
        //subClasses.add("windows"); // added, but deprecated
        // furniture and interior
        subClasses.add("antiques");
        subClasses.add("bed");
        subClasses.add("candles");
        subClasses.add("carpet");
        subClasses.add("curtain");
        subClasses.add("doors"); // added
        subClasses.add("flooring"); // added
        subClasses.add("furniture");
        subClasses.add("household_linen"); // added
        subClasses.add("interior_decoration");
        subClasses.add("kitchen");
        subClasses.add("lamps"); // deprecated
        subClasses.add("lighting"); // added
        subClasses.add("tiles");
        subClasses.add("window_blind");
        // electronics
        subClasses.add("computer");
        subClasses.add("electronics");
        subClasses.add("hifi");
        subClasses.add("mobile_phone");
        subClasses.add("radiotechnics");
        subClasses.add("vacuum_cleaner");
        // outdoors and sport, vehicles
        subClasses.add("atv"); // added
        subClasses.add("bicycle");
        subClasses.add("boat"); // added
        subClasses.add("car");
        subClasses.add("car_repair");
        subClasses.add("car_parts");
        subClasses.add("caravan"); // added
        subClasses.add("fuel");
        subClasses.add("fishing");
        subClasses.add("free_flying"); // doesn't exist?
        subClasses.add("golf"); // added
        subClasses.add("hunting");
        subClasses.add("jetski"); // added
        subClasses.add("military_surplus"); // added
        subClasses.add("motorcycle");
        subClasses.add("outdoor");
        subClasses.add("scuba_diving");
        subClasses.add("ski"); // added
        subClasses.add("snowmobile"); // added
        subClasses.add("sports");
        subClasses.add("swimming_pool");
        subClasses.add("trailer"); // added
        subClasses.add("tyres");
        // art, music, hobbies
        subClasses.add("art");
        subClasses.add("collector");
        subClasses.add("craft");
        subClasses.add("frame");
        subClasses.add("games");
        subClasses.add("model"); // added
        subClasses.add("music");
        subClasses.add("musical_instrument");
        subClasses.add("photo");
        subClasses.add("camera");
        subClasses.add("trophy");
        subClasses.add("video");
        subClasses.add("video_games");
        // stationery, gifts, books, newspapers
        subClasses.add("anime");
        subClasses.add("books");
        subClasses.add("gift");
        subClasses.add("lottery");
        subClasses.add("newsagent");
        subClasses.add("stationery");
        subClasses.add("ticket");
        // others
        subClasses.add("bookmaker");
        subClasses.add("cannabis"); // added
        subClasses.add("copyshop");
        subClasses.add("dry_cleaning");
        subClasses.add("e-cigarette");
        subClasses.add("funeral_directors");
        subClasses.add("laundry");
        subClasses.add("money_lender");
        subClasses.add("party"); // added
        subClasses.add("pawnbroker");
        subClasses.add("pet");
        subClasses.add("pet_grooming"); // added
        subClasses.add("pest_control"); // added
        subClasses.add("pyrotechnics");
        subClasses.add("religion");
        subClasses.add("tobacco");
        subClasses.add("toys");
        subClasses.add("travel_agency");
        subClasses.add("vacant");
        subClasses.add("weapons");
        subClasses.add("outpost"); // added
        
        osmFeatureClasses.put("shop", subClasses);   
        
        // sport
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("9pin");
        subClasses.add("10pin");
        subClasses.add("american_football");
        subClasses.add("aikido");
        subClasses.add("archery");
        subClasses.add("athletics");
        subClasses.add("australian_football");
        subClasses.add("badminton");
        subClasses.add("bandy"); // added
        subClasses.add("baseball");
        subClasses.add("basketball");
        subClasses.add("beachvolleyball");
        subClasses.add("biathlon"); // added
        subClasses.add("billiards");
        subClasses.add("bmx");
        subClasses.add("bobsleigh");
        subClasses.add("boules");
        subClasses.add("bowls");
        subClasses.add("boxing");
        subClasses.add("bullfighting"); // added
        subClasses.add("canadian_football");
        subClasses.add("canoe");
        subClasses.add("chess");
        subClasses.add("cliff_diving");
        subClasses.add("climbing");
        subClasses.add("climbing_adventure");
        subClasses.add("cockfighting");
        subClasses.add("cricket");
        subClasses.add("crossfit"); // added
        subClasses.add("croquet");
        subClasses.add("curling");
        subClasses.add("cycling");
        subClasses.add("darts");
        subClasses.add("dog_agility"); // added
        subClasses.add("dog_racing");
        subClasses.add("equestrian");
        subClasses.add("fencing");
        subClasses.add("field_hockey");
        subClasses.add("fitness"); // added
        subClasses.add("floorball"); // added
        subClasses.add("free_flying");
        subClasses.add("futsal"); // added
        subClasses.add("gaelic_games");
        subClasses.add("golf");
        subClasses.add("gymnastics");
        subClasses.add("handball");
        subClasses.add("hapkido");
        subClasses.add("horseshoes");
        subClasses.add("horse_racing");
        subClasses.add("ice_hockey");
        subClasses.add("ice_skating");
        subClasses.add("ice_stock");
        subClasses.add("jiu-jitsu"); // added
        subClasses.add("judo");
        subClasses.add("karate"); // added
        subClasses.add("karting");
        subClasses.add("kickboxing"); // added
        subClasses.add("kitesurfing");
        subClasses.add("korfball");
        subClasses.add("krachtbal"); // added
        subClasses.add("lacrosse"); // added
        subClasses.add("martial_arts"); // added
        subClasses.add("miniature_golf"); // added
        subClasses.add("model_aerodrome");
        subClasses.add("motocross");
        subClasses.add("motor");
        subClasses.add("multi");
        subClasses.add("netball");
        subClasses.add("obstacle_course");
        subClasses.add("orienteering");
        subClasses.add("paddle_tennis");
        subClasses.add("padel"); // added
        subClasses.add("parachuting");
        subClasses.add("paragliding"); // doesn't exist?
        subClasses.add("pelota");
        subClasses.add("pesäpallo"); // added
        subClasses.add("pickleball"); // added
        subClasses.add("pilates"); // added
        subClasses.add("racquet");
        subClasses.add("rc_car");
        subClasses.add("roller_skating");
        subClasses.add("rowing");
        subClasses.add("rugby_league");
        subClasses.add("rugby_union");
        subClasses.add("running");
        subClasses.add("sailing");
        subClasses.add("scuba_diving");
        subClasses.add("shooting");
        subClasses.add("shot-put"); // added
        subClasses.add("skateboard");
        subClasses.add("ski_jumping"); // added
        subClasses.add("skiing"); // added
        subClasses.add("snooker"); // added
        subClasses.add("soccer");
        subClasses.add("speedway"); // added
        subClasses.add("squash"); // added
        subClasses.add("sumo");
        subClasses.add("surfing");
        subClasses.add("swimming");
        subClasses.add("table_tennis");
        subClasses.add("table_soccer");
        subClasses.add("taekwondo");
        subClasses.add("tennis");
        subClasses.add("toboggan");
        subClasses.add("ultimate"); // added
        subClasses.add("volleyball");
        subClasses.add("wakeboarding"); // added
        subClasses.add("water_polo");
        subClasses.add("water_ski");
        subClasses.add("weightlifting");
        subClasses.add("wrestling");
        subClasses.add("yoga");

        osmFeatureClasses.put("sport", subClasses);

        ////////////////////////////////////////////////////////////////
        // added FeatureClass

        // Telecom
        subClasses = new ArrayList<>();

        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("exchange");
        subClasses.add("connection_point");
        subClasses.add("distribution_point");
        subClasses.add("service_device");
        subClasses.add("data_center");

        osmFeatureClasses.put("telecom", subClasses);

        // added FeatureClass
        ////////////////////////////////////////////////////////////////

        // tourism
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        subClasses.add("alpine_hut");
        subClasses.add("apartment");
        subClasses.add("aquarium");
        subClasses.add("artwork");
        subClasses.add("attraction");
        subClasses.add("camp_pitch"); // added
        subClasses.add("camp_site");
        subClasses.add("caravan_site");
        subClasses.add("chalet");
        subClasses.add("gallery");
        subClasses.add("guest_house");
        subClasses.add("hostel");
        subClasses.add("hotel");
        subClasses.add("information");
        subClasses.add("motel");
        subClasses.add("museum");
        subClasses.add("picnic_site");
        subClasses.add("theme_park");
        subClasses.add("viewpoint");
        subClasses.add("wilderness_hut");
        subClasses.add("zoo");
        //subClasses.add("yes"); // added
             
        osmFeatureClasses.put("tourism", subClasses);   
        
        // Waterway
        subClasses = new ArrayList<>();
        
        // fill with all known subclasses
        subClasses.add(UNDEFINED);
        // natural watercourses
        subClasses.add("river");
        subClasses.add("riverbank");
        subClasses.add("stream");
        subClasses.add("tidal_channel"); // added
        // man-made waterways
        subClasses.add("canal");
        subClasses.add("drain");
        subClasses.add("ditch");
        subClasses.add("pressurised"); // added
        subClasses.add("wadi"); // doesn't exist?
        subClasses.add("fairway");
        // facilities
        subClasses.add("dock");
        subClasses.add("boatyard");
        // barriers on waterways
        subClasses.add("dam");
        subClasses.add("weir");
        subClasses.add("waterfall ");
        subClasses.add("lock_gate");
        // other features on waterways
        subClasses.add("turning_point");
        subClasses.add("water_point");
        subClasses.add("fuel");
             
        osmFeatureClasses.put("waterway", subClasses);   
        
        subClasses = new ArrayList<>();
        subClasses.add("adminlevel_1");
        subClasses.add("adminlevel_2");
        subClasses.add("adminlevel_3");
        subClasses.add("adminlevel_4");
        subClasses.add("adminlevel_5");
        subClasses.add("adminlevel_6");
        subClasses.add("adminlevel_7");
        subClasses.add("adminlevel_8");
        subClasses.add("adminlevel_9");
        subClasses.add("adminlevel_10");
        subClasses.add("adminlevel_11");
        subClasses.add("adminlevel_12");
        
        osmFeatureClasses.put("ohdm_boundary", subClasses);        
                
        this.setupClassIDs_Names();
    }

    HashMap<Integer, String> classID_ClassName = new HashMap<>();
    HashMap<Integer, String> classID_SubclassName = new HashMap<>();

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
                String fullClassName = OSMClassification.createFullClassName(className, subClassName);
                
                this.classIDs.put(fullClassName, id);
                this.fullClassNames.add(fullClassName);
                this.classID_ClassName.put(id, className);
                this.classID_SubclassName.put(id, subClassName);
                id++;
            }
        }
    }

    public static final String CLASS_SUBCLASS_SEPERATORS_SIGN = "_";



    public static String createFullClassName(String className, String subclassname) {
        return className + CLASS_SUBCLASS_SEPERATORS_SIGN + subclassname;
    }

    public String getClassNameById(int classid) {
        return this.classID_ClassName.get(classid);
    }

    public String getClassNameByFullName(String fullClassName) {
        int index = fullClassName.indexOf(CLASS_SUBCLASS_SEPERATORS_SIGN);
        if(index == -1) {
            return fullClassName;
        }

        return fullClassName.substring(0, index);
    }

    public String getSubClassNameByFullName(String fullClassName) {
        int index = fullClassName.indexOf(CLASS_SUBCLASS_SEPERATORS_SIGN);
        if(index == -1) {
            return "undefined";
        }

        return fullClassName.substring(index+1);
    }


    public String getFullClassName(String classCodeString) {
        return this.getFullClassName(Integer.parseInt(classCodeString));
    }

    public String getFullClassName(int classCode) {
        String nothing = "undefined";

        if(classCode > -1 && this.fullClassNames.size() > classCode) {
            // classIds start with 1 but index with 0
            return this.fullClassNames.get(classCode-1);
        }

        return nothing;
    }

    /**
     * @param className
     * @param subClassName
     * @return -1 if no known class and sub class name, a non-negative number 
     * otherwise
     */
    public int getOHDMClassID(String className, String subClassName) {
        String fullClassName = OSMClassification.createFullClassName(className, subClassName);
        
        // find entry
        Integer id = this.classIDs.get(fullClassName);
        if(id != null) {
            return id;
        }
        
        // try undefined
        fullClassName = OSMClassification.createFullClassName(className, OSMClassification.UNDEFINED);
        id = this.classIDs.get(fullClassName);
        if(id != null) {
            return id;
        }
        
//        System.out.println("not found: " + this.createFullClassName(className, subClassName));
        
        // else
        return -1;
    }
    
    private boolean isClassName(String key) {
        return OSMClassification.getOSMClassification().osmFeatureClasses.keySet().contains(key);
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
        Iterator<String> classIter = OSMClassification.osmClassification.osmFeatureClasses.keySet().iterator();

        while(classIter.hasNext()) {
            String className = classIter.next();
            List<String> subClasses = OSMClassification.osmClassification.osmFeatureClasses.get(className);
            Iterator<String> subClassIter = subClasses.iterator();

            while(subClassIter.hasNext()) {
                String subClassName = subClassIter.next();
                
                // add to database
                sq.append("INSERT INTO ");
                sq.append(classificationTableName);
                sq.append(" (class, subclassname) VALUES ('");
                sq.append(className);
                sq.append("', '");
                sq.append(subClassName);
                sq.append("') RETURNING id;");
                
                ResultSet insertResult = sq.executeWithResult();
                insertResult.next();
                int classID = insertResult.getInt(1);
                
                // keep in memory
                String fullClassName = OSMClassification.createFullClassName(className, subClassName);
                
                this.classIDs.put(fullClassName, classID);
            }
        }
    }
    
    public static final String CLASSIFICATIONTABLE = "classification";
    
    /**
     * reads classification into memory from table or even creates classification
     * table if not exists (TODO: currently: table is always created)
     * @param sq
     * @param schema
     * @throws SQLException 
     */
    public void setupClassificationTable(SQLStatementQueue sq, String schema) throws SQLException {
        DB.drop(sq, schema, CLASSIFICATIONTABLE);
        
        // create table
        sq.append("CREATE TABLE ");
        sq.append(DB.getFullTableName(schema, CLASSIFICATIONTABLE));
        sq.append("(classcode bigint PRIMARY KEY, ");
        sq.append("classname character varying, ");
        sq.append("subclassname character varying);");

        // init first line: unknown classification
        sq.append("INSERT INTO ");
        sq.append(DB.getFullTableName(schema, CLASSIFICATIONTABLE));
        sq.append(" VALUES (-1, 'no_class', 'no_subclass');");

        // now append real data
        int id = 0;

        // create classification table
        // iterate classes
        Iterator<String> classIter = osmClassification.osmFeatureClasses.keySet().iterator();

        while(classIter.hasNext()) {
            String className = classIter.next();
            List<String> subClasses = osmClassification.osmFeatureClasses.get(className);
            Iterator<String> subClassIter = subClasses.iterator();

            while(subClassIter.hasNext()) {
                String subClassName = subClassIter.next();

                // keep in memory
                Integer idInteger = id;
                String fullClassName = OSMClassification.createFullClassName(className, subClassName);

                this.classIDs.put(fullClassName, idInteger);

                // add to database
                sq.append("INSERT INTO ");
                sq.append(CLASSIFICATIONTABLE);
                sq.append(" VALUES (");
                sq.append(id++);
                sq.append(", '");
                sq.append(className);
                sq.append("', '");
                sq.append(subClassName);
                sq.append("');");
            }
        }
        
        sq.forceExecute();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    //                                ohdm specific helper stuff                                 //
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public List<String> getGenericTableNames(int ohdm_db_geomType) {
        List<String> tableNames = new ArrayList<>();
        for(String className : this.osmFeatureClasses.keySet()) {

            /* there will be a table for each class and type:
             * produce tableName [classname]_[geometryType]
             */
            String geometryName = OHDM_DB.getGeometryName(ohdm_db_geomType);

            // tables are named after their geometry but plural..
            tableNames.add(className + "_" + geometryName + "s");
        }

        return tableNames;
    }

    // for some naive tests
    public static void main(String args[]) {
        OSMClassification c = OSMClassification.getOSMClassification();
        
        String classname = OSMClassification.createFullClassName("admin_level", "12");
        int id = c.getOHDMClassID("admin_level", "12");
        System.out.println(classname + " / id: " + id);
    }

    public boolean classExists(String value) {
        return (this.osmFeatureClasses.keySet().contains(value));
    }
}
