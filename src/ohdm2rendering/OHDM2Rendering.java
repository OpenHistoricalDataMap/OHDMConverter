package ohdm2rendering;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import osm.OSMClassification;
import util.DB;
import util.OHDM_DB;
import util.SQLStatementQueue;
import util.Parameter;

/**
 *
 * @author thsc
 */
public class OHDM2Rendering {
    public static final String BBOX_FUNCTION_TAIL = ".ohdm_bboxGeometry";
    
    public static final String ALL = "all";
    public static final String GENERIC = "generic";
    public static final String V1 = "v1";
    public static final String BOUNDARIES = "boundaries";
    
    public static void main(String[] args) throws SQLException, IOException {
        String sourceParameterFileName = "db_ohdm.txt";
        String targetParameterFileName = "db_rendering.txt";
        

        if(args.length > 0) {
            sourceParameterFileName = args[0];
        }

        if(args.length > 1) {
            targetParameterFileName = args[1];
        }
            
//            Connection sourceConnection = Importer.createLocalTestSourceConnection();
//            Connection targetConnection = Importer.createLocalTestTargetConnection();

        Parameter sourceParameter = new Parameter(sourceParameterFileName);
        Parameter targetParameter = new Parameter(targetParameterFileName);

        Connection connection = DB.createConnection(targetParameter);

        String targetSchema = targetParameter.getSchema();
            
        String sourceSchema = sourceParameter.getSchema();
        
        SQLStatementQueue sql = new SQLStatementQueue(connection);
        OHDM2Rendering renderer = new OHDM2Rendering();
        
        renderer.setupRenderingDB(sql, targetSchema);
        
        String renderoutput = targetParameter.getRenderoutput();
        
        switch(renderoutput) {
            case ALL:
                renderer.doGeneric(sql, sourceSchema, targetSchema);
                renderer.doV1(sql, sourceSchema, targetSchema);
                renderer.doBoundaries(sql, sourceSchema, targetSchema);
                break;
            case GENERIC:
                renderer.doGeneric(sql, sourceSchema, targetSchema);
                break;
            case V1:
                renderer.doV1(sql, sourceSchema, targetSchema);
                break;
            case BOUNDARIES:
                renderer.doBoundaries(sql, sourceSchema, targetSchema);
                break;
            default:
                renderer.fatal("unknown rendering output (fatal): " + renderoutput);
        }
        
        System.out.println("Render tables creation finished");
    }
    
    private void doGeneric(SQLStatementQueue sql, String sourceSchema, 
            String targetSchema) throws SQLException {
        
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("produce generic rendering tables");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        this.createGeneric(sql, sourceSchema, targetSchema);
    }
    
    private void doV1(SQLStatementQueue sql, String sourceSchema, 
            String targetSchema) throws SQLException {

        this.doBoundaries(sql, sourceSchema, targetSchema);
        
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("produce rendering tables version 1");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        this.createV1(sql, sourceSchema, targetSchema);
    }
    
    private void doBoundaries(SQLStatementQueue sql, String sourceSchema, 
            String targetSchema) throws SQLException {
        
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("produce boundaries");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        this.createBoundaries(sql, sourceSchema, targetSchema);
    }
    
    void fatal(String message) {
        System.err.println(message);
        System.err.println("stop excecuting");
        System.exit(1);
    }
    
    void setupRenderingDB(SQLStatementQueue sql, String targetSchema) throws SQLException {
        String geometryName = null;
        
        String createBBOXName = targetSchema + ".ohdm_bboxGeometry";
        String createBBOXFunction = createBBOXName + "(boxstring character varying, srs integer)";
        
        sql.append("DROP FUNCTION ");
        sql.append(createBBOXName);
        sql.append("(character varying, integer);");
        try {
            sql.forceExecute();
        }
        catch(SQLException e) {
            System.err.println("exception ignored: " + e);
        }
        
        sql.append("CREATE OR REPLACE FUNCTION ");
        sql.append(createBBOXFunction);
        sql.append(" RETURNS geometry AS $$ DECLARE box geometry; BEGIN ");
        sql.append("SELECT st_asewkt(ST_MakeEnvelope (");
        sql.append("string_to_array[1]::double precision, ");
        sql.append("string_to_array[2]::double precision, ");
        sql.append("string_to_array[3]::double precision,  ");
        sql.append("string_to_array[4]::double precision, ");
        sql.append("srs)) FROM (SELECT string_to_array(boxstring,',')) as a");
        sql.append(" INTO box;");
        sql.append(" RETURN box;");
        sql.append(" END;");
        sql.append(" $$ LANGUAGE plpgsql;");
        sql.forceExecute();
    }
    
    void createGeneric(SQLStatementQueue sql, String sourceSchema, 
            String targetSchema) throws SQLException {
        
        OSMClassification classification = OSMClassification.getOSMClassification();
        for(String className : classification.osmFeatureClasses.keySet()) {

            /* there will be a table for each class and type:
             * produce tableName [classname]_[geometryType]
             */
            for(int geometryType = 1; geometryType <= 3; geometryType++) {

                String geometryName = OHDM_DB.getGeometryName(geometryType);

                // tables are named after their geometry but plural..
                String tableName = className + "_" + geometryName + "s";
                
                /* iterate all subclasses of this class. Create 
                rendering table in the first loop and fill it
                with data from other subclasses in following loops
                */
                boolean dropAndCreate = true;
                
                // iterate subclasses
                for(String subClassName : classification.osmFeatureClasses.get(className)) {
                    int classID = classification.getOHDMClassID(className, subClassName);
                    
                    this.produceRenderingTable(sql, dropAndCreate, 
                            sourceSchema, targetSchema, tableName, classID, 
                            geometryType);

                    // next loop, append lines to table, do not create
                    dropAndCreate = false;
                }
                
                // table filled, create function and index
                this.produceFunctionAndIndex(sql, 
                        targetSchema, tableName, geometryType);
            }
        }
    }
    
    void produceRenderingTable(SQLStatementQueue sql, boolean dropAndCreate, 
            String sourceSchema, String targetSchema, String tableName,
            int classID, int geometryType) throws SQLException {
        
            String targetFullTableName = targetSchema + "." + tableName;
            String geometryName = OHDM_DB.getGeometryName(geometryType);
/*
select l.line, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.valid_since_offset, gg.valid_until_offset into ohdm_rendering.test from
(SELECT id, classification_id, name from ohdm.geoobject where classification_id = 140) as o,
(SELECT id_target, type_target, id_geoobject_source, valid_since, valid_until, valid_since_offset, valid_until_offset FROM ohdm.geoobject_geometry) as gg,
(SELECT id, line FROM ohdm.lines) as l,
(SELECT subclassname, id FROM ohdm.classification) as c
where gg.type_target = 2 AND l.id = gg.id_target AND o.id = gg.id_geoobject_source AND o.classification_id = c.id;
            
select o.name, st_asewkt(p.polygon), c.class, c.subclassname from
(SELECT id, name from ohdm.geoobject) as o,
(SELECT id_target, type_target, classification_id, id_geoobject_source, valid_since, valid_until, valid_since_offset, valid_until_offset FROM ohdm.geoobject_geometry) as gg,
(SELECT id, polygon FROM ohdm.polygons) as p,
(SELECT class, subclassname, id FROM ohdm.classification) as c
where gg.type_target = 3 AND p.id = gg.id_target AND o.id = gg.id_geoobject_source AND gg.classification_id = c.id;            
         */
        
        
        // drop table in first loop
        if(dropAndCreate) {
            sql.append("drop table ");
            sql.append(targetFullTableName);
            sql.append(" CASCADE;");
            try {
                sql.forceExecute();
            }
            catch(SQLException e) {
                // ignore
            }
        }

        // add data in following loops
        if(!dropAndCreate) {
            sql.append("INSERT INTO ");
            sql.append(targetFullTableName);
            sql.append("( ");

            sql.append(geometryName);

            sql.append(", object_id, geom_id, classid, subclassname, name, valid_since, valid_until, tags, user_id) ");
        }

        sql.append("select g.");
        sql.append(geometryName);

        sql.append(", o.id as object_id, g.id as geom_id, c.id as classid, c.subclassname, o.name, gg.valid_since, ");
        sql.append("gg.valid_until, gg.tags, gg.user_id ");

        // create and fill in first loop
        if(dropAndCreate) {
            sql.append("into ");
            sql.append(targetFullTableName);
        }

        sql.append(" from");

        // geoobject o
        sql.append(" (SELECT id, name from ");
        sql.append(sourceSchema);
        sql.append(".geoobject) as o, ");

        // geoobject_geometry gg
        sql.append("(SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, ");
        sql.append("valid_until, tags, source_user_id as user_id FROM ");
        sql.append(sourceSchema);
        sql.append(".geoobject_geometry where classification_id = ");
        sql.append(classID);

        sql.append(") as gg, ");

        // geometry g
        sql.append("(SELECT id, ");
        sql.append(geometryName);
        sql.append(" FROM ");
        sql.append(sourceSchema);
        sql.append(".");
        sql.append(geometryName);
        sql.append("s"); // make it plural.. is plural geometry name in points, lines, polygons
        sql.append(") as g,");

        // classification c
        sql.append("(SELECT id, subclassname FROM ");
        sql.append(sourceSchema);
        sql.append(".classification where id = ");
        sql.append(classID);
        sql.append(") as c ");

        // WHERE clause
        sql.append("where gg.type_target = ");
        sql.append(geometryType);

        sql.append(" AND g.id = gg.id_target AND o.id = gg.id_geoobject_source;");

        long now = System.currentTimeMillis();
        sql.forceExecute();
        long now2 = System.currentTimeMillis();

        long duration = now2 - now;

        System.out.println("done: " + 
            OSMClassification.getOSMClassification().getFullClassName(Integer.toString(classID)) +
            " (" + classID + ") into " + targetFullTableName + " time: " + util.Util.getTimeString(duration));

    }
    
    void produceFunctionAndIndex(SQLStatementQueue sql, 
            String targetSchema, String tableName, int geometryType) throws SQLException {
        
        String targetFullTableName = targetSchema + "." + tableName;
        String geometryName = OHDM_DB.getGeometryName(geometryType);

        // transform geometries from wgs'84 (4326) to pseudo mercator (3857)
//        System.out.println(targetFullTableName + ": transform geometry " + geometryName + " to pseude mercator EPSG:3857");
        sql.append("UPDATE ");
        sql.append(targetFullTableName);
        sql.append(" SET ");
        sql.append(geometryName);
        sql.append(" = ST_TRANSFORM(");
        sql.append(geometryName);
        sql.append(", 3857);");

        long now = System.currentTimeMillis();

        sql.forceExecute();

        long now2 = System.currentTimeMillis();
        long duration = now2 - now;

        System.out.println("done converting to 4326->3857: time: " + util.Util.getTimeString(duration));
//        System.out.println("..done");

        // create function
        String tableFunctionName = targetSchema + ".ohdm_" + 
                tableName + "(date, character varying)";

        /*
        Note: There is no need to drop that function. It's is dropped
        as side effect of dropping associated the table with CASCADE option
        */

        String createBBOXName = targetSchema + OHDM2Rendering.BBOX_FUNCTION_TAIL;

        /*
//        System.out.println("create function " + tableFunctionName);
        sql.append("CREATE OR REPLACE FUNCTION ");
        sql.append(tableFunctionName);
        sql.append(" RETURNS SETOF ");
        sql.append(targetFullTableName);
        sql.append(" AS $$ SELECT * FROM ");
        sql.append(targetFullTableName);
        sql.append(" where $1 between valid_since AND valid_until ");
        sql.append("AND ("); 
        sql.append(geometryName);
        sql.append(" &&  "); // bounding box intersection
        sql.append(createBBOXName); // call bbox method to create bounding box geomtry of string
        sql.append("($2, 3857));");
        sql.append(" $$ LANGUAGE SQL;");
        sql.forceExecute();
//        System.out.println("done");
         */

        /*
        a call like this can now be used to retrieve valid geometries (from highway lines in that case)
        select * from ohdm_highway_lines('2017-01-01', '1444503.75,6861512.0,1532647.25,6922910.0');                

        Note: SRS 3857 is implied in that call. Geoserver developer don't
        have to care about spatial reference systems.
        */              

        // create a spatial index.. some table are expected to be huge. Like:
        // CREATE INDEX highway_lines_gist ON public.highway_lines USING GIST (line);
        sql.append(" CREATE INDEX  ");
        sql.append(tableName);
        sql.append("_gist ON ");
        sql.append(targetFullTableName);
        sql.append(" USING GIST (");
        sql.append(geometryName);
        sql.append("); ");

        now = System.currentTimeMillis();

        sql.forceExecute();

        now2 = System.currentTimeMillis();
        duration = now2 - now;

        System.out.println("done creating spatial index: time: " + util.Util.getTimeString(duration));

    }
    
    /**
     * Create a table and inserts all geometries from ohdm which
     * are of described classNames
     * 
     * @param sql
     * @param sourceSchema
     * @param targetSchema
     * @param tableName
     * @param classNames
     * @param geometryType 
     */
    void createRenderingTable(SQLStatementQueue sql, String sourceSchema, 
            String targetSchema, String tableName, List<OHDM_Class> classNames, 
            int geometryType) throws SQLException {
        
        OSMClassification osm = OSMClassification.getOSMClassification();

        boolean createAndDrop = true;
        for(OHDM_Class className : classNames) {
            int classID = osm.getOHDMClassID(className.className, 
                    className.subclassName);

            if(classID < 0) {
                this.fatal("cannot find class id for " + className.className +
                        " / " + className.subclassName);
            }

            // got it - produce or fill that table
            this.produceRenderingTable(sql, createAndDrop, 
                    sourceSchema, targetSchema, 
                    tableName, // target table name
                    classID, geometryType);
            
            // don't drop and create again
            createAndDrop = false;
        }
        
        // finally produce function and index
        this.produceFunctionAndIndex(sql, targetSchema, tableName, geometryType);
        
    }

    void createBoundaries(SQLStatementQueue sql, String sourceSchema, 
            String targetSchema) throws SQLException {
        
        List<OHDM_Class> tableClasses;
        String tableName;
        
        /**************************************************************/
        /**      highway_huge_lines: motorway + trunk + their links   */
        /**************************************************************/
        String tableNameBegin = "boundaries_admin_";
        
        for(int level = 1; level <= 12; level++) {
            tableClasses = new ArrayList<>(); // empty each loop
            tableName = tableNameBegin + level;
            String subclassname = "adminlevel_" + level;
            tableClasses.add(new OHDM_Class("ohdm_boundary", subclassname));
            
            this.createRenderingTable(sql, sourceSchema, targetSchema, 
                    tableName, tableClasses, OHDM_DB.OHDM_LINESTRING_GEOMTYPE);
        }
    }

    void createLanduseTables(SQLStatementQueue sql, String sourceSchema,
                             String targetSchema) throws SQLException {

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("produce public landuse polygons");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        List<OHDM_Class> tableClasses;
        String tableName;
        tableClasses = new ArrayList<>();
        tableName = "landuse_gardeningAndFarm"; // empty with german data Check!!
        tableClasses.add(new OHDM_Class("landuse", "allotments"));
        tableClasses.add(new OHDM_Class("landuse", "farmland"));
        tableClasses.add(new OHDM_Class("landuse", "farmyard"));
        tableClasses.add(new OHDM_Class("landuse", "greenhouse_horticulture"));
        tableClasses.add(new OHDM_Class("landuse", "orchard"));
        tableClasses.add(new OHDM_Class("landuse", "peat_cutting"));
        tableClasses.add(new OHDM_Class("landuse", "plant_nursery"));
        tableClasses.add(new OHDM_Class("landuse", "recreation_ground"));
        tableClasses.add(new OHDM_Class("landuse", "village_green"));
        tableClasses.add(new OHDM_Class("landuse", "vineyard"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "landuse_freeGreenAndWood"; //
        tableClasses.add(new OHDM_Class("landuse", "conservation"));
        tableClasses.add(new OHDM_Class("landuse", "forest"));
        tableClasses.add(new OHDM_Class("landuse", "grass"));
        tableClasses.add(new OHDM_Class("landuse", "greenfield"));
        tableClasses.add(new OHDM_Class("landuse", "meadow"));
        tableClasses.add(new OHDM_Class("landuse", "pasture"));

        tableClasses.add(new OHDM_Class("natural", "wood"));
        tableClasses.add(new OHDM_Class("natural", "tree_row"));
        tableClasses.add(new OHDM_Class("natural", "scrub"));
        tableClasses.add(new OHDM_Class("natural", "heath"));
        tableClasses.add(new OHDM_Class("natural", "moor"));
        tableClasses.add(new OHDM_Class("natural", "grassland"));
        tableClasses.add(new OHDM_Class("natural", "fell"));
        tableClasses.add(new OHDM_Class("natural", "valley"));
        tableClasses.add(new OHDM_Class("natural", "ridge"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "landuse_water";
        tableClasses.add(new OHDM_Class("landuse", "basin"));
        tableClasses.add(new OHDM_Class("landuse", "reservoir"));

        tableClasses.add(new OHDM_Class("natural", "water"));
        tableClasses.add(new OHDM_Class("natural", "wetland"));
        tableClasses.add(new OHDM_Class("natural", "bay"));
        tableClasses.add(new OHDM_Class("natural", "beach"));
        tableClasses.add(new OHDM_Class("natural", "coastline"));
        tableClasses.add(new OHDM_Class("natural", "hot_spring"));
        tableClasses.add(new OHDM_Class("natural", "geyser"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "landuse_grey";
        tableClasses.add(new OHDM_Class("natural", "bare_rock"));
        tableClasses.add(new OHDM_Class("natural", "scree"));
        tableClasses.add(new OHDM_Class("natural", "shingle"));
        tableClasses.add(new OHDM_Class("natural", "sand"));
        tableClasses.add(new OHDM_Class("natural", "mud"));
        tableClasses.add(new OHDM_Class("natural", "glacier"));
        tableClasses.add(new OHDM_Class("natural", "arete"));
        tableClasses.add(new OHDM_Class("natural", "cliff"));
        tableClasses.add(new OHDM_Class("natural", "arete"));
        tableClasses.add(new OHDM_Class("natural", "rock"));
        tableClasses.add(new OHDM_Class("natural", "stone"));
        tableClasses.add(new OHDM_Class("natural", "sinkhole"));
        tableClasses.add(new OHDM_Class("natural", "cave_entrance"));


        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);


        tableClasses = new ArrayList<>();
        tableName = "landuse_brown";
        tableClasses.add(new OHDM_Class("landuse", "brownfield"));
        tableClasses.add(new OHDM_Class("landuse", "construction"));
        tableClasses.add(new OHDM_Class("landuse", "landfill"));
        tableClasses.add(new OHDM_Class("landuse", "salt_pond"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "landuse_residentalEtc";
        tableClasses.add(new OHDM_Class("landuse", "cemetery"));
        tableClasses.add(new OHDM_Class("landuse", "garages"));
        tableClasses.add(new OHDM_Class("landuse", "residential"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "landuse_commercialEtc";
        tableClasses.add(new OHDM_Class("landuse", "commercial"));
        tableClasses.add(new OHDM_Class("landuse", "retail"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "landuse_industrial";
        tableClasses.add(new OHDM_Class("landuse", "industrial"));
        tableClasses.add(new OHDM_Class("landuse", "port"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "landuse_military";
        tableClasses.add(new OHDM_Class("landuse", "military"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "landuse_transport";
        tableClasses.add(new OHDM_Class("landuse", "railway"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);
    }

    void createPublicTransportEntrancPoints(SQLStatementQueue sql, String sourceSchema,
                                            String targetSchema) throws SQLException {
        List<OHDM_Class> tableClasses;
        String tableName;

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("produce public transport entries");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        tableClasses = new ArrayList<>();
        tableName = "subwayEntry_points";
        tableClasses.add(new OHDM_Class("railway", "subway_entrance"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "tramEntry_points";
        tableClasses.add(new OHDM_Class("railway", "tram_stop"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

    }

    void createShopPoints(SQLStatementQueue sql, String sourceSchema,
                                            String targetSchema) throws SQLException {
        List<OHDM_Class> tableClasses;
        String tableName;

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("produce public shop points");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        ////////////////////////////////////////////////////////////////
        //                      shop tables                           //
        ////////////////////////////////////////////////////////////////

        tableClasses = new ArrayList<>();
        tableName = "shop_iconAlcohol";
        tableClasses.add(new OHDM_Class("shop", "alcohol"));
        tableClasses.add(new OHDM_Class("shop", "wine"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconBakery";
        tableClasses.add(new OHDM_Class("shop", "bakery"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconBeverages";
        tableClasses.add(new OHDM_Class("shop", "beverages"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconButcher";
        tableClasses.add(new OHDM_Class("shop", "butcher"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconButcher";
        tableClasses.add(new OHDM_Class("shop", "butcher"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconCoffee";
        tableClasses.add(new OHDM_Class("shop", "coffee"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconConfectionery";
        tableClasses.add(new OHDM_Class("shop", "confectionery"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconConvenience";
        tableClasses.add(new OHDM_Class("shop", "convenience"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconDeli";
        tableClasses.add(new OHDM_Class("shop", "deli"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconApple";
        tableClasses.add(new OHDM_Class("shop", "farm"));
        tableClasses.add(new OHDM_Class("shop", "greengrocer"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconIceCream";
        tableClasses.add(new OHDM_Class("shop", "ice_cream"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconFish";
        tableClasses.add(new OHDM_Class("shop", "seafood"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconTea";
        tableClasses.add(new OHDM_Class("shop", "tea"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconShoppingMall";
        tableClasses.add(new OHDM_Class("shop", "department_store"));
        tableClasses.add(new OHDM_Class("shop", "mall"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconKiosk";
        tableClasses.add(new OHDM_Class("shop", "kiosk"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconSupermarket";
        tableClasses.add(new OHDM_Class("shop", "supermarket"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

        tableClasses = new ArrayList<>();
        tableName = "shop_iconClothes";
        tableClasses.add(new OHDM_Class("shop", "clothes"));
        tableClasses.add(new OHDM_Class("shop", "fashion"));
        tableClasses.add(new OHDM_Class("shop", "boutique"));
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);


        tableClasses = new ArrayList<>();
        tableName = "shop_iconPoint";
        tableClasses.add(new OHDM_Class("shop", "brewing_supplies"));
        tableClasses.add(new OHDM_Class("shop", "cheese"));
        tableClasses.add(new OHDM_Class("shop", "chocolate"));
        tableClasses.add(new OHDM_Class("shop", "dairy"));
        tableClasses.add(new OHDM_Class("shop", "organic"));
        tableClasses.add(new OHDM_Class("shop", "pasta"));
        tableClasses.add(new OHDM_Class("shop", "pastry"));
        tableClasses.add(new OHDM_Class("shop", "spices"));
        tableClasses.add(new OHDM_Class("shop", "general"));
        tableClasses.add(new OHDM_Class("shop", "general"));
        tableClasses.add(new OHDM_Class("shop", "baby_goods"));
        tableClasses.add(new OHDM_Class("shop", "bag"));
        tableClasses.add(new OHDM_Class("shop", "fabric"));
        tableClasses.add(new OHDM_Class("shop", "jewelry"));
        tableClasses.add(new OHDM_Class("shop", "leather"));
        tableClasses.add(new OHDM_Class("shop", "shoes"));
        tableClasses.add(new OHDM_Class("shop", "tailor"));
        tableClasses.add(new OHDM_Class("shop", "watches"));
        tableClasses.add(new OHDM_Class("shop", "charity"));
        tableClasses.add(new OHDM_Class("shop", "second_hand"));
        tableClasses.add(new OHDM_Class("shop", "variety_store"));
        tableClasses.add(new OHDM_Class("shop", "beauty"));
        tableClasses.add(new OHDM_Class("shop", "chemist"));
        tableClasses.add(new OHDM_Class("shop", "cosmetics"));
        tableClasses.add(new OHDM_Class("shop", "drugstore"));
        tableClasses.add(new OHDM_Class("shop", "erotic"));
        tableClasses.add(new OHDM_Class("shop", "hairdresser"));
        tableClasses.add(new OHDM_Class("shop", "hearing_aids"));
        tableClasses.add(new OHDM_Class("shop", "herbalist"));
        tableClasses.add(new OHDM_Class("shop", "massage"));
        tableClasses.add(new OHDM_Class("shop", "medical_supply"));
        tableClasses.add(new OHDM_Class("shop", "nutrition_supplements"));
        tableClasses.add(new OHDM_Class("shop", "optician"));
        tableClasses.add(new OHDM_Class("shop", "perfumery"));
        tableClasses.add(new OHDM_Class("shop", "tattoo"));
        tableClasses.add(new OHDM_Class("shop", "bathroom_furnishing"));
        tableClasses.add(new OHDM_Class("shop", "hearing_aids"));
        tableClasses.add(new OHDM_Class("shop", "doityourself"));
        tableClasses.add(new OHDM_Class("shop", "electrical"));
        tableClasses.add(new OHDM_Class("shop", "energy"));
        tableClasses.add(new OHDM_Class("shop", "fireplace"));
        tableClasses.add(new OHDM_Class("shop", "florist"));
        tableClasses.add(new OHDM_Class("shop", "garden_centre"));
        tableClasses.add(new OHDM_Class("shop", "garden_furniture"));
        tableClasses.add(new OHDM_Class("shop", "gas"));
        tableClasses.add(new OHDM_Class("shop", "glaziery"));
        tableClasses.add(new OHDM_Class("shop", "hardware"));
        tableClasses.add(new OHDM_Class("shop", "houseware"));
        tableClasses.add(new OHDM_Class("shop", "locksmith"));
        tableClasses.add(new OHDM_Class("shop", "paint"));
        tableClasses.add(new OHDM_Class("shop", "trade"));
        tableClasses.add(new OHDM_Class("shop", "antiques"));
        tableClasses.add(new OHDM_Class("shop", "bed"));
        tableClasses.add(new OHDM_Class("shop", "candles"));
        tableClasses.add(new OHDM_Class("shop", "carpet"));
        tableClasses.add(new OHDM_Class("shop", "curtain"));
        tableClasses.add(new OHDM_Class("shop", "furniture"));
        tableClasses.add(new OHDM_Class("shop", "interior_decoration"));
        tableClasses.add(new OHDM_Class("shop", "kitchen"));
        tableClasses.add(new OHDM_Class("shop", "lamps"));
        tableClasses.add(new OHDM_Class("shop", "tiles"));
        tableClasses.add(new OHDM_Class("shop", "window_blind"));
        tableClasses.add(new OHDM_Class("shop", "computer"));
        tableClasses.add(new OHDM_Class("shop", "electronics"));
        tableClasses.add(new OHDM_Class("shop", "hifi"));
        tableClasses.add(new OHDM_Class("shop", "mobile_phone"));
        tableClasses.add(new OHDM_Class("shop", "radiotechnics"));
        tableClasses.add(new OHDM_Class("shop", "vacuum_cleaner"));
        tableClasses.add(new OHDM_Class("shop", "bicycle"));
        tableClasses.add(new OHDM_Class("shop", "car"));
        tableClasses.add(new OHDM_Class("shop", "car_repair"));
        tableClasses.add(new OHDM_Class("shop", "car_parts"));
        tableClasses.add(new OHDM_Class("shop", "fuel"));
        tableClasses.add(new OHDM_Class("shop", "fishing"));
        tableClasses.add(new OHDM_Class("shop", "free_flying"));
        tableClasses.add(new OHDM_Class("shop", "hunting"));
        tableClasses.add(new OHDM_Class("shop", "motorcycle"));
        tableClasses.add(new OHDM_Class("shop", "outdoor"));
        tableClasses.add(new OHDM_Class("shop", "scuba_diving"));
        tableClasses.add(new OHDM_Class("shop", "sports"));
        tableClasses.add(new OHDM_Class("shop", "swimming_pool"));
        tableClasses.add(new OHDM_Class("shop", "tyres"));
        tableClasses.add(new OHDM_Class("shop", "art"));
        tableClasses.add(new OHDM_Class("shop", "collector"));
        tableClasses.add(new OHDM_Class("shop", "craft"));
        tableClasses.add(new OHDM_Class("shop", "frame"));
        tableClasses.add(new OHDM_Class("shop", "games"));
        tableClasses.add(new OHDM_Class("shop", "music"));
        tableClasses.add(new OHDM_Class("shop", "musical_instrument"));
        tableClasses.add(new OHDM_Class("shop", "photo"));
        tableClasses.add(new OHDM_Class("shop", "camera"));
        tableClasses.add(new OHDM_Class("shop", "trophy"));
        tableClasses.add(new OHDM_Class("shop", "video"));
        tableClasses.add(new OHDM_Class("shop", "video_games"));
        tableClasses.add(new OHDM_Class("shop", "anime"));
        tableClasses.add(new OHDM_Class("shop", "books"));
        tableClasses.add(new OHDM_Class("shop", "gift"));
        tableClasses.add(new OHDM_Class("shop", "lottery"));
        tableClasses.add(new OHDM_Class("shop", "newsagent"));
        tableClasses.add(new OHDM_Class("shop", "stationery"));
        tableClasses.add(new OHDM_Class("shop", "ticket"));
        tableClasses.add(new OHDM_Class("shop", "bookmaker"));
        tableClasses.add(new OHDM_Class("shop", "copyshop"));
        tableClasses.add(new OHDM_Class("shop", "dry_cleaning"));
        tableClasses.add(new OHDM_Class("shop", "e-cigarette"));
        tableClasses.add(new OHDM_Class("shop", "funeral_directors"));
        tableClasses.add(new OHDM_Class("shop", "laundry"));
        tableClasses.add(new OHDM_Class("shop", "money_lender"));
        tableClasses.add(new OHDM_Class("shop", "pawnbroker"));
        tableClasses.add(new OHDM_Class("shop", "pet"));
        tableClasses.add(new OHDM_Class("shop", "pyrotechnics"));
        tableClasses.add(new OHDM_Class("shop", "religion"));
        tableClasses.add(new OHDM_Class("shop", "tobacco"));
        tableClasses.add(new OHDM_Class("shop", "toys"));
        tableClasses.add(new OHDM_Class("shop", "travel_agency"));
        tableClasses.add(new OHDM_Class("shop", "vacant"));
        tableClasses.add(new OHDM_Class("shop", "pet"));
        tableClasses.add(new OHDM_Class("shop", "weapons"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POINT_GEOMTYPE);

    }

    void createHighwayLines(SQLStatementQueue sql, String sourceSchema,
                                            String targetSchema) throws SQLException {
        List<OHDM_Class> tableClasses;
        String tableName;

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("produce highway lines");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        ////////////////////////////////////////////////////////////////
        //                      highway tables                        //
        ////////////////////////////////////////////////////////////////

        /**************************************************************/
        /**      highway_huge_lines: motorway + trunk + their links   */
        /**************************************************************/
        tableClasses = new ArrayList<>();

        tableName = "highway_huge_lines";
        tableClasses.add(new OHDM_Class("highway", "motorway"));
        tableClasses.add(new OHDM_Class("highway", "trunk"));
        tableClasses.add(new OHDM_Class("highway", "motorway_link"));
        tableClasses.add(new OHDM_Class("highway", "trunk_link"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_LINESTRING_GEOMTYPE);

        /**************************************************************/
        /*           highway_primary_lines: primary + links           */
        /**************************************************************/
        tableClasses = new ArrayList<>();

        tableName = "highway_primary_lines";
        tableClasses.add(new OHDM_Class("highway", "primary"));
        tableClasses.add(new OHDM_Class("highway", "primary_link"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_LINESTRING_GEOMTYPE);

        /**************************************************************/
        /*           highway_secondary_lines: secondary + links           */
        /**************************************************************/
        tableClasses = new ArrayList<>();

        tableName = "highway_secondary_lines";
        tableClasses.add(new OHDM_Class("highway", "secondary"));
        tableClasses.add(new OHDM_Class("highway", "secondary_link"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_LINESTRING_GEOMTYPE);

        /**************************************************************/
        /*           highway_tertiary_lines: tertiary + links           */
        /**************************************************************/
        tableClasses = new ArrayList<>();

        tableName = "highway_tertiary_lines";
        tableClasses.add(new OHDM_Class("highway", "tertiary"));
        tableClasses.add(new OHDM_Class("highway", "tertiary_link"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_LINESTRING_GEOMTYPE);

        /**************************************************************/
        /*                        highway_path_lines                  */
        /**************************************************************/
        tableClasses = new ArrayList<>();

        tableName = "highway_path_lines";
        tableClasses.add(new OHDM_Class("highway", "track"));
        tableClasses.add(new OHDM_Class("highway", "path"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_LINESTRING_GEOMTYPE);

        /**************************************************************/
        /*                    highway_small_lines                     */
        /**************************************************************/
        tableClasses = new ArrayList<>();

        tableName = "highway_small_lines";
        tableClasses.add(new OHDM_Class("highway", "unclassified"));
        tableClasses.add(new OHDM_Class("highway", "living_street"));
        tableClasses.add(new OHDM_Class("highway", "service"));
        tableClasses.add(new OHDM_Class("highway", "footway"));

        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_LINESTRING_GEOMTYPE);

    }

    void createV1(SQLStatementQueue sql, String sourceSchema,
            String targetSchema) throws SQLException {

        // create entrance point public transport
        this.createPublicTransportEntrancPoints(sql, sourceSchema, targetSchema);

        // create landuse tables
        this.createLanduseTables(sql, sourceSchema, targetSchema);

        // create shop points tables
        this.createShopPoints(sql, sourceSchema, targetSchema);

        // create way tablees
        this.createHighwayLines(sql, sourceSchema, targetSchema);

        List<OHDM_Class> tableClasses;
        String tableName;

        ////////////////////////////////////////////////////////////////
        //                      building tables                       // 
        ////////////////////////////////////////////////////////////////
        
        // TODO add more...
        /**************************************************************/
        /*                    building_apartments                     */
        /**************************************************************/
        tableClasses = new ArrayList<>();
        
        tableName = "building_apartments";
        tableClasses.add(new OHDM_Class("building", "apartments"));
        
        this.createRenderingTable(sql, sourceSchema, targetSchema,
                tableName, tableClasses, OHDM_DB.OHDM_POLYGON_GEOMTYPE);
        
        
    }
}
