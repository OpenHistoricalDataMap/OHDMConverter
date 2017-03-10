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
    
    public static final String GENERIC = "generic";
    public static final String V1 = "v1";
    
    
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
            case GENERIC:
                System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                System.out.println("produce generic rendering tables");
                System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                renderer.createGeneric(sql, sourceSchema, targetSchema);
                break;
            case V1:
                System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                System.out.println("produce rendering tables version 1");
                System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                renderer.createV1(sql, sourceSchema, targetSchema);
                break;
            default:
                renderer.fatal("unknown rendering output (fatal): " + renderoutput);
        }
    }
    
    void fatal(String message) {
        System.err.println(message);
        System.exit(1);
    }
    
    void setupRenderingDB(SQLStatementQueue sql, String targetSchema) throws SQLException {
        String geometryName = null;
        
        /*
        create function set creates bounding box based on bbox parameter 
        from wms request:
        CREATE OR REPLACE FUNCTION public.ohdm_createBBOXGeometry(boxstring character varying, srs integer)
        RETURNS geometry AS $$DECLARE box geometry; BEGIN 
        SELECT st_asewkt(ST_MakeEnvelope (
        string_to_array[1]::double precision, 
        string_to_array[2]::double precision, 
        string_to_array[3]::double precision,  
        string_to_array[4]::double precision, 
        srs)) FROM (SELECT string_to_array(boxstring,',')) as a
        INTO box;
        RETURN box;
        END;
        $$ LANGUAGE plpgsql;
        */            
        
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

            sql.append(", object_id, geom_id, subclassname, name, valid_since, valid_until, valid_since_offset, valid_until_offset) ");
        }

        sql.append("select g.");
        sql.append(geometryName);

        sql.append(", o.id as object_id, g.id as geom_id, c.subclassname, o.name, gg.valid_since, ");
        sql.append("gg.valid_until, gg.valid_since_offset, ");
        sql.append("gg.valid_until_offset ");

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
        sql.append("valid_until, valid_since_offset, valid_until_offset FROM ");
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
        sql.append("(SELECT subclassname FROM ");
        sql.append(sourceSchema);
        sql.append(".classification where id = ");
        sql.append(classID);
        sql.append(") as c ");

        // WHERE clause
        sql.append("where gg.type_target = ");
        sql.append(geometryType);

        sql.append(" AND g.id = gg.id_target AND o.id = gg.id_geoobject_source;");

        sql.forceExecute();

        System.out.println("done: " + 
            OSMClassification.getOSMClassification().getFullClassName(Integer.toString(classID)) +
            " (" + classID + ") into " + targetFullTableName);

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
        sql.forceExecute();
//        System.out.println("..done");

        // create function
        String tableFunctionName = targetSchema + ".ohdm_" + 
                tableName + "(date, character varying)";

        /*
        Note: There is no need to drop that function. It's is dropped
        as side effect of dropping associated the table with CASCADE option
        */

        String createBBOXName = targetSchema + OHDM2Rendering.BBOX_FUNCTION_TAIL;

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

//        System.out.println(sql.getCurrentStatement());
        sql.forceExecute();
//        System.out.println("done");

        sql.forceExecute();
        
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
    
    void createV1(SQLStatementQueue sql, String sourceSchema, 
            String targetSchema) throws SQLException {
        
        List<OHDM_Class> tableClasses;
        String tableName;
        
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
