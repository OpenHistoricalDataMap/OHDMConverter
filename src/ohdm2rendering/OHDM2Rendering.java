package ohdm2rendering;

import inter2ohdm.Importer;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
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
/*
select l.line, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.valid_since_offset, gg.valid_until_offset into ohdm_rendering.test from
(SELECT id, classification_id, name from ohdm.geoobject where classification_id = 140) as o,
(SELECT id_target, type_target, id_geoobject_source, valid_since, valid_until, valid_since_offset, valid_until_offset FROM ohdm.geoobject_geometry) as gg,
(SELECT id, line FROM ohdm.lines) as l,
(SELECT subclassname, id FROM ohdm.classification) as c
where gg.type_target = 2 AND l.id = gg.id_target AND o.id = gg.id_geoobject_source AND o.classification_id = c.id;
         */
        SQLStatementQueue sql = new SQLStatementQueue(connection);
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
        sql.append(createBBOXFunction);
        sql.append(";");
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
        
        OSMClassification classification = OSMClassification.getOSMClassification();
        for(String featureClassString : classification.osmFeatureClasses.keySet()) {

            /* there will be a table for each class and type:
             * produce tableName [classname]_[geometryType]
             */
            for(int targetType = 1; targetType <= 3; targetType++) {

                String tableName = featureClassString + "_";
                switch(targetType) {
                    case 1: tableName += "points"; break;
                    case 2: tableName += "lines"; break;
                    case 3: tableName += "polygons"; break;
                }
                
                String fullTableName = targetSchema + "." + tableName;
                        
                /* iterate all subclasses of this class. Create 
                rendering table in the first loop and fill it
                with data from other subclasses in following loops
                */
                boolean first = true;
                
                // iterate subclasses
                for(String subClassName : classification.osmFeatureClasses.get(featureClassString)) {
                    int classID = classification.getOHDMClassID(featureClassString, subClassName);

                    // what's the coloumn name of our geometry
                    switch(targetType) {
                        case 1: geometryName = "point"; break; // select point in geom table
                        case 2: geometryName = "line"; break; // select line in geom table
                        case 3: geometryName = "polygon"; break; // select polygon in geom table
//                        case 1: sql.append("point"); break; // select point in geom table
//                        case 2: sql.append("line"); break; // select line in geom table
//                        case 3: sql.append("polygon"); break; // select polygon in geom table
                    }
                    
                    // drop table in first loop
                    if(first) {
                        sql.append("drop table ");
                        sql.append(fullTableName);
                        sql.append(" CASCADE;");
                        try {
                            sql.forceExecute();
                        }
                        catch(SQLException e) {
                            // ignore
                        }
                    }
                    
                    // add data in following loops
                    if(!first) {
                        sql.append("INSERT INTO ");
                        sql.append(fullTableName);
                        sql.append("( ");
                        
                        sql.append(geometryName);
                        
                        sql.append(", object_id, geom_id, subclassname, name, valid_since, valid_until, valid_since_offset, valid_until_offset) ");
                    }

                    sql.append("select g.");
                    sql.append(geometryName);
                    
//                    switch(targetType) {
//                    case 1: sql.append("point"); break; // select point in geom table
//                    case 2: sql.append("line"); break; // select line in geom table
//                    case 3: sql.append("polygon"); break; // select polygon in geom table
//                    }
                    
                    sql.append(", o.id as object_id, g.id as geom_id, c.subclassname, o.name, gg.valid_since, ");
                    sql.append("gg.valid_until, gg.valid_since_offset, ");
                    sql.append("gg.valid_until_offset ");

                    // create and fill in first loop
                    if(first) {
                        sql.append("into ");
                        sql.append(fullTableName);
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
//                    switch(targetType) {
//                    case 1: sql.append("point"); break; // select point in geom table
//                    case 2: sql.append("line"); break; // select line in geom table
//                    case 3: sql.append("polygon"); break; // select polygon in geom table
//                    }
                    sql.append(" FROM ");
                    sql.append(sourceSchema);
                    sql.append(".");
                    switch(targetType) {
                    case 1: sql.append("points"); break; // select point in geom table
                    case 2: sql.append("lines"); break; // select line in geom table
                    case 3: sql.append("polygons"); break; // select polygon in geom table
                    }
                    sql.append(") as g,");
                    
                    // classification c
                    sql.append("(SELECT subclassname FROM ");
                    sql.append(sourceSchema);
                    sql.append(".classification where id = ");
                    sql.append(classID);
                    sql.append(") as c ");
                    
                    // WHERE clause
                    sql.append("where gg.type_target = ");
                    sql.append(targetType);

                    sql.append(" AND g.id = gg.id_target AND o.id = gg.id_geoobject_source;");
                    if(fullTableName.equalsIgnoreCase("ohdm_rendering.highway_lines")) {
                        int debuggingStop = 42;
                    }
                    
                    sql.forceExecute();
                    
                    System.out.println("done: " + classID + " " + fullTableName + "| classes: " + featureClassString + "/" + subClassName);
                    
                    first = false;
                }
                
                // transform geometries from wgs'84 (4326) to pseudo mercator (3857)
                sql.append("UPDATE ");
                sql.append(fullTableName);
                sql.append(" SET ");
                sql.append(geometryName);
                sql.append(" = ST_TRANSFORM(");
                sql.append(geometryName);
                sql.append(", 3857);");
                sql.forceExecute();
                
                // create function for convient access in geoserver
                /*
    CREATE OR REPLACE FUNCTION public.ZZZ_test(date, character varying) RETURNS SETOF public.highway_lines AS $$
        SELECT * FROM public.highway_lines where $1 between valid_since AND valid_until 
            AND ST_INTERSECTS(line, public.ohdm_createBBOXGeometry($2, 3857));
    $$ LANGUAGE SQL;
                */
                
                String tableFunctionName = targetSchema + ".ohdm_" + 
                        tableName + "(date, character varying)";
                
                sql.append("DROP FUNCTION ");
                sql.append(tableFunctionName);
                sql.append(";");
                try {
                    sql.forceExecute();
                }
                catch(SQLException e) {
                    System.err.println("exception ignored: " + e);
                }
                
                sql.append("CREATE OR REPLACE FUNCTION ");
                sql.append(tableFunctionName);
                sql.append(" RETURNS SETOF ");
                sql.append(fullTableName);
                sql.append(" AS $$ SELECT * FROM ");
                sql.append(fullTableName);
                sql.append(" where $1 between valid_since AND valid_until ");
                sql.append("AND ST_INTERSECTS("); 
                sql.append(geometryName);
                sql.append(",  ");
                sql.append(createBBOXName);
                sql.append("($2, 3857));");
                sql.append(" $$ LANGUAGE SQL;");
                
                sql.forceExecute();
            }
        }
        sql.forceExecute();
    }
}
