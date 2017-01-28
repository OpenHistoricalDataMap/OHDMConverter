package ohdm2rendering;

import inter2ohdm.Importer;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import osm.OSMClassification;
import util.DB;
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
        
//        String targetServerName = "localhost";
//        String targetPortNumber = "5432";
//        String targetUser = "admin";
//        String targetPWD = "root";
//        String targetPath = "ohdm_full";
//
        // connect to target OHDM DB - ohm
//            String targetServerName = "ohm.f4.htw-berlin.de";
//            String targetPortNumber = "5432";
//            String targetUser = "...";
//            String targetPWD = "...";
//            String targetPath = "ohdm_rendering";

//        Properties targetConnProps = new Properties();
//        targetConnProps.put("user", targetUser);
//        targetConnProps.put("password", targetPWD);
//        Connection connection = DriverManager.getConnection(
//            "jdbc:postgresql://" + targetServerName
//                    + ":" + targetPortNumber + "/" + targetPath, targetConnProps);
//        
//        String sourceSchema = "ohdm";
//        String targetSchema = "ohdm_rendering";
//        String targetSchema = "osw";
            
        String sourceSchema = sourceParameter.getSchema();
/*
select l.line, c.subclassname, o.name, gg.valid_since, gg.valid_until, gg.valid_since_offset, gg.valid_until_offset into ohdm_rendering.test from
(SELECT id, classification_id, name from ohdm.geoobject where classification_id = 140) as o,
(SELECT id_target, type_target, id_geoobject_source, valid_since, valid_until, valid_since_offset, valid_until_offset FROM ohdm.geoobject_geometry) as gg,
(SELECT id, line FROM ohdm.lines) as l,
(SELECT subclassname, id FROM ohdm.classification) as c
where gg.type_target = 2 AND l.id = gg.id_target AND o.id = gg.id_geoobject_source AND o.classification_id = c.id;
         */
            
        OSMClassification classification = OSMClassification.getOSMClassification();
        for(String featureClassString : classification.osmFeatureClasses.keySet()) {

            /* there will be a table for each class and type:
             * produce tableName [classname]_[geometryType]
             */
            for(int targetType = 1; targetType <= 3; targetType++) {
                
                String tableName = targetSchema + "." + featureClassString + "_";
                switch(targetType) {
                    case 1: tableName += "points"; break;
                    case 2: tableName += "lines"; break;
                    case 3: tableName += "polygons"; break;
                }
            
                /* iterate all subclasses of this class. Create 
                rendering table in the first loop and fill it
                with data from other subclasses in following loops
                */
                boolean first = true;
                
                // iterate subclasses
                for(String subClassName : classification.osmFeatureClasses.get(featureClassString)) {
                    int classID = classification.getOHDMClassID(featureClassString, subClassName);

                    SQLStatementQueue sql = new SQLStatementQueue(connection);
                    
                    // drop table in first loop
                    if(first) {
                        sql.append("drop table ");
                        sql.append(tableName);
                        sql.append(";");
                        try {
                            sql.forceExecute();
                        }
                        catch(SQLException e) {
                            System.err.println("ignore that sql exception:\n" + e.getMessage() + "\n" + sql.toString());
                        }
                    }
                    
                    // add data in following loops
                    if(!first) {
                        sql.append("INSERT INTO ");
                        sql.append(tableName);
                        sql.append("( ");
                        switch(targetType) {
                        case 1: sql.append("point"); break; // select point in geom table
                        case 2: sql.append("line"); break; // select line in geom table
                        case 3: sql.append("polygon"); break; // select polygon in geom table
                        }
                        sql.append(", subclassname, name, valid_since, valid_until, valid_since_offset, valid_until_offset) ");
                    }

                    sql.append("select g.");
                    switch(targetType) {
                    case 1: sql.append("point"); break; // select point in geom table
                    case 2: sql.append("line"); break; // select line in geom table
                    case 3: sql.append("polygon"); break; // select polygon in geom table
                    }
                    sql.append(", c.subclassname, o.name, gg.valid_since, ");
                    sql.append("gg.valid_until, gg.valid_since_offset, ");
                    sql.append("gg.valid_until_offset ");

                    // create and fill in first loop
                    if(first) {
                        sql.append("into ");
                        sql.append(tableName);
                    }

                    sql.append(" from");
                    
                    // geoobject
                    sql.append(" (SELECT id, name from ");
                    sql.append(sourceSchema);
                    sql.append(".geoobject) as o, ");
                    
                    // geoobject_geometry
                    sql.append("(SELECT id_target, classification_id, type_target, id_geoobject_source, valid_since, ");
                    sql.append("valid_until, valid_since_offset, valid_until_offset FROM ");
                    sql.append(sourceSchema);
                    sql.append(".geoobject_geometry where classification_id = ");
                    sql.append(classID);

                    sql.append(") as gg, ");
                    
                    // geometry
                    sql.append("(SELECT id, ");
                    switch(targetType) {
                    case 1: sql.append("point"); break; // select point in geom table
                    case 2: sql.append("line"); break; // select line in geom table
                    case 3: sql.append("polygon"); break; // select polygon in geom table
                    }
                    sql.append(" FROM ");
                    sql.append(sourceSchema);
                    sql.append(".");
                    switch(targetType) {
                    case 1: sql.append("points"); break; // select point in geom table
                    case 2: sql.append("lines"); break; // select line in geom table
                    case 3: sql.append("polygons"); break; // select polygon in geom table
                    }
                    sql.append(") as g,");
                    
                    // classification
                    sql.append("(SELECT subclassname FROM ");
                    sql.append(sourceSchema);
                    sql.append(".classification where id = ");
                    sql.append(classID);
                    sql.append(") as c ");
                    
                    // WHERE clause
                    sql.append("where gg.type_target = ");
                    sql.append(targetType);

                    sql.append(" AND g.id = gg.id_target AND o.id = gg.id_geoobject_source;");
                    if(tableName.equalsIgnoreCase("ohdm_rendering.highway_lines")) {
                        int debuggingStop = 42;
                    }
                    sql.forceExecute();
                    
                    System.out.println("done: " + classID + " " + tableName + "| classes: " + featureClassString + "/" + subClassName);
                    
                    first = false;
                }
            }
        }
    }
}
