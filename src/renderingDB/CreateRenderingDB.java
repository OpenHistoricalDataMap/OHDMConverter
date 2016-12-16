package renderingDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import osm.OSMClassification;
import osmupdatewizard.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public class CreateRenderingDB {
    public static void main(String[] args) throws SQLException {
        
        String targetServerName = "localhost";
        String targetPortNumber = "5432";
        String targetUser = "admin";
        String targetPWD = "root";
        String targetPath = "ohdm_rendering";

        // connect to target OHDM DB - ohm
//            String targetServerName = "ohm.f4.htw-berlin.de";
//            String targetPortNumber = "5432";
//            String targetUser = "...";
//            String targetPWD = "...";
//            String targetPath = "ohdm_rendering";

        Properties targetConnProps = new Properties();
        targetConnProps.put("user", targetUser);
        targetConnProps.put("password", targetPWD);
        Connection connection = DriverManager.getConnection(
            "jdbc:postgresql://" + targetServerName
                    + ":" + targetPortNumber + "/" + targetPath, targetConnProps);
/*
select l.line, c.subclass, o.name, gg.valid_since, gg.valid_until, gg.valid_since_offset, gg.valid_until_offset into ohdm_rendering.test from
(SELECT id, classification_id, name from ohdm.geoobject where classification_id = 140) as o,
(SELECT id_target, type_target, id_geoobject_source, valid_since, valid_until, valid_since_offset, valid_until_offset FROM ohdm.geoobject_geometry) as gg,
(SELECT id, line FROM ohdm.lines) as l,
(SELECT subclass, id FROM ohdm.classification) as c
where gg.type_target = 2 AND l.id = gg.id_target AND o.id = gg.id_geoobject_source AND o.classification_id = c.id;
         */
            
        OSMClassification classification = OSMClassification.getOSMClassification();
        for(String featureClassString : classification.osmFeatureClasses.keySet()) {
            for(int targetType = 1; targetType < 3; targetType++) {
                // produce tableName
                String tableName = /* targetPath + "." + */ featureClassString + "_";
                switch(targetType) {
                    case 1: tableName += "points"; break;
                    case 2: tableName += "lines"; break;
                    case 3: tableName += "poylgons"; break;
                }
                
                Iterator<String> classIDs = classification.getClassIDs(featureClassString);
                while(classIDs.hasNext()) {
                    SQLStatementQueue sql = new SQLStatementQueue(connection);
                    sql.append("drop table ");
//                    sql.append(targetPath);
//                    sql.append(".");
                    sql.append(tableName);
                    sql.append(";");
                    sql.forceExecute();
                
                    sql.append("select l.line, c.subclass, o.name, gg.valid_since, ");
                    sql.append("gg.valid_until, gg.valid_since_offset, ");
                    sql.append("gg.valid_until_offset into ");
//                    sql.append(targetPath);
//                    sql.append(".");
                    sql.append(tableName);
                    sql.append(" from");
                    sql.append(" (SELECT id, classification_id, name from ohdm.geoobject where classification_id = ");
                
                    String classID = classIDs.next();
                    System.out.println(classID);
                    sql.append(classID);
                    
                    sql.append(") as o,(SELECT id_target, type_target, id_geoobject_source, valid_since, ");
                    sql.append("valid_until, valid_since_offset, valid_until_offset FROM ohdm.geoobject_geometry) as gg,");
                    sql.append("(SELECT id, line FROM ohdm.lines) as l,");
                    sql.append("(SELECT subclass, id FROM ohdm.classification) as c ");
                    sql.append("where gg.type_target = ");
                    sql.append(targetType);
                    
                    sql.append(" AND l.id = gg.id_target AND o.id = gg.id_geoobject_source AND o.classification_id = c.id;");
                    sql.forceExecute();
                }
            }
        }
    }
}
