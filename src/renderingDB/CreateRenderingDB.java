package renderingDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

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
    }
    
    
}
