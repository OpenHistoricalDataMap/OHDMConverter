package osm2inter;

import util.InterDB;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import util.DB;
import util.Parameter;
import util.SQLStatementQueue;

/**
 * Inter DB already exists
 * @author thsc
 */
public class SQL_OSM2Inter_Updater {
    private final Parameter interDBSetting;
    private final Parameter changesDBSetting;
    
    SQL_OSM2Inter_Updater(Parameter interDBSetting, Parameter changesDBSetting) throws SQLException {
        this.interDBSetting = interDBSetting;
        this.changesDBSetting = changesDBSetting;
        
        // setup changes db
        Connection conn = DB.createConnection(changesDBSetting);
        SQLStatementQueue sql = DB.createSQLStatementQueue(conn, changesDBSetting);
        InterDB.createTables(sql, changesDBSetting.getSchema());
    }
    
    public static void main(String args[]) throws IOException, SQLException {
        SQL_OSM2Inter_Updater updater = new SQL_OSM2Inter_Updater(null, new Parameter("db_changes.txt"));
        
        
    }
}
