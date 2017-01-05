package inter2ohdm;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import util.Parameter;

/**
 *
 * @author thsc
 */
public abstract class Importer {
    protected final Connection sourceConnection;
    protected final Connection targetConnection;
    
    Importer(Connection sourceConnection, Connection targetConnection) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
    }
    
    Importer(String sourceParameterFile, String targetParameterFile) throws IOException, SQLException {
        Parameter sourceParameter = new Parameter(sourceParameterFile);
        Parameter targetParameter = new Parameter(targetParameterFile);
        
        this.sourceConnection = this.createConnection(sourceParameter);
        this.targetConnection = this.createConnection(targetParameter);
    }
    
    public final static Connection createConnection(Parameter parameter) throws SQLException {
        
        String user, pwd, servername, path, port;
        
        user = parameter.getUserName();
        if(user == null) return null;
        
        pwd = parameter.getPWD();
        if(pwd == null) return null;
        
        servername = parameter.getServerName();
        if(servername == null) return null;
        
        path = parameter.getdbName();
        if(path == null) return null;
        
        port = parameter.getPortNumber();
        if(port == null) return null;
        
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", pwd);
        
        return DriverManager.getConnection(
                "jdbc:postgresql://" + servername
                + ":" + port + "/" + path, connectionProperties);
        
    }
    
    protected ResultSet executeQueryOnTarget(String sql) throws SQLException {
        PreparedStatement stmt = this.targetConnection.prepareStatement(sql);
        ResultSet result = stmt.executeQuery();
        
        return result;
    }
    
    protected void executeOnTarget(String sql) throws SQLException {
        PreparedStatement stmt = this.targetConnection.prepareStatement(sql);
        stmt.execute();
    }
    
    /**
     * connects to localhost:5432 with admin/root dbname: ohdm
     * @return
     * @throws SQLException 
     */
    static protected Connection createLocalTestSourceConnection() throws SQLException, SQLException, SQLException {
        // connect to OHDM source (intermediate database)
        String sourceServerName = "localhost";
        String sourcePortNumber = "5432";
        String sourceUser = "admin";
        String sourcePWD = "root";
        String sourcePath = "ohdm";

        Properties sourceConnProps = new Properties();
        sourceConnProps.put("user", sourceUser);
        sourceConnProps.put("password", sourcePWD);
        return DriverManager.getConnection(
                "jdbc:postgresql://" + sourceServerName
                + ":" + sourcePortNumber + "/" + sourcePath, sourceConnProps);
    }
    
    /**
     * connects to localhost:5432 with admin/root dbname: ohdm_full
     * @return
     * @throws SQLException 
     */
    static protected Connection createLocalTestTargetConnection() throws SQLException {
            // connect to target OHDM DB - local
            String targetServerName = "localhost";
            String targetPortNumber = "5432";
            String targetUser = "admin";
            String targetPWD = "root";
            String targetPath = "ohdm_full";
        
            // connect to target OHDM DB - ohm
//            String targetServerName = "ohm.f4.htw-berlin.de";
//            String targetPortNumber = "5432";
//            String targetUser = "...";
//            String targetPWD = "..";
//            String targetPath = "ohdm_rendering";
        
            Properties targetConnProps = new Properties();
            targetConnProps.put("user", targetUser);
            targetConnProps.put("password", targetPWD);
            return DriverManager.getConnection(
                    "jdbc:postgresql://" + targetServerName
                    + ":" + targetPortNumber + "/" + targetPath, targetConnProps);
    }
    
    static protected String getFullTableName(String schema, String tableName) {
        return schema + "." + tableName;
    }
    
    public abstract boolean importWay(OHDMWay way) throws SQLException;

    public abstract boolean importRelation(OHDMRelation relation) throws SQLException;

    public abstract boolean importNode(OHDMNode node) throws SQLException;

    public abstract boolean importPostProcessing(OHDMElement element) throws SQLException;
}
