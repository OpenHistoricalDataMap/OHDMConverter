package osmupdatewizard;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import static osmupdatewizard.SQLImportCommandBuilder.WAYTABLE;
import static osmupdatewizard.SQLImportCommandBuilder.NODETABLE;
import static osmupdatewizard.SQLImportCommandBuilder.WAYMEMBER;

/**
 * Fill OHDM rendering data base from intermediate data base.
 * It's a short cut to increase development and not meant
 * to be part of the productive system
 * @author thsc
 */
public class Intermediate2OHDMRendering {
    private final Connection sourceConnection;
    private final Connection targetConnection;
    
    Intermediate2OHDMRendering(Connection sourceConnection, Connection targetConnection) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
    }
    
    void dropHighways() {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
        sq.append("DROP SEQUENCE public.highway_lines_seq;");
        sq.append("DROP TABLE public.highway_lines;");
        
        sq.flush();
        
    }
    
    void setupHighways() {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
        sq.append("CREATE SEQUENCE public.highway_lines_seq ");
        sq.append("INCREMENT 1 ");
        sq.append("MINVALUE 1 ");
        sq.append("MAXVALUE 9223372036854775807 ");
        sq.append("START 1 ");
        sq.append("CACHE 1;");
        sq.flush();
        
        sq.append("CREATE TABLE public.highway_lines (");
        sq.append("id bigint NOT NULL DEFAULT nextval('public.higway_lines_seq'::regclass),");
        sq.append("line geometry,");
        sq.append("subclassname character varying,");
        sq.append("name character varying,");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("PRIMARY KEY (id));");
        
        sq.flush();
    }
    
    
    public void doHighways() {
        /*
        this.dropHighways();
        this.setupHighways();
        */
        
        this.doConvert2RenderingDB();
        
    }

    private void doConvert2RenderingDB() {
        // open ways table and iterate ways
        
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(WAYTABLE).append(";");
        
        try {
            PreparedStatement stmt = this.sourceConnection.prepareStatement(sql.toString());
            ResultSet qResult = stmt.executeQuery();
            qResult.next();
            
            OHDMWay way = this.createOHDMWay(qResult);
            
            // find all associated nodes and add to that way
            /* SQL Query is like this
                select * from nodes_table where osm_id IN 
                (SELECT node_id FROM waynodes_table where way_id = ID_of_way);            
            */        
            
            sql = new StringBuilder("select * from ");
            sql.append(NODETABLE);
            sql.append(" where osm_id IN (SELECT node_id FROM ");            
            sql.append(WAYMEMBER);
            sql.append(" where way_id = ");            
            sql.append(way.getOSMID());
            sql.append(");");  
            
            stmt = this.sourceConnection.prepareStatement(sql.toString());
            qResult = stmt.executeQuery();
            
            while(qResult.next()) {
                OHDMNode node = this.createOHDMNode(qResult);
                way.addNode(node);
            }
            
            String wayGeometryWKT = way.getWKTGeometry();
            int i = 42; // just to have a break for the debugger.
            
        } catch (SQLException ex) {
            System.err.println(ex.getLocalizedMessage());
        }
    }
    
    private OHDMWay createOHDMWay(ResultSet qResult) throws SQLException {
        // get all data to create an ohdm way object
        BigDecimal osmIDBig = qResult.getBigDecimal("osm_id");
        BigDecimal classCodeBig = qResult.getBigDecimal("classcode");
        String sTags = qResult.getString("serializedtags");
        BigDecimal ohdmIDBig = qResult.getBigDecimal("ohdm_id");
        BigDecimal ohdmObjectIDBig = qResult.getBigDecimal("ohdm_object");
        String nodeIDs = qResult.getString("node_ids");
        boolean valid = qResult.getBoolean("valid");

        OHDMWay way = new OHDMWay(osmIDBig, classCodeBig, sTags, nodeIDs, ohdmIDBig, ohdmObjectIDBig, valid);

        return way;
    }
    
    private OHDMNode createOHDMNode(ResultSet qResult) throws SQLException {
        BigDecimal osmIDBig = qResult.getBigDecimal("osm_id");
        BigDecimal classCodeBig = qResult.getBigDecimal("classcode");
        String sTags = qResult.getString("serializedtags");
        String longitude = qResult.getString("longitude");
        String latitude = qResult.getString("latitude");
        BigDecimal ohdmIDBig = qResult.getBigDecimal("ohdm_id");
        BigDecimal ohdmObjectIDBig = qResult.getBigDecimal("ohdm_object");
        boolean valid = qResult.getBoolean("valid");

        OHDMNode node = new OHDMNode(osmIDBig, classCodeBig, sTags, longitude, latitude, ohdmIDBig, ohdmObjectIDBig, valid);

        return node;
    }
    
    
    
    public static void main(String args[]) {
    
        // let's fill OHDM database

        // connect to OHDM rendering database
        String serverName = "localhost";
        String portNumber = "5432";
        String user = "admin";
        String pwd = "root";
        String path = "ohdm";

        try {
            Properties connProps = new Properties();
            connProps.put("user", user);
            connProps.put("password", pwd);
            Connection connection = DriverManager.getConnection(
                    "jdbc:postgresql://" + serverName
                    + ":" + portNumber + "/" + path, connProps);
          
            Intermediate2OHDMRendering renderDBFiller = 
                    new Intermediate2OHDMRendering(connection, null);
            
            renderDBFiller.doHighways();
  
        } catch (SQLException e) {
          System.err.println("cannot connect to database: " + e.getLocalizedMessage());
        }
    }
}
