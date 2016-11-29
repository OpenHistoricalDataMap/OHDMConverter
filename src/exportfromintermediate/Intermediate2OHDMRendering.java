package exportfromintermediate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import osmupdatewizard.OSMClassification;
import osmupdatewizard.SQLStatementQueue;
import static osmupdatewizard.SQLImportCommandBuilder.WAYTABLE;
import static osmupdatewizard.SQLImportCommandBuilder.NODETABLE;
import static osmupdatewizard.SQLImportCommandBuilder.WAYMEMBER;

/**
 * Fill OHDM rendering data base from intermediate data base.
 * It's a short cut to increase development and not meant
 * to be part of the productive system
 * @author thsc
 */
public class Intermediate2OHDMRendering extends Export2OHDM {
    
    Intermediate2OHDMRendering(Connection sourceConnection, Connection targetConnection) {
        super(sourceConnection, targetConnection, null);
    }
    
    void dropHighways() {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
        sq.append("DROP SEQUENCE public.highway_lines_seq CASCADE;");
        sq.append("DROP TABLE public.highway_lines CASCADE;");
        
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
        sq.append("id bigint NOT NULL DEFAULT nextval('public.highway_lines_seq'::regclass),");
//        sq.append("line geometry,");
        sq.append("line character varying,");
        sq.append("subclassname character varying,");
        sq.append("name character varying,");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("PRIMARY KEY (id));");
        
        sq.flush();
    }
    
    
    public void doHighways() {
        this.dropHighways();
        this.setupHighways();
        
        this.doConvert2RenderingDB();
        
    }

    private void doConvert2RenderingDB() {
        // open ways table and iterate ways
        
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(WAYTABLE).append(";");
        
        int waynumber = 0;
        int insertedHighways = 0;
        try {
            PreparedStatement stmt = this.sourceConnection.prepareStatement(sql.toString());
            ResultSet qResultWay = stmt.executeQuery();
            
            while(qResultWay.next()) {
                waynumber++;
                OHDMWay way = this.createOHDMWay(qResultWay);

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
                ResultSet qResultNode = stmt.executeQuery();

                while(qResultNode.next()) {
                    OHDMNode node = this.createOHDMNode(qResultNode);
                    way.addNode(node);
                }

                int ohdmClassID = OSMClassification.getOSMClassification().getOHDMClassID(way);
                if(this.way2RenderingDB(way, "highway")) {
                    insertedHighways++;
                } 
            }
        } catch (SQLException ex) {
            System.err.println(ex.getLocalizedMessage());
        }
        
        System.out.println("Checked ways / inserted ways: " + waynumber + " / " + insertedHighways);
    }
    
    private boolean way2RenderingDB(OHDMWay way, String featureClass) {
        String className = way.getClassName();
        if(!className.equalsIgnoreCase(featureClass)) return false;
        
        // right class
        String wayGeometryWKT = way.getWKTGeometry();

        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
            
            /* insert like this:
INSERT INTO highway_lines(
            line, subclassname, name, valid_since, valid_until)
    VALUES ('WKT', 2, 'name', '1970-01-01', '2016-01-01');            
                    */
            
        sq.append("INSERT INTO highway_lines(\n");
        sq.append("line, subclassname, name, valid_since, valid_until)");
        sq.append(" VALUES (");
        
        sq.append("'");
        sq.append(way.getWKTGeometry()); // geometry
        sq.append("', '");
        
        sq.append(way.getSubClassName()); // feature sub class

        sq.append("', '");
        
        sq.append(way.getName()); // feature sub class

        sq.append("', '");
        sq.append(way.validSince());

        sq.append("', '");
        sq.append(way.validUntil());
        
        sq.append("');");
        
        sq.flush();
        
        return true;
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
                    new Intermediate2OHDMRendering(connection, connection);
            
            renderDBFiller.doHighways();
  
        } catch (SQLException e) {
          System.err.println("cannot connect to database: " + e.getLocalizedMessage());
        }
    }
}
