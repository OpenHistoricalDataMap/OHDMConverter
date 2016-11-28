package osmupdatewizard;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import static osmupdatewizard.SQLImportCommandBuilder.WAYTABLE;
import static osmupdatewizard.SQLImportCommandBuilder.NODETABLE;

/**
 * Fill OHDM rendering data base from intermediate data base.
 * It's a short cut to increase development and not meant
 * to be part of the productive system
 * @author thsc
 */
public class Intermediate2OHDMRendering {
    private final Connection connection;
    
    Intermediate2OHDMRendering(Connection connection) {
        this.connection = connection;
    }
    
    void dropHighways() {
        SQLStatementQueue sq = new SQLStatementQueue(this.connection);
        
        sq.append("DROP SEQUENCE public.highway_lines_seq;");
        sq.append("DROP TABLE public.highway_lines;");
        
        sq.flush();
        
    }
    
    void setupHighways() {
        SQLStatementQueue sq = new SQLStatementQueue(this.connection);
        
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
    
    
    public void go() {
        // extract ways
        this.extractWays();
    }
    
    
    public void doHighways() {
        this.dropHighways();
        this.setupHighways();
    }

    private void extractWays() {
        // open ways table

        
    StringBuilder sqlWay = new StringBuilder("SELECT * FROM ");
    sqlWay.append(WAYTABLE).append(" WHERE osm_id = ? ;");
    StringBuilder sqlNodes = new StringBuilder("SELECT * FROM ");
    sqlNodes.append(NODETABLE).append(" WHERE id_way = ?;");
        
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
                    new Intermediate2OHDMRendering(connection);
            
            renderDBFiller.doHighways();
  
        } catch (SQLException e) {
          System.err.println("cannot connect to database: " + e.getLocalizedMessage());
        }
    }
}
