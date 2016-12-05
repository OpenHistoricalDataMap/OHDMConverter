package exportfromintermediate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import osmupdatewizard.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public class ImportRendering extends Importer {

    public ImportRendering(Connection sourceConnection, Connection targetConnection) {
        this(sourceConnection, targetConnection, false);
    }
    
    public ImportRendering(Connection sourceConnection, Connection targetConnection, 
            boolean dropAndCreate) {
        
        super(sourceConnection, targetConnection);
        if(dropAndCreate) {
            this.init();
        }
    }
    
    private void init() {
        this.dropHighways();
        this.setupHighways();
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

    @Override
    public boolean importWay(OHDMWay way) {
        String className = way.getClassName();
        if(!className.startsWith("highway")) return false;
        
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

    @Override
    public boolean importRelation(OHDMRelation relation) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean importNode(OHDMNode node) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public static void main(String args[]) {
        // let's fill OHDM database

        try {
            // connect to OHDM source (intermediate database)
            String sourceServerName = "localhost";
            String sourcePortNumber = "5432";
            String sourceUser = "admin";
            String sourcePWD = "root";
            String sourcePath = "ohdm";
        
            Properties sourceConnProps = new Properties();
            sourceConnProps.put("user", sourceUser);
            sourceConnProps.put("password", sourcePWD);
            Connection sourceConnection = DriverManager.getConnection(
                    "jdbc:postgresql://" + sourceServerName
                    + ":" + sourcePortNumber + "/" + sourcePath, sourceConnProps);
            
            // connect to target OHDM DB
            String targetServerName = "localhost";
            String targetPortNumber = "5432";
            String targetUser = "admin";
            String targetPWD = "root";
            String targetPath = "ohdm_full";
        
            Properties targetConnProps = new Properties();
            targetConnProps.put("user", sourceUser);
            targetConnProps.put("password", sourcePWD);
            Connection targetConnection = DriverManager.getConnection(
                    "jdbc:postgresql://" + targetServerName
                    + ":" + targetPortNumber + "/" + targetPath, targetConnProps);
            
//            Transfer i = new ImportRendering(sourceConnection, targetConnection, false);
            Importer i = new ImportRendering(sourceConnection, targetConnection, true);
          
            // prepare extractor
            ExportIntermediateDB exporter = 
                    new ExportIntermediateDB(sourceConnection, i);
            
            // extract nodes
            exporter.processNodes();
            // extract ways
            exporter.processWays();
            // extract relations
            exporter.processRelations();
  
        } catch (SQLException e) {
          System.err.println("cannot connect to database: " + e.getLocalizedMessage());
        }
    }
}
