package exportfromintermediate;

import java.sql.Connection;
import java.sql.SQLException;
import osmupdatewizard.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public class ImportRendering extends Importer {
    
    public final String HIGWAYTABLE_NAME = "public.highway_lines_osw";

    public ImportRendering(Connection sourceConnection, Connection targetConnection) {
        this(sourceConnection, targetConnection, false);
    }
    
    public ImportRendering(Connection sourceConnection, Connection targetConnection, 
            boolean dropAndCreate) {
        
        super(sourceConnection, targetConnection);
        if(dropAndCreate) {
            this.init();
            System.out.println("tables dropped and re-created");
        }
    }
    
    private void init() {
        this.dropHighways();
        this.setupHighways();
    }
    
    void dropHighways() {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
        sq.append("DROP SEQUENCE ");
        sq.append(this.HIGWAYTABLE_NAME);
        sq.append("_seq CASCADE;");
        
        sq.append("DROP TABLE ");
        sq.append(this.HIGWAYTABLE_NAME);
        sq.append(" CASCADE;");
        
        sq.flush();
    }
    
    void setupHighways() {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
        sq.append("CREATE SEQUENCE ");
        sq.append(this.HIGWAYTABLE_NAME);
        sq.append("_seq ");
        sq.append("INCREMENT 1 ");
        sq.append("MINVALUE 1 ");
        sq.append("MAXVALUE 9223372036854775807 ");
        sq.append("START 1 ");
        sq.append("CACHE 1;");
        sq.flush();
        
        sq.append("CREATE TABLE ");
        sq.append(this.HIGWAYTABLE_NAME);
        sq.append(" (");
        sq.append("id bigint NOT NULL DEFAULT nextval('");
        sq.append(this.HIGWAYTABLE_NAME);
        sq.append("_seq'::regclass),");
        sq.append("line geometry,");
        sq.append("subclassname character varying,");
        sq.append("name character varying,");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("PRIMARY KEY (id));");
        
        sq.flush();
    }

    private int numberWays = 0;
    
    @Override
    public boolean importWay(OHDMWay way) {
        if(way.isPolygone) {
            // dont' import a circle in this app
            return false;
        }
        
        String className = way.getClassName();
        if(!className.startsWith("highway")) return false;
        
        // right class and real linestring - import that thing
        String wayGeometryWKT = way.getWKTGeometry();
        
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
            
            /* insert like this:
INSERT INTO this.HIGWAYTABLE_NAME(
            line, subclassname, name, valid_since, valid_until)
    VALUES ('WKT', 2, 'name', '1970-01-01', '2016-01-01');            
                    */
            
        sq.append("INSERT INTO ");
        sq.append(this.HIGWAYTABLE_NAME);
        sq.append("(\n");
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
        
        // count
        this.numberWays++;
        if(numberWays % 100 == 0) {
            System.out.print("*");
        }
        
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
    
    void printStatistics() {
        System.out.println(this.numberWays + " ways inserted");
    }
    
    public static void main(String args[]) {
        // let's fill OHDM database

        try {
            Connection sourceConnection = Importer.createLocalTestSourceConnection();
            Connection targetConnection = Importer.createLocalTestTargetConnection();
            
//            Transfer i = new ImportRendering(sourceConnection, targetConnection, false);
            ImportRendering i = new ImportRendering(sourceConnection, targetConnection, true);
          
            // prepare extractor
            ExportIntermediateDB exporter = 
                    new ExportIntermediateDB(sourceConnection, i);
            
            // extract nodes
//            exporter.processNodes();
            
            // extract ways
            exporter.processWays();
            
            // extract relations
//            exporter.processRelations();
            
            System.out.println(exporter.getStatistics());
  
        } catch (SQLException e) {
          System.err.println("cannot connect to database: " + e.getLocalizedMessage());
        }
    }
}
