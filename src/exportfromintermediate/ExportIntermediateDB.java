package exportfromintermediate;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import static osmupdatewizard.SQLImportCommandBuilder.RELATIONTABLE;
import static osmupdatewizard.SQLImportCommandBuilder.WAYMEMBER;
import static osmupdatewizard.SQLImportCommandBuilder.WAYTABLE;
import static osmupdatewizard.SQLImportCommandBuilder.NODETABLE;

/**
 *
 * @author thsc
 */
public class ExportIntermediateDB {
    private final Importer importer;
    private final Connection sourceConnection;
    
    private int numberNodes = 0;
    private int numberWays = 0;
    private int numberRelations = 0;
    
    ExportIntermediateDB(Connection sourceConnection, Importer importer) {
        this.sourceConnection = sourceConnection;
        this.importer = importer;
    }
    
    void processNodes() {
        // go through node table and do what has to be done.
        
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(NODETABLE).append(";");
        
        int number = 0;
        try {
            PreparedStatement stmt = this.sourceConnection.prepareStatement(sql.toString());
            ResultSet qResultNode = stmt.executeQuery();
            
            while(qResultNode.next()) {
                number++;
                OHDMNode node = this.createOHDMNode(qResultNode);
                
                // now process that stuff
                if(this.importer.importNode(node)) {
                    this.numberNodes++;
                }
                
            }
        } catch (SQLException ex) {
            System.err.println(ex.getLocalizedMessage());
        }
        
        System.out.println("Checked nodes: " + number);
    }
    
    void processWays() {
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
                
                // process that stuff
                if(this.importer.importWay(way)) {
                    this.numberWays++;
                }
                
            }
        } catch (SQLException ex) {
            System.err.println(ex.getLocalizedMessage());
        }
        
        System.out.println("Checked ways / inserted ways: " + waynumber + " / " + insertedHighways);
    }
    
    void processRelations() {
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(RELATIONTABLE).append(";");
        
        int number = 0;
        try {
            PreparedStatement stmt = this.sourceConnection.prepareStatement(sql.toString());
            ResultSet qResultRelations = stmt.executeQuery();
            
            while(qResultRelations.next()) {
                number++;
                OHDMRelation relation = this.createOHDMRelation(qResultRelations);

                // find all associated nodes and add to that relation
                
                // TODO: that a copy from way...
                sql = new StringBuilder("select * from ");
                sql.append(NODETABLE);
                sql.append(" where osm_id IN (SELECT node_id FROM ");            
                sql.append(WAYMEMBER);
                sql.append(" where way_id = ");            
                sql.append(relation.getOSMID());
                sql.append(");");  

                stmt = this.sourceConnection.prepareStatement(sql.toString());
                ResultSet qResultNode = stmt.executeQuery();

                while(qResultNode.next()) {
                    OHDMNode node = this.createOHDMNode(qResultNode);
                    relation.addNode(node);
                }
                
                // find all associated ways and add to that relation
                
                // TODO

                // process that stuff
                if(this.importer.importRelation(relation)) {
                    this.numberRelations++;
                }
                
            }
        } catch (SQLException ex) {
            System.err.println(ex.getLocalizedMessage());
        }
        
        System.out.println("Checked ways: " + number);
        
    }
    
    public String getStatistics() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("imported: ");
        sb.append(this.numberNodes);
        sb.append(" nodes | ");
        sb.append(this.numberWays);
        sb.append(" ways | ");
        sb.append(this.numberRelations);
        sb.append(" relations | ");
        
        return sb.toString();
    }
    
    ///////////////////////////////////////////////////////////////////////
    //                         factory methods                           //
    ///////////////////////////////////////////////////////////////////////
    
    protected OHDMRelation createOHDMRelation(ResultSet qResult) throws SQLException {
        return null; // TODO
    }
    
    protected OHDMWay createOHDMWay(ResultSet qResult) throws SQLException {
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
    
    protected OHDMNode createOHDMNode(ResultSet qResult) throws SQLException {
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
    
}
