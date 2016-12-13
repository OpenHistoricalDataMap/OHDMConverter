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
import static osmupdatewizard.SQLImportCommandBuilder.RELATIONMEMBER;
import osmupdatewizard.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public class ExportIntermediateDB extends IntermediateDB {
    private final Importer importer;
    
    private int numberNodes = 0;
    private int numberWays = 0;
    private int numberRelations = 0;
    
    ExportIntermediateDB(Connection sourceConnection, Importer importer) {
        super(sourceConnection);
        
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
        
        System.out.println("Checked / imported nodes: " + number + " / " + this.numberNodes);
    }
    
    void processWays() {
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(WAYTABLE).append(";");
        
        int waynumber = 0;
        try {
            PreparedStatement stmt = this.sourceConnection.prepareStatement(sql.toString());
            ResultSet qResultWay = stmt.executeQuery();
            
            while(qResultWay.next()) {
                waynumber++;
                OHDMWay way = this.createOHDMWay(qResultWay);
                
                this.addNodes2OHDMWay(way);

                // process that stuff
                if(this.importer.importWay(way)) {
                    this.numberWays++;
                }
            }
        } catch (SQLException ex) {
            System.err.println(ex.getLocalizedMessage());
        }
        
        System.out.println("Checked / imported ways:  " + waynumber + " / " + this.numberWays);
    }
    
    void addNodes2OHDMWay(OHDMWay way) throws SQLException {
        // find all associated nodes and add to that way
        /* SQL Query is like this
            select * from nodes_table where osm_id IN 
            (SELECT node_id FROM waynodes_table where way_id = ID_of_way);            
        */ 
        SQLStatementQueue sql = new SQLStatementQueue(this.sourceConnection);

        sql.append("select * from ");
        sql.append(NODETABLE);
        sql.append(" where osm_id IN (SELECT node_id FROM ");            
        sql.append(WAYMEMBER);
        sql.append(" where way_id = ");            
        sql.append(way.getOSMIDString());
        sql.append(");");  

        ResultSet qResultNode = sql.executeWithResult();

        while(qResultNode.next()) {
            OHDMNode node = this.createOHDMNode(qResultNode);
            way.addNode(node);
        }
        
        qResultNode.close();
    }
    
    void processRelations() throws SQLException {
        SQLStatementQueue sql = new SQLStatementQueue(this.sourceConnection);
        
        sql.append("SELECT * FROM ");
        sql.append(RELATIONTABLE);
        sql.append(";");
        
        int number = 0;
        try {
            ResultSet qResultRelations = sql.executeWithResult();
            
            while(qResultRelations.next()) {
                number++;
                OHDMRelation relation = this.createOHDMRelation(qResultRelations);

                // find all associated nodes and add to that relation
                sql.append("select * from ");
                sql.append(RELATIONMEMBER);
                sql.append(" where relation_id = ");            
                sql.append(relation.getOSMIDString());
                sql.append(";");  

                ResultSet qResultRelation = sql.executeWithResult();

                boolean relationMemberComplete = true; // assume we find all member
                
                while(qResultRelation.next()) {
                    String roleString =  qResultRelation.getString("role");

                    // extract member objects from their tables
                    BigDecimal id;
                    OHDMElement.GeometryType type = null;
                    
                    sql.append("SELECT * FROM ");
                    
                    id = qResultRelation.getBigDecimal("node_id");
                    if(id != null) {
                        sql.append(NODETABLE);
                        type = OHDMElement.GeometryType.POINT;
                    } else {
                        id = qResultRelation.getBigDecimal("way_id");
                        if(id != null) {
                            sql.append(WAYTABLE);
                            type = OHDMElement.GeometryType.LINESTRING;
                        } else {
                            id = qResultRelation.getBigDecimal("member_rel_id");
                            if(id != null) {
                                sql.append(RELATIONTABLE);
                                type = OHDMElement.GeometryType.RELATION;
                            } else {
                                // we have a serious problem here.. or no member
                            }
                        }
                    }
                    sql.append(" where osm_id = ");
                    sql.append(id.toString());
                    sql.append(";");
                    
                    ResultSet memberResult = sql.executeWithResult();
                    if(memberResult.next()) {
                        // this call can fail, see else branch
                        OHDMElement memberElement = null;
                        switch(type) {
                            case POINT: 
                                memberElement = this.createOHDMNode(memberResult);
                                break;
                            case LINESTRING:
                                memberElement = this.createOHDMWay(memberResult);
                                break;
                            case RELATION:
                                memberElement = this.createOHDMRelation(memberResult);
                                break;
                        }
                        relation.addMember(memberElement, roleString);
                    } else {
                        /* this call can fail
                        a) if this program is buggy - which is most likely :) OR
                        b) intermediate DB has not imported whole world. In that
                        case, relation can refer to data which are not actually 
                        stored in intermediate db tables.. 
                        in that case .. remove whole relation: parts of it are 
                        outside our current scope
                        */
                        relation.remove();
                        relationMemberComplete = false; 
                    }
                    memberResult.close();
                    
                    if(!relationMemberComplete) break;
                }
                
                // process that stuff
                if(relationMemberComplete && this.importer.importRelation(relation)) {
                    this.numberRelations++;
                }
            }
        } catch (SQLException ex) {
            System.err.println(ex.getLocalizedMessage());
        }
        
        System.out.println("Checked / imported relations: " + number + " / " + this.numberRelations);
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
}
