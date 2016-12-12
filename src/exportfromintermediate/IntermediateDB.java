package exportfromintermediate;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import static osmupdatewizard.SQLImportCommandBuilder.NODETABLE;
import static osmupdatewizard.SQLImportCommandBuilder.RELATIONMEMBER;
import static osmupdatewizard.SQLImportCommandBuilder.RELATIONTABLE;
import static osmupdatewizard.SQLImportCommandBuilder.WAYMEMBER;
import static osmupdatewizard.SQLImportCommandBuilder.WAYTABLE;
import osmupdatewizard.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public class IntermediateDB {
    protected final Connection sourceConnection;
    
    IntermediateDB(Connection sourceConnection) {
        this.sourceConnection = sourceConnection;
    }
    
    protected String getIntermediateTableName(OHDMElement element) {
        if(element instanceof OHDMNode) {
            return(NODETABLE);
        } else if(element instanceof OHDMWay) {
            return(WAYTABLE);
        } else {
            return(RELATIONTABLE);
        } 
    }
    
    public void setOHDM_ID(OHDMElement element, int ohdmID) throws SQLException {
        if(element == null) return;
        
        /*
        UPDATE [waysTable] SET ohdm_id=[ohdmID] WHERE osm_id = [osmID];
        */
        SQLStatementQueue sq = new SQLStatementQueue(this.sourceConnection);
        sq.append("UPDATE ");
        
        sq.append(this.getIntermediateTableName(element));
        
        sq.append(" SET ohdm_id= ");
        sq.append(ohdmID);
        sq.append(" WHERE osm_id = ");
        sq.append(element.getOSMID());
        sq.append(";");
        sq.forceExecute();
    }
    
    void remove(OHDMElement element) throws SQLException {
        SQLStatementQueue sq = new SQLStatementQueue(this.sourceConnection);
        
        /*
        remove entries which refer to that element
        */

        if(element instanceof OHDMRelation) {
            // remove line from relationsmember
            sq.append("DELETE FROM ");

            sq.append(RELATIONMEMBER);

            sq.append(" WHERE relation_id = ");
            sq.append(element.getOSMID());
            sq.append(";");
            sq.forceExecute();
        } else if(element instanceof OHDMWay) {
            // remove line from relationsmember
            sq.append("DELETE FROM ");

            sq.append(WAYMEMBER);

            sq.append(" WHERE way_id = ");
            sq.append(element.getOSMID());
            sq.append(";");
            sq.forceExecute();
        }
        
        /*
        DELETE FROM [table] WHERE osm_id = [osmID]
        */
        
        sq.append("DELETE FROM ");
        
        sq.append(this.getIntermediateTableName(element));
        
        sq.append(" WHERE osm_id = ");
        sq.append(element.getOSMID());
        sq.append(";");
        sq.forceExecute();
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
        sql.append(way.getOSMID().intValue());
        sql.append(");");  

        ResultSet qResultNode = sql.executeWithResult();

        while(qResultNode.next()) {
            OHDMNode node = this.createOHDMNode(qResultNode);
            way.addNode(node);
        }
        
        qResultNode.close();
    }
    
    
    ///////////////////////////////////////////////////////////////////////
    //                         factory methods                           //
    ///////////////////////////////////////////////////////////////////////
    
    protected OHDMRelation createOHDMRelation(ResultSet qResult) throws SQLException {
        // get all data to create an ohdm way object
        BigDecimal osmIDBig = qResult.getBigDecimal("osm_id");
        BigDecimal classCodeBig = qResult.getBigDecimal("classcode");
        String sTags = qResult.getString("serializedtags");
        BigDecimal ohdmIDBig = qResult.getBigDecimal("ohdm_id");
        BigDecimal ohdmObjectIDBig = qResult.getBigDecimal("ohdm_object");
        String memberIDs = qResult.getString("member_ids");
        boolean valid = qResult.getBoolean("valid");

        OHDMRelation relation = new OHDMRelation(this, osmIDBig, classCodeBig, sTags, memberIDs, ohdmIDBig, ohdmObjectIDBig, valid);
        
        return relation;
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

        OHDMWay way = new OHDMWay(this, osmIDBig, classCodeBig, sTags, nodeIDs, ohdmIDBig, ohdmObjectIDBig, valid);

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

        OHDMNode node = new OHDMNode(this, osmIDBig, classCodeBig, sTags, longitude, latitude, ohdmIDBig, ohdmObjectIDBig, valid);

        return node;
    }
    
}
