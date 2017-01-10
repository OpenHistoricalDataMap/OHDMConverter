package inter2ohdm;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import static osm2inter_v2.InterDB.NODETABLE;
import static osm2inter_v2.InterDB.RELATIONMEMBER;
import static osm2inter_v2.InterDB.RELATIONTABLE;
import static osm2inter_v2.InterDB.WAYMEMBER;
import static osm2inter_v2.InterDB.WAYTABLE;
import util.DB;
import util.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public class IntermediateDB {
    protected final Connection sourceConnection;
    private final String schema;
    
    IntermediateDB(Connection sourceConnection, String schema) {
        this.sourceConnection = sourceConnection;
        this.schema = schema;
    }
    
    protected String getIntermediateTableName(OHDMElement element) {
        if(element instanceof OHDMNode) {
            return(DB.getFullTableName(this.schema, NODETABLE));
        } else if(element instanceof OHDMWay) {
            return(DB.getFullTableName(this.schema, WAYTABLE));
        } else {
            return(DB.getFullTableName(this.schema, RELATIONTABLE));
        } 
    }
    
    public void setOHDM_IDs(OHDMElement element, String ohdmObjectIDString, 
            String ohdmGeomIDString) throws SQLException {
        
        if(element == null) return;
        
        if(ohdmObjectIDString == null && ohdmGeomIDString == null) return;
        
        /*
        UPDATE [waysTable] SET ohdm_id=[ohdmID] WHERE osm_id = [osmID];
        */
        SQLStatementQueue sq = new SQLStatementQueue(this.sourceConnection);
        sq.append("UPDATE ");
        
        sq.append(this.getIntermediateTableName(element));
        
        sq.append(" SET ");
        boolean parameterSet = false;
        if(ohdmObjectIDString != null) {
            sq.append("ohdm_object_id = ");
            sq.append(ohdmObjectIDString);
            parameterSet = true;
        }
        
        if(ohdmGeomIDString != null) {
            if(parameterSet) {
                sq.append(", ");
            }
            sq.append("ohdm_geom_id = ");
            sq.append(ohdmGeomIDString);
        }
        
        sq.append(" WHERE osm_id = ");
        sq.append(element.getOSMIDString());
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

            sq.append(DB.getFullTableName(this.schema, RELATIONMEMBER));

            sq.append(" WHERE relation_id = ");
            sq.append(element.getOSMIDString());
            sq.append(";");
            sq.forceExecute();
        } else if(element instanceof OHDMWay) {
            // remove line from relationsmember
            sq.append("DELETE FROM ");

            sq.append(DB.getFullTableName(this.schema, WAYMEMBER));

            sq.append(" WHERE way_id = ");
            sq.append(element.getOSMIDString());
            sq.append(";");
            sq.forceExecute();
        }
        
        /*
        DELETE FROM [table] WHERE osm_id = [osmID]
        */
        
        sq.append("DELETE FROM ");
        
        sq.append(this.getIntermediateTableName(element));
        
        sq.append(" WHERE osm_id = ");
        sq.append(element.getOSMIDString());
        sq.append(";");
        sq.forceExecute();
    }
    
    OHDMWay addNodes2OHDMWay(OHDMWay way) throws SQLException {
        // find all associated nodes and add to that way
        /* SQL Query is like this
            select * from nodes_table where osm_id IN 
            (SELECT node_id FROM waynodes_table where way_id = ID_of_way);            
        */ 
        SQLStatementQueue sql = new SQLStatementQueue(this.sourceConnection);

        sql.append("select * from ");
        sql.append(DB.getFullTableName(this.schema, NODETABLE));
        sql.append(" where osm_id IN (SELECT node_id FROM ");            
        sql.append(DB.getFullTableName(this.schema, WAYMEMBER));
        sql.append(" where way_id = ");            
        sql.append(way.getOSMIDString());
        sql.append(");");  

        ResultSet qResultNode = sql.executeWithResult();

        while(qResultNode.next()) {
            OHDMNode node = this.createOHDMNode(qResultNode);
            way.addNode(node);
        }
        
        qResultNode.close();
        
        return way;
    }
    
    
    ///////////////////////////////////////////////////////////////////////
    //                         factory methods                           //
    ///////////////////////////////////////////////////////////////////////
    
    private String osmIDString;
    private String classCodeString;
    private String sTags;
    private String ohdmObjectIDString;
    private String ohdmGeomIDString;
    private String memberIDs;
    private boolean valid;
    
    private void readCommonColumns(ResultSet qResult) throws SQLException {
        osmIDString = this.extractBigDecimalAsString(qResult, "osm_id");
        System.out.print(", " + osmIDString);
        if(osmIDString.equalsIgnoreCase("1368193")) {
            int i = 42;
        }
        classCodeString = this.extractBigDecimalAsString(qResult, "classcode");
        sTags = qResult.getString("serializedtags");
        ohdmObjectIDString = this.extractBigDecimalAsString(qResult, "ohdm_object_id");
        ohdmGeomIDString = this.extractBigDecimalAsString(qResult, "ohdm_geom_id");
        valid = qResult.getBoolean("valid");
    }
    
    private String extractBigDecimalAsString(ResultSet qResult, String columnName) throws SQLException {
        BigDecimal bigDecimal = qResult.getBigDecimal(columnName);
        if(bigDecimal != null) {
            return bigDecimal.toString();
        }
        return null;
    }
    
    protected OHDMRelation createOHDMRelation(ResultSet qResult) throws SQLException {
        // get all data to create an ohdm way object
        this.readCommonColumns(qResult);
        memberIDs = qResult.getString("member_ids");

        OHDMRelation relation = new OHDMRelation(this, osmIDString, classCodeString, sTags, memberIDs, ohdmObjectIDString, ohdmGeomIDString, valid);
        
        return relation;
    }
    
    protected OHDMWay createOHDMWay(ResultSet qResult) throws SQLException {
        this.readCommonColumns(qResult);
        String nodeIDs = qResult.getString("node_ids");
        boolean isPart = qResult.getBoolean("is_part");

        OHDMWay way = new OHDMWay(this, osmIDString, classCodeString, sTags, nodeIDs, ohdmObjectIDString, ohdmGeomIDString, isPart, valid);

        return way;
    }
    
    protected OHDMNode createOHDMNode(ResultSet qResult) throws SQLException {
        this.readCommonColumns(qResult);
        String longitude = qResult.getString("longitude");
        String latitude = qResult.getString("latitude");
        boolean isPart = qResult.getBoolean("is_part");
        
        OHDMNode node = new OHDMNode(this, osmIDString, classCodeString, sTags, longitude, latitude, ohdmObjectIDString, ohdmGeomIDString, isPart, valid);

        return node;
    }
    
}
