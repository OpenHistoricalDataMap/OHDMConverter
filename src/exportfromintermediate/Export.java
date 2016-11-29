package exportfromintermediate;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author thsc
 */
public class Export {
    protected final Connection sourceConnection;
    protected final Connection targetConnection;
    
    Export(Connection sourceConnection, Connection targetConnection) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
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
