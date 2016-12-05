package exportfromintermediate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author thsc
 */
public abstract class Importer {
    protected final Connection sourceConnection;
    protected final Connection targetConnection;
    
    Importer(Connection sourceConnection, Connection targetConnection) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
    }
    
    protected ResultSet executeQueryOnTarget(String sql) throws SQLException {
        PreparedStatement stmt = this.targetConnection.prepareStatement(sql);
        ResultSet result = stmt.executeQuery();
        result.next();
        
        return result;
    }
    
    protected void executeOnTarget(String sql) throws SQLException {
        PreparedStatement stmt = this.targetConnection.prepareStatement(sql);
        stmt.execute();
    }
    
    public abstract boolean importWay(OHDMWay way);

    public abstract boolean importRelation(OHDMRelation relation);

    public abstract boolean importNode(OHDMNode node);
}
