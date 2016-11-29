package exportfromintermediate;

import java.sql.Connection;

/**
 *
 * @author thsc
 */
public class Transfer {
    protected final Connection sourceConnection;
    protected final Connection targetConnection;
    
    Transfer(Connection sourceConnection, Connection targetConnection) {
        this.sourceConnection = sourceConnection;
        this.targetConnection = targetConnection;
    }
    
}
