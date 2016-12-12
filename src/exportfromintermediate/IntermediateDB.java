package exportfromintermediate;

import java.sql.Connection;
import static osmupdatewizard.SQLImportCommandBuilder.NODETABLE;
import static osmupdatewizard.SQLImportCommandBuilder.RELATIONTABLE;
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
    
    public void setOHDM_ID(OHDMElement element, int ohdmID) {
        if(element == null) return;
        
        /*
        UPDATE [waysTable] SET ohdm_id=[ohdmID] WHERE osm_id = [osmID];
        */
        SQLStatementQueue sq = new SQLStatementQueue(this.sourceConnection);
        sq.append("UPDATE ");
        
        if(element instanceof OHDMNode) {
            sq.append(NODETABLE);
        } else if(element instanceof OHDMWay) {
            sq.append(WAYTABLE);
        } else {
            sq.append(RELATIONTABLE);
        } 
        
        sq.append(" SET ohdm_id= ");
        sq.append(ohdmID);
        sq.append(" WHERE osm_id = ");
        sq.append(element.getOSMID());
        sq.append(";");
        sq.forceExecute();
    }
}
