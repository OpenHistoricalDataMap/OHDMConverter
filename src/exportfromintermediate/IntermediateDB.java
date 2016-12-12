package exportfromintermediate;

import java.sql.Connection;
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
    
    public void setOHDM_ID(OHDMElement element, int ohdmID) {
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
    
    void remove(OHDMElement element) {
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
}
