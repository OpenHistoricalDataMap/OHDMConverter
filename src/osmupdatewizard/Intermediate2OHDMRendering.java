package osmupdatewizard;

import java.sql.Connection;
import static osmupdatewizard.SQLImportCommandBuilder.WAYTABLE;
import static osmupdatewizard.SQLImportCommandBuilder.NODETABLE;

/**
 * Fill OHDM rendering data base from intermediate data base.
 * It's a short cut to increase development and not meant
 * to be part of the productive system
 * @author thsc
 */
public class Intermediate2OHDMRendering {
    private final Connection connection;
    
    Intermediate2OHDMRendering(Connection connection) {
        this.connection = connection;
        
    }
    
    public void go() {
        // extract ways
        this.extractWays();
    }

    private void extractWays() {
        // open ways table

        
    StringBuilder sqlWay = new StringBuilder("SELECT * FROM ");
    sqlWay.append(WAYTABLE).append(" WHERE osm_id = ? ;");
    StringBuilder sqlNodes = new StringBuilder("SELECT * FROM ");
    sqlNodes.append(NODETABLE).append(" WHERE id_way = ?;");
        
    }
}
