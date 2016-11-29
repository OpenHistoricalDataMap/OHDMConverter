package exportfromintermediate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import osmupdatewizard.SQLStatementQueue;
import osmupdatewizard.TagElement;

/**
 *
 * @author thsc
 */
public class OHDMImporter extends Transfer implements Importer {
    
    public OHDMImporter(Connection sourceConnection, Connection targetConnection) {
        super(sourceConnection, targetConnection);
    }

    @Override
    public boolean importWay(OHDMWay way) {
//        String className = way.getClassName();
//        if(!className.equalsIgnoreCase(featureClass)) return false;
//        
//        // right class
//        String wayGeometryWKT = way.getWKTGeometry();
//
//        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
//            
//            /* insert like this:
//INSERT INTO highway_lines(
//            line, subclassname, name, valid_since, valid_until)
//    VALUES ('WKT', 2, 'name', '1970-01-01', '2016-01-01');            
//                    */
//            
//        sq.append("INSERT INTO highway_lines(\n");
//        sq.append("line, subclassname, name, valid_since, valid_until)");
//        sq.append(" VALUES (");
//        
//        sq.append("'");
//        sq.append(way.getWKTGeometry()); // geometry
//        sq.append("', '");
//        
//        sq.append(way.getSubClassName()); // feature sub class
//
//        sq.append("', '");
//        
//        sq.append(way.getName()); // feature sub class
//
//        sq.append("', '");
//        sq.append(way.validSince());
//
//        sq.append("', '");
//        sq.append(way.validUntil());
//        
//        sq.append("');");
//        
//        sq.flush();
//        
        return true;
    }

    @Override
    public boolean importRelation(OHDMRelation relation) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    private int idExternalSystemOSM = -1;
    private int getOHDM_ID_externalSystemOSM() {
        if(this.idExternalSystemOSM == -1) {
            try {
                ResultSet result = 
                        this.executeQueryOnTarget("SELECT id FROM external_systems where name = 'OSM';");

                this.idExternalSystemOSM = result.getInt(1);

            } catch (SQLException ex) {
                Logger.getLogger(OHDMImporter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        return this.idExternalSystemOSM;
    }
    
    private final HashMap<String, Integer> idExternalUsers = new HashMap<>();
    private int getOHDM_ID_ExternalUser(String externalUserID, String externalUserName) {
        Integer idInteger = this.idExternalUsers.get(externalUserID);
        if(idInteger != null) { // already in memory
            return idInteger;
        }
        
        try {
            // search in db
            // SELECT id from external_users where userid = '43566';
            ResultSet result = this.executeQueryOnTarget
                    ("SELECT id from external_users where userid = '" +
                                    externalUserID + "';");

            int ohdmID = result.getInt(1);
            
            // keep it
            this.idExternalUsers.put(externalUserID, ohdmID);
            
            return ohdmID;
                // create entry
    /*
    INSERT INTO external_users(userid, username, external_system_id)
        VALUES ('43566', 'anbr', 1);            
                */
        } catch (SQLException ex) {
            SQLStatementQueue s = new SQLStatementQueue(this.targetConnection);
            s.append("INSERT INTO external_users(userid, username, external_system_id) VALUES ('");
            s.append(externalUserID);
            s.append("', '");
            s.append(externalUserName);
            s.append("', ");
            s.append(this.getOHDM_ID_externalSystemOSM());
            s.append(");");
            s.flush();
            
            // again - it is now in database
            return this.getOHDM_ID_ExternalUser(externalUserID, externalUserName);
        }
        
    }
    @Override
    public boolean importNode(OHDMNode node) {
        ArrayList<TagElement> tags = node.getTags();
        
        /* nodes without tags have no identity and are part of a way or relation
        and stored with them. We are done here.
        */
        if(tags == null) return false;
        // tag has an identity due to its tags

        // create user entry or find user primary key
        String externalUserID = node.getUserID();
        String externalUsername = node.getUsername();
        
        int ohdm_id_ExternalUser = this.getOHDM_ID_ExternalUser(externalUserID, externalUsername);
        
        // create OHDM object
        // HIER WEITERMACHEN... BEKOMMT man bei einem insert den primary key zurück?
        
        // create a geoemtry in OHDM
        
        // create entry in object_geometry table
        
        // keep some special tags (url etc, see wiki)
        
        // remind those actions in intermediate database by setting ohdm_id
        
        return true;
    }
    
    private ResultSet executeQueryOnTarget(String sql) throws SQLException {
        PreparedStatement stmt = this.targetConnection.prepareStatement(sql);
        ResultSet result = stmt.executeQuery();
        result.next();
        
        return result;
    }
    
    private void executeOnTarget(String sql) throws SQLException {
        PreparedStatement stmt = this.targetConnection.prepareStatement(sql);
        stmt.execute();
    }
    

}
