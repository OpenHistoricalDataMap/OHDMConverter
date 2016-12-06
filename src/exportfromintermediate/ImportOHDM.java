package exportfromintermediate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import osmupdatewizard.SQLStatementQueue;
import osmupdatewizard.TagElement;

/**
 * That class imports (and updates) data from intermediate database to OHDM.
 * It changes both, ohdm data and intermediate data.
 * 
 * @author thsc
 */
public class ImportOHDM extends Importer {
    
    public ImportOHDM(Connection sourceConnection, Connection targetConnection) {
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
                Logger.getLogger(ImportOHDM.class.getName()).log(Level.SEVERE, null, ex);
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
    
    ////////////////////////////////////////////////////////////////////////
    //                          CREATE STRUCTURES                         //
    ////////////////////////////////////////////////////////////////////////

    // primary key are created identically
    static protected String getCreatePrimaryKeyDescription(String schema, String tableName) {
        return "id bigint NOT NULL DEFAULT nextval('"
                + ImportOHDM.getSequenceName(ImportOHDM.getFull_TableName(schema, tableName))
                + "'::regclass),"
                + " CONSTRAINT "
                + tableName
                + "_pkey PRIMARY KEY (id)";
    }
    
    static protected void createSequence(Connection targetConnection, String schema, String tableName) {
        SQLStatementQueue sq = new SQLStatementQueue(targetConnection);
        
        sq.append("CREATE SEQUENCE "); 
        sq.append(ImportOHDM.getSequenceName(ImportOHDM.getFull_TableName(schema, tableName)));
        sq.append(" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;");
        sq.flush();
    }
    
    static protected void drop(Connection targetConnection, String schema, String tableName) {
        SQLStatementQueue sq = new SQLStatementQueue(targetConnection);
        
        sq.append("DROP SEQUENCE ");
        sq.append(ImportOHDM.getSequenceName(ImportOHDM.getFull_TableName(schema, tableName)));
        sq.append(" CASCADE;");
        sq.flush();
        
        sq.append("DROP TABLE ");
        sq.append(ImportOHDM.getFull_TableName(schema, tableName));
        sq.append(" CASCADE;");
        sq.flush();
    }
    
    ////////////////////////////////////////////////////////////////////////
    //                                names                               //
    ////////////////////////////////////////////////////////////////////////
    
    static protected String getSequenceName(String tableName) {
        return tableName + "_id ";
    }
    
    static protected String getFull_TableName(String schema, String tableName) {
        return schema + "." + tableName;
    }
    
    static protected String getCreateTableBegin(String schema, String tableName) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("CREATE TABLE ");
        sb.append(ImportOHDM.getFull_TableName(schema, tableName));
        sb.append(" (");
        sb.append(ImportOHDM.getCreatePrimaryKeyDescription(schema, tableName)); // without schema!
        
        return sb.toString();
    }
    
    // Table names
    static final String EXTERNAL_SYSTEMS = "external_systems";
    static final String EXTERNAL_USERS = "external_users";
    static final String CLASSIFICATION = "classification";
    static final String CONTENT = "content";
    static final String GEOOBJECT = "geoobject";
    static final String GEOOBJECT_CONTENT = "geoobject_content";
    static final String GEOOBJECT_GEOMETRY = "geoobject_geometry";
    static final String GEOOBJECT_URL = "geoobject_url";
    static final String LINES = "lines";
    static final String POINTS = "points";
    static final String POLYGONS = "polygons";
    static final String URL = "url";
    
    
    void dropOHDMTables(Connection targetConnection, String schema) {
        // drop
        ImportOHDM.drop(targetConnection, schema, EXTERNAL_SYSTEMS);
        ImportOHDM.drop(targetConnection, schema, EXTERNAL_USERS);
        ImportOHDM.drop(targetConnection, schema, CLASSIFICATION);
        ImportOHDM.drop(targetConnection, schema, CONTENT);
        ImportOHDM.drop(targetConnection, schema, GEOOBJECT);
        ImportOHDM.drop(targetConnection, schema, GEOOBJECT_CONTENT);
        ImportOHDM.drop(targetConnection, schema, GEOOBJECT_GEOMETRY);
        ImportOHDM.drop(targetConnection, schema, GEOOBJECT_URL);
        ImportOHDM.drop(targetConnection, schema, LINES);
        ImportOHDM.drop(targetConnection, schema, POINTS);
        ImportOHDM.drop(targetConnection, schema, POLYGONS);
        ImportOHDM.drop(targetConnection, schema, URL);
    }
    
    void createOHDMTables(Connection targetConnection, String schema) {
        SQLStatementQueue sq;
        
        // EXTERNAL SYSTEMS
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, EXTERNAL_SYSTEMS);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.EXTERNAL_SYSTEMS));
        // add table specifics
        sq.append(",");
        sq.append("name character varying,");
        sq.append("description character varying");
        sq.append(");");
        sq.flush();
        
        // EXTERNAL_USERS
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, EXTERNAL_USERS);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.EXTERNAL_USERS));
        // add table specifics:
        sq.append(",");
        sq.append("userid bigint,");
        sq.append("username character varying,");
        sq.append("external_system_id bigint NOT NULL");
        // TODO: add foreign key constraints
        sq.append(");");
        sq.flush();
        
        // CLASSIFICATION
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, CLASSIFICATION);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.CLASSIFICATION));
        // add table specifics:
        sq.append(",");
        sq.append("class character varying,");
        sq.append("subclass character varying");
        sq.append(");");
        sq.flush();
        
        // CONTENT
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, CONTENT);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.CONTENT));
        // add table specifics:
        sq.append(",");
        sq.append("name character varying,");
        sq.append("value bytea NOT NULL,");
        sq.append("mimetype character varying");
        sq.append(");");
        sq.flush();
        
        // GEOOBJECT
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, GEOOBJECT);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.GEOOBJECT));
        // add table specifics:
        sq.append(",");
        sq.append("name character varying,");
        sq.append("classification_id bigint NOT NULL,");
        sq.append("source_user_id bigint NOT NULL");
        sq.append(");");
        sq.flush();
        
        // GEOOBJECT_CONTENT
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, GEOOBJECT_CONTENT);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.GEOOBJECT_CONTENT));
        // add table specifics:
        sq.append(",");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("valid_since_offset bigint DEFAULT 0,");
        sq.append("valid_until_offset bigint DEFAULT 0,");
        sq.append("geoobject_id bigint NOT NULL,");
        sq.append("content_id bigint NOT NULL");
        sq.append(");");
        sq.flush();
        
        // GEOOBJECT_GEOMETRY
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, GEOOBJECT_GEOMETRY);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.GEOOBJECT_GEOMETRY));
        // add table specifics:
        sq.append(",");
        sq.append("id_point bigint,");
        sq.append("id_line bigint,");
        sq.append("id_polygon bigint,");
        sq.append("id_geoobject_target bigint,");
        sq.append("id_geoobject_source bigint NOT NULL,");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("valid_since_offset bigint DEFAULT 0,");
        sq.append("valid_until_offset bigint DEFAULT 0");
        sq.append(");");
        sq.flush();
        
        // GEOOBJECT_URL
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, GEOOBJECT_URL);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.GEOOBJECT_URL));
        // add table specifics:
        sq.append(",");
        sq.append("geoobject_id bigint NOT NULL,");
        sq.append("url_id bigint NOT NULL,");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("valid_since_offset bigint DEFAULT 0,");
        sq.append("valid_until_offset bigint DEFAULT 0");
        sq.append(");");
        sq.flush();
        
        // LINES
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, LINES);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.LINES));
        // add table specifics:
        sq.append(",");
        sq.append("line geometry,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.flush();
        
        // POINTS
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, POINTS);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.POINTS));
        // add table specifics:
        sq.append(",");
        sq.append("point geometry,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.flush();
        
        // POLYGONS
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, POLYGONS);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.POLYGONS));
        // add table specifics:
        sq.append(",");
        sq.append("polygone geometry,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.flush();
        
        // URL
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, URL);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.URL));
        // add table specifics:
        sq.append(",");
        sq.append("url character varying");
        sq.append(");");
        sq.flush();
        
    }
    
    public static void main(String args[]) {
        // let's fill OHDM database
        try {
            Connection sourceConnection = Importer.createLocalTestSourceConnection();
            Connection targetConnection = Importer.createLocalTestTargetConnection();
            
            ImportOHDM ohdmImporter = new ImportOHDM(sourceConnection, targetConnection);
            
            ohdmImporter.dropOHDMTables(targetConnection, "ohdm");
            ohdmImporter.createOHDMTables(targetConnection, "ohdm");

            /*
            ExportIntermediateDB exporter = 
                    new ExportIntermediateDB(sourceConnection, ohdmImporter);
            
            exporter.processNodes();
            exporter.processWays();
            exporter.processRelations();
            */
  
        } catch (SQLException e) {
          System.err.println("cannot connect to database: " + e.getLocalizedMessage());
        }
    }
    
}
