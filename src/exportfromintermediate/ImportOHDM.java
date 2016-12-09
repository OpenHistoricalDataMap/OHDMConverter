package exportfromintermediate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import osm.OSMClassification;
import osmupdatewizard.SQLStatementQueue;
import osmupdatewizard.TagElement;

/**
 * That class imports (and updates) data from intermediate database to OHDM.
 * It changes both, ohdm data and intermediate data.
 * 
 * @author thsc
 */
public class ImportOHDM extends Importer {
    private final String sourceSchema;
    private final String targetSchema;
    
    public ImportOHDM(Connection sourceConnection, Connection targetConnection, 
            String sourceSchema, String targetSchema) {
        super(sourceConnection, targetConnection);
        
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
    }

    @Override
    public boolean importWay(OHDMWay way) {
        return this.importOHDMElement(way);
    }

    @Override
    public boolean importRelation(OHDMRelation relation) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    private int idExternalSystemOSM = -1;
    private int getOHDM_ID_externalSystemOSM() {
        if(this.idExternalSystemOSM == -1) {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("SELECT id FROM ");
                sb.append(ImportOHDM.getFullTableName(targetSchema, ImportOHDM.EXTERNAL_SYSTEMS));
                sb.append(" where name = 'OSM' OR name = 'osm';");
                ResultSet result = 
                        this.executeQueryOnTarget(sb.toString());
                
                result.next();
                this.idExternalSystemOSM = result.getInt(1);

            } catch (SQLException ex) {
                Logger.getLogger(ImportOHDM.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        return this.idExternalSystemOSM;
    }

    boolean validUserID(String userID) {
        if(userID.equalsIgnoreCase("-1")) { 
            return false; 
        }
        
        return true;
    }
    
    static final int UNKNOWN_USER_ID = -1;
    
    private final HashMap<String, Integer> idExternalUsers = new HashMap<>();
    private int getOHDM_ID_ExternalUser(String externalUserID, String externalUserName) {
        if(!this.validUserID(externalUserID)) return ImportOHDM.UNKNOWN_USER_ID;
        
        Integer idInteger = this.idExternalUsers.get(externalUserID);
        if(idInteger != null) { // already in memory
            return idInteger;
        }
        
        int osm_id = this.getOHDM_ID_externalSystemOSM();
        
        int ohdmID = -1; // -1 means failure
        try {
            // search in db
            // SELECT id from external_users where userid = '43566';
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT id from ");
            sb.append(ImportOHDM.getFullTableName(this.targetSchema, ImportOHDM.EXTERNAL_USERS));
            sb.append(" where userid = '");
            sb.append(externalUserID);
            sb.append("' AND external_system_id = '");
            sb.append(osm_id);
            sb.append("';");
            
            ResultSet result = this.executeQueryOnTarget(sb.toString());
            
            if(result.next()) {
                // there is an entry
                ohdmID = result.getInt(1);

                // keep it
                this.idExternalUsers.put(externalUserID, ohdmID);
            } else {
                // there is no entry
                StringBuilder s = new StringBuilder();
                //SQLStatementQueue s = new SQLStatementQueue(this.targetConnection);
                s.append("INSERT INTO ");
                s.append(ImportOHDM.getFullTableName(this.targetSchema, ImportOHDM.EXTERNAL_USERS));
                s.append(" (userid, username, external_system_id) VALUES ('");
                s.append(externalUserID);
                s.append("', '");
                s.append(externalUserName);
                s.append("', ");
                s.append(this.getOHDM_ID_externalSystemOSM());
                s.append(") RETURNING id;");
                //s.flush();
                
                ResultSet insertResult = this.executeQueryOnTarget(s.toString());
                insertResult.next();
                ohdmID = insertResult.getInt(1);
            }
        } catch (SQLException ex) {
            // TODO serious probleme
            System.err.println("thats a serious problem, cannot insert/select external user id: " + ex.getMessage());
        }
        
        return ohdmID;
        
    }
    
    int addOHDMObject(OHDMElement ohdmElement, int externalUserID) throws SQLException {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
        String name = ohdmElement.getName();
        int classID = ohdmElement.getClassCode();
        
        sq.append("INSERT INTO ");
        sq.append(ImportOHDM.getFullTableName(this.targetSchema, ImportOHDM.GEOOBJECT));
        sq.append(" (name, classification_id, source_user_id) VALUES ('");
        sq.append(name);
        sq.append("', ");
        sq.append(classID);
        sq.append(", ");
        sq.append(externalUserID);
        sq.append(") RETURNING id;");
        
        ResultSet result = sq.executeQueryOnTarget();
        result.next();
        return result.getInt(1);
    }
    
    int addGeometry(OHDMElement ohdmElement, int externalUserID) throws SQLException {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
        sq.append("INSERT INTO ");
        
        String fullTableName;
        
        switch(ohdmElement.getGeometryType()) {
            case POINT: 
                fullTableName = ImportOHDM.getFullTableName(this.targetSchema, ImportOHDM.POINTS);
                sq.append(fullTableName);
                sq.append(" (point, ");
                break;
            case LINESTRING: 
                fullTableName = ImportOHDM.getFullTableName(this.targetSchema, ImportOHDM.LINES);
                sq.append(fullTableName);
                sq.append(" (line, ");
                break;
            case POLYGON: 
                fullTableName = ImportOHDM.getFullTableName(this.targetSchema, ImportOHDM.POLYGONS);
                sq.append(fullTableName);
                sq.append(" (polygon, ");
                break;
        }
        
        sq.append(" source_user_id) VALUES ('");
        
        String wkt = ohdmElement.getWKTGeometry();
        sq.append(wkt);
        sq.append("', ");
        sq.append(externalUserID);
        sq.append(") RETURNING id;");
        
        ResultSet result = sq.executeQueryOnTarget();
        result.next();
        return result.getInt(1);
    }
    
    void addValidity(OHDMElement ohdmElement, int object_id, int geometry_id) {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
        sq.append("INSERT INTO ");
        sq.append(ImportOHDM.getFullTableName(this.targetSchema, ImportOHDM.GEOOBJECT_GEOMETRY));
        sq.append(" (id_geoobject_source, valid_since, valid_until, ");
        
        switch(ohdmElement.getGeometryType()) {
            case POINT: 
                sq.append(" id_point) VALUES (");
                break;
            case LINESTRING: 
                sq.append(" id_line) VALUES (");
                break;
            case POLYGON: 
                sq.append(" id_polygon) VALUES (");
                break;
        }
        
        sq.append(object_id);
        sq.append(", ");
        sq.append("'01-01-1970', "); // since
        sq.append("'01-01-2017', "); // until
        sq.append(geometry_id);
        sq.append(");");
        
        sq.flush();
    }
    
    void addContentAndURL(OHDMElement ohdmElement, int object_id) {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
    }
    
    void updateIntermediateSource(OHDMElement ohdmElement, int object_id, int geometry_id) {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
    }
    
    private boolean elementHasIdentity(OHDMElement ohdmElement) {
        String name = ohdmElement.getName();
        
        // must have a name
        if(name == null || name.length() < 1) return false;
        
        // name must not be a single number
        try {
            Integer.parseInt(name);
            
            // it's a number and only a number
            return false;
        }
        catch(NumberFormatException e) {
            // that's ok - no number.. go ahead
        }
        
        return true;
    }
    
    public boolean importOHDMElement(OHDMElement ohdmElement) {
        ArrayList<TagElement> tags = ohdmElement.getTags();
        
        try {
            /* nodes without tags have no identity and are part of a way or relation
            and stored with them. We are done here and return
            */
            if(!this.elementHasIdentity(ohdmElement)) {
                return false;
            }

            // create user entry or find user primary key
            String externalUserID = ohdmElement.getUserID();
            String externalUsername = ohdmElement.getUsername();

            int id_ExternalUser = this.getOHDM_ID_ExternalUser(externalUserID, externalUsername);

            // create OHDM object
            int object_id = this.addOHDMObject(ohdmElement, id_ExternalUser);

            // create a geoemtry in OHDM
            int geometry_id = this.addGeometry(ohdmElement, id_ExternalUser);

            // create entry in object_geometry table
            addValidity(ohdmElement, object_id, geometry_id);

            // keep some special tags (url etc, see wiki)
            addContentAndURL(ohdmElement, object_id);

            // remind those actions in intermediate database by setting ohdm_id
            updateIntermediateSource(ohdmElement, object_id, geometry_id);
        }
        catch(Exception e) {
            System.err.println("failure during node import: " + e.getMessage());
        }
        
        return true;
    }
    
    @Override
    public boolean importNode(OHDMNode node) {
        return this.importOHDMElement(node);
    }
    
    ////////////////////////////////////////////////////////////////////////
    //                          CREATE STRUCTURES                         //
    ////////////////////////////////////////////////////////////////////////

    // ids are defined identically in each table
    static protected String getCreateTableBegin(String schema, String tableName) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("CREATE TABLE ");
        sb.append(ImportOHDM.getFullTableName(schema, tableName));
        sb.append(" (");
        sb.append(ImportOHDM.getCreatePrimaryKeyDescription(schema, tableName));
        
        return sb.toString();
    }
    
    // primary key are created identically
    static protected String getCreatePrimaryKeyDescription(String schema, String tableName) {
        return "id bigint NOT NULL DEFAULT nextval('"
                + ImportOHDM.getSequenceName(ImportOHDM.getFullTableName(schema, tableName))
                + "'::regclass),"
                + " CONSTRAINT "
                + tableName
                + "_pkey PRIMARY KEY (id)";
    }
    
    static protected void createSequence(Connection targetConnection, String schema, String tableName) {
        SQLStatementQueue sq = new SQLStatementQueue(targetConnection);
        
        sq.append("CREATE SEQUENCE "); 
        sq.append(ImportOHDM.getSequenceName(ImportOHDM.getFullTableName(schema, tableName)));
        sq.append(" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;");
        sq.flush();
    }
    
    static protected void drop(Connection targetConnection, String schema, String tableName) {
        SQLStatementQueue sq = new SQLStatementQueue(targetConnection);
        
        String fullTableName = ImportOHDM.getFullTableName(schema, tableName);
        
        sq.append("DROP SEQUENCE ");
        sq.append(ImportOHDM.getSequenceName(fullTableName));
        sq.append(" CASCADE;");
        sq.flush();
        
        sq.append("DROP TABLE ");
        sq.append(fullTableName);
        sq.append(" CASCADE;");
        sq.flush();
    }
    
    ////////////////////////////////////////////////////////////////////////
    //                                names                               //
    ////////////////////////////////////////////////////////////////////////
    
    static protected String getSequenceName(String tableName) {
        return tableName + "_id ";
    }
    
    static protected String getFullTableName(String schema, String tableName) {
        return schema + "." + tableName;
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
    
    
    void dropOHDMTables(Connection targetConnection) {
        // drop
        ImportOHDM.drop(targetConnection, this.targetSchema, EXTERNAL_SYSTEMS);
        ImportOHDM.drop(targetConnection, this.targetSchema, EXTERNAL_USERS);
        ImportOHDM.drop(targetConnection, this.targetSchema, CLASSIFICATION);
        ImportOHDM.drop(targetConnection, this.targetSchema, CONTENT);
        ImportOHDM.drop(targetConnection, this.targetSchema, GEOOBJECT);
        ImportOHDM.drop(targetConnection, this.targetSchema, GEOOBJECT_CONTENT);
        ImportOHDM.drop(targetConnection, this.targetSchema, GEOOBJECT_GEOMETRY);
        ImportOHDM.drop(targetConnection, this.targetSchema, GEOOBJECT_URL);
        ImportOHDM.drop(targetConnection, this.targetSchema, LINES);
        ImportOHDM.drop(targetConnection, this.targetSchema, POINTS);
        ImportOHDM.drop(targetConnection, this.targetSchema, POLYGONS);
        ImportOHDM.drop(targetConnection, this.targetSchema, URL);
    }
    
    void createOHDMTables(Connection targetConnection) throws SQLException {
        String schema = this.targetSchema;
        
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
        
        // insert osm as external system !!
        sq.append("INSERT INTO ");
        sq.append(ImportOHDM.getFullTableName(schema, ImportOHDM.EXTERNAL_SYSTEMS));
        sq.append(" (name, description) VALUES ('osm', 'Open Street Map');");
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
        
        // fill classification
        OSMClassification.getOSMClassification().write2Table(
                targetConnection, 
                ImportOHDM.getFullTableName(schema, ImportOHDM.CLASSIFICATION)
            );
        
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
        sq.append("type character varying,");
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
        sq.append("polygon geometry,");
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
        sq.append("url character varying,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.flush();
        
    }
    
    public static void main(String args[]) {
        // let's fill OHDM database
        try {
            Connection sourceConnection = Importer.createLocalTestSourceConnection();
            Connection targetConnection = Importer.createLocalTestTargetConnection();
            
            ImportOHDM ohdmImporter = new ImportOHDM(sourceConnection, targetConnection,
                "public", "ohdm");
            
            ohdmImporter.dropOHDMTables(targetConnection);
            ohdmImporter.createOHDMTables(targetConnection);

            ExportIntermediateDB exporter = 
                    new ExportIntermediateDB(sourceConnection, ohdmImporter);
            
            exporter.processNodes();
            exporter.processWays();
            /*
            exporter.processRelations();
            */
            
            System.out.println(exporter.getStatistics());
  
        } catch (SQLException e) {
          System.err.println("cannot connect to database: " + e.getLocalizedMessage());
        }
    }
    
}
