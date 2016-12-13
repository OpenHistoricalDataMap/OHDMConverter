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
    private final IntermediateDB intermediateDB;
    
    public ImportOHDM(IntermediateDB intermediateDB, 
            Connection sourceConnection, Connection targetConnection, 
            String sourceSchema, String targetSchema) {
        
        super(sourceConnection, targetConnection);
        
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.intermediateDB = intermediateDB;
    }

    @Override
    public boolean importWay(OHDMWay way) {
        return (this.importOHDMElement(way) != null);
    }

    @Override
    public boolean importRelation(OHDMRelation relation) throws SQLException {
        /* there are two options:
        a) that relation represents a multigeometry (in most cases)
        b) it represents a polygon with one or more hole(s)
         */
        
        // in either case.. create an ohdm object
        String ohdmIDString = this.importOHDMElement(relation);
        
        if(ohdmIDString == null) return false; // object has not be written
        
        /* previous message has already stored geometry
          option b) is already handled so far
        */
        
        // handle option a)
        if(!relation.isPolygon()) {
            // get all ohdm ids and store it
            StringBuilder sq = new StringBuilder();
            
            /**
             * INSERT INTO [geoobject_geometry] 
             * (id_geoobject_source, id_target, type_target, valid_since, 
             * valid_until VALUES (..)
             */
            
            sq.append("INSERT INTO ");
            sq.append(ImportOHDM.GEOOBJECT_GEOMETRY);
            sq.append("(id_geoobject_source, id_target, type_target, valid_since,");
            sq.append(" valid_until VALUES ");
            
            boolean notFirstSet = false;
            for(int i = 0; i < relation.getMemberSize(); i++) {
                OHDMElement member = relation.getMember(i);
                String memberOHDMObjectIDString = member.getOHDMObjectID();
                if(memberOHDMObjectIDString == null) continue; // // TODO: member not yet in ohdm db.
                
                if(notFirstSet) {
                    sq.append(", ");
                }
                
                sq.append("(");
                sq.append(ohdmIDString); // id source
                sq.append(", ");
                sq.append(memberOHDMObjectIDString); // id target
                sq.append(", ");
                if(member instanceof OHDMNode) { // type_target
                    sq.append(ImportOHDM.TARGET_POINT);
                } else if(member instanceof OHDMWay) {
                    sq.append(ImportOHDM.TARGET_LINESTRING);
                } else {
                    sq.append(ImportOHDM.TARGET_GEOOBJECT);
                }
                sq.append(", ");
                sq.append(this.defaultSince); // since
                sq.append(", ");
                sq.append(this.defaultUntil); // until
                sq.append(")"); // end that value set
            }
            sq.append(";"); // end that value set
            
            if(notFirstSet) {
                // there is at least one value set - excecute
                SQLStatementQueue sql = new SQLStatementQueue(this.targetConnection);
                sql.exec(sq.toString());
                return true;
            }
        }

        return false;
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
    
    String addOHDMObject(OHDMElement ohdmElement, int externalUserID) throws SQLException {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
        String name = ohdmElement.getName();
        String classIDString = ohdmElement.getClassCodeString();
        
        sq.append("INSERT INTO ");
        sq.append(ImportOHDM.getFullTableName(this.targetSchema, ImportOHDM.GEOOBJECT));
        sq.append(" (name, classification_id, source_user_id) VALUES ('");
        sq.append(name);
        sq.append("', ");
        sq.append(classIDString);
        sq.append(", ");
        sq.append(externalUserID);
        sq.append(") RETURNING id;");
        
        ResultSet result = sq.executeWithResult();
        result.next();
        return result.getBigDecimal(1).toString();
    }
    
    String addGeometry(OHDMElement ohdmElement, int externalUserID) throws SQLException {
        String wkt = ohdmElement.getWKTGeometry();
        if(wkt == null || wkt.length() < 1) return null;
        
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
        
        sq.append(wkt);
        sq.append("', ");
        sq.append(externalUserID);
        sq.append(") RETURNING id;");

        try {
            ResultSet result = sq.executeWithResult();
            result.next();
            return result.getBigDecimal(1).toString();
        }
        catch(SQLException e) {
            System.err.println("failure when inserting geometry, wkt:\n" + wkt + "\nosm_id: " + ohdmElement.getOSMIDString());
            throw e;
        }
    }

    private final String defaultSince = "01-01-1970";
    private final String defaultUntil = "01-01-2017";
            
    void addValidity(OHDMElement ohdmElement, String ohdmIDString, String ohdmGeomIDString, int externalUserID) throws SQLException {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
        
        sq.append("INSERT INTO ");
        sq.append(ImportOHDM.getFullTableName(this.targetSchema, ImportOHDM.GEOOBJECT_GEOMETRY));
        sq.append(" (type_target, id_geoobject_source, id_target, valid_since, valid_until, source_user_id) VALUES (");

        // what table is reference by id_geometry
        switch(ohdmElement.getGeometryType()) {
            case POINT: 
                sq.append(ImportOHDM.TARGET_POINT);
                break;
            case LINESTRING: 
                sq.append(ImportOHDM.TARGET_LINESTRING);
                break;
            case POLYGON: 
                sq.append(ImportOHDM.TARGET_POLYGON);
                break;
        }
        
        sq.append(", ");
        sq.append(ohdmIDString);
        sq.append(", ");
        sq.append(ohdmGeomIDString);
        sq.append(", '");
        sq.append(this.defaultSince);
        sq.append("', '"); 
        sq.append(this.defaultUntil);
        sq.append("', "); // until
        sq.append(externalUserID);
        sq.append(");");
        
        sq.forceExecute();
    }
    
    void addContentAndURL(OHDMElement ohdmElement, String ohdmIDString) {
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
    
    /**
     * 
     * @param ohdmElement
     * @return ohdm_id as string
     */
    public String importOHDMElement(OHDMElement ohdmElement) {
        ArrayList<TagElement> tags = ohdmElement.getTags();
        
        try {
            /* nodes without tags have no identity and are part of a way or relation
            and stored with them. We are done here and return
            */
            if(!this.elementHasIdentity(ohdmElement)) {
                return null;
            }

            // create user entry or find user primary key
            String externalUserID = ohdmElement.getUserID();
            String externalUsername = ohdmElement.getUsername();

            int id_ExternalUser = this.getOHDM_ID_ExternalUser(externalUserID, 
                    externalUsername);

            // create OHDM object
            String ohdmObjectIDString = this.addOHDMObject(ohdmElement, id_ExternalUser);

            // create a geoemtry in OHDM
            String ohdmGeomIDString = this.addGeometry(ohdmElement, id_ExternalUser);

            if(ohdmGeomIDString != null && ohdmObjectIDString != null) {
                // create entry in object_geometry table
                addValidity(ohdmElement, ohdmObjectIDString, ohdmGeomIDString, 
                        id_ExternalUser);
            }

            // keep some special tags (url etc, see wiki)
            addContentAndURL(ohdmElement, ohdmObjectIDString);

            // remind those actions in intermediate database by setting ohdm_geom_id
            this.intermediateDB.setOHDM_IDs(ohdmElement, ohdmObjectIDString, ohdmGeomIDString);
        
            return ohdmObjectIDString;
        }
        catch(Exception e) {
            System.err.println("failure during node import: " + e.getMessage());
        }
        
        return null;
    }
    
    @Override
    public boolean importNode(OHDMNode node) {
        return (this.importOHDMElement(node) != null);
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
    
    static protected void createSequence(Connection targetConnection, String schema, String tableName) throws SQLException {
        SQLStatementQueue sq = new SQLStatementQueue(targetConnection);
        
        sq.append("CREATE SEQUENCE "); 
        sq.append(ImportOHDM.getSequenceName(ImportOHDM.getFullTableName(schema, tableName)));
        sq.append(" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;");
        sq.forceExecute();
    }
    
    static protected void drop(Connection targetConnection, String schema, String tableName) throws SQLException {
        SQLStatementQueue sq = new SQLStatementQueue(targetConnection);
        
        String fullTableName = ImportOHDM.getFullTableName(schema, tableName);
        
        sq.append("DROP SEQUENCE ");
        sq.append(ImportOHDM.getSequenceName(fullTableName));
        sq.append(" CASCADE;");
        sq.forceExecute();
        
        sq.append("DROP TABLE ");
        sq.append(fullTableName);
        sq.append(" CASCADE;");
        sq.forceExecute();
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
    
    // Geometry Types 
    static int TARGET_POINT = 1;
    static int TARGET_LINESTRING = 2;
    static int TARGET_POLYGON = 3;
    static int TARGET_GEOOBJECT = 0;
    
    void dropOHDMTables(Connection targetConnection) throws SQLException {
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
        sq.forceExecute();
        
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
        sq.forceExecute();
        
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
        sq.forceExecute();
        
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
        sq.forceExecute();
        
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
        sq.forceExecute();
        
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
        sq.forceExecute();
        
        // GEOOBJECT_GEOMETRY
        // sequence
        ImportOHDM.createSequence(targetConnection, schema, GEOOBJECT_GEOMETRY);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(ImportOHDM.getCreateTableBegin(schema, ImportOHDM.GEOOBJECT_GEOMETRY));
        // add table specifics:
        sq.append(",");
        sq.append("id_target bigint,");
        sq.append("type_target bigint,");
        sq.append("id_geoobject_source bigint NOT NULL,");
        sq.append("role character varying,");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("valid_since_offset bigint DEFAULT 0,");
        sq.append("valid_until_offset bigint DEFAULT 0,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.forceExecute();
        
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
        sq.forceExecute();
        
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
        sq.forceExecute();
        
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
        sq.forceExecute();
        
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
        sq.forceExecute();
        
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
        sq.forceExecute();
        
    }
    
    public static void main(String args[]) {
        // let's fill OHDM database
        try {
            Connection sourceConnection = Importer.createLocalTestSourceConnection();
            Connection targetConnection = Importer.createLocalTestTargetConnection();
            
            IntermediateDB iDB = new IntermediateDB(sourceConnection);
            
            ImportOHDM ohdmImporter = new ImportOHDM(iDB, sourceConnection, 
                    targetConnection, "public", "ohdm");
            
            ohdmImporter.dropOHDMTables(targetConnection);
            ohdmImporter.createOHDMTables(targetConnection);

            ExportIntermediateDB exporter = 
                    new ExportIntermediateDB(sourceConnection, ohdmImporter);
            
            exporter.processNodes();
            exporter.processWays();
            exporter.processRelations();
            
            System.out.println(exporter.getStatistics());
  
        } catch (SQLException e) {
          System.err.println("cannot connect to database: " + e.getLocalizedMessage());
        }
    }
    
}
