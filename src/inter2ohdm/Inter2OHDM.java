package inter2ohdm;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import osm.OSMClassification;
import util.DB;
import util.SQLStatementQueue;
import util.Parameter;

/**
 * That class imports (and updates) data from intermediate database to OHDM.
 * It changes both, ohdm data and intermediate data.
 * 
 * @author thsc
 */
public class Inter2OHDM extends Importer {
    private final String sourceSchema;
    private final String targetSchema;
    private final IntermediateDB intermediateDB;
    private final SQLStatementQueue sourceQueue;
    private final SQLStatementQueue targetQueue;
    private String defaultSince = "01-01-1970";
    private String defaultUntil = "01-01-2017";
    
    public Inter2OHDM(IntermediateDB intermediateDB, 
            Connection sourceConnection, Connection targetConnection, 
            String sourceSchema, String targetSchema) {
        
        super(sourceConnection, targetConnection);
        
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.intermediateDB = intermediateDB;
        
        this.sourceQueue = new SQLStatementQueue(sourceConnection);
        this.targetQueue = new SQLStatementQueue(targetConnection);
        
        this.defaultSince = "2016-01-01";
        this.defaultUntil = this.getTodayString();
    }
    
    private String getTodayString() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, 0);
        
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");

        String formatted = format1.format(cal.getTime());
        return formatted;
    }

    @Override
    public boolean importWay(OHDMWay way) {
        return (this.importOHDMElement(way) != null);
    }

    /**
     * TODO handle boundary attribute admin-level!!http://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative#admin_level
     * @param relation
     * @return
     * @throws SQLException 
     */
    @Override
    public boolean importRelation(OHDMRelation relation) throws SQLException {
        // debug stop
        if(relation.getOSMIDString().equalsIgnoreCase("1368193")) {
            int i = 42;
        }
        
        String ohdmIDString = null;
        
        if(this.elementHasIdentity(relation)) {
            // save relation with identity.
            ohdmIDString = this.importOHDMElement(relation);
            
        } else {
            // relations without an identity are not imported with 
            // some exceptions
            
            switch(relation.getClassName()) {
                case "building":
                case "landuse":
                    ohdmIDString = this.getOSMDummyObject_OHDM_ID();
            }
        }
        
        if(ohdmIDString == null) return false;
        
        // remember it ohdm incarnation...
        relation.setOHDMObjectID(ohdmIDString);
        
        /* now there are two options:
        a) that relation represents a multigeometry (in most cases)
        b) it represents a polygon with one or more hole(s)
         */
        
        /* status:
        Object is stored but geometry not.
        
        1) Relation is stored as geometry only in two cases:
        
        a) that relation is a polygon but made up by several ways
        b) is a multipolygone with holes
        
        2) Otherwise, relation is stored as geoobject wit relations to 
        other geoobjects.
        */
        
        // handle option 2)
        if(!relation.isPolygon()) {
            return this.saveRelationAsRelatedObjects(relation, ohdmIDString);
        } else {
            if(relation.isMultipolygon()) {
                return this.saveRelationAsMultipolygon(relation);
            } 
        }

        return false;
    }
    
    int getTargetTypeInt(OHDMElement ohdmElement) {
        int targetType = 0;
        switch(ohdmElement.getGeometryType()) {
            case POINT: 
                targetType = Inter2OHDM.TARGET_POINT;
                break;
            case LINESTRING: 
                targetType = Inter2OHDM.TARGET_LINESTRING;
                break;
            case POLYGON: 
                targetType = Inter2OHDM.TARGET_POLYGON;
                break;
        }
        
        return targetType;
    }
    
    @Override
    public boolean importPostProcessing(OHDMElement element) throws SQLException {
        // are there historic names?
        HashMap<String, String> oldNames = element.getOldNameStrings();
        if(oldNames == null) return false;
        
        /* we have a list of pairs like
        yyyy-yyyy oldName1
        yyyy-yyyy oldName2
        yyyy-yyyy oldName3
        whereas oldName1 can be identical to oldName3 or 2 or all can be 
        the same.. in that case, time spans must be combined...
        */

        int targetType = this.getTargetTypeInt(element);
        String classCodeString = element.getClassCodeString(); // just a guess, might have changed over time.
        int externalUserID = this.getOHDM_ID_ExternalUser(element);
        
        // we keep all newly created elements in that map: name / ohdmID
        HashMap<String, String> newOldElements = new HashMap<>();
        for(String oldTimeSpan : oldNames.keySet()) {
            int i = oldTimeSpan.indexOf("-");
            if(i == -1) continue;
            
            String fromYear = oldTimeSpan.substring(0, i) + "-01-01";
            String toYear = oldTimeSpan.substring(i+1) + "-01-01";
            
            String oldName = oldNames.get(oldTimeSpan);
            
            /*
            It not seldom that e.g. a place is renamed for a while
            and get's its name back after a regime change. That happend
            e.g. in Germany between 1933-1945 but also after 1949-1989
            in Eastgermany.
            Thus, maybe that old could have already been used
             */
            
            String newOldOHDMID = newOldElements.get(oldName);

            // if this not already exist..create
            if(newOldOHDMID == null || newOldOHDMID.length() == 0) {
                newOldOHDMID = this.addOHDMObject(oldName, this.getOHDM_ID_ExternalUser(element));
                // remember
                newOldElements.put(oldName, newOldOHDMID);
            }
            
            String targetIDString;
            if(element instanceof OHDMRelation) {
                // relations can have more than one associated geometry
                OHDMRelation relation = (OHDMRelation) element;
                for(i = 0; i < relation.getMemberSize(); i++) {
                    OHDMElement member = relation.getMember(i);
                    
                    targetIDString = member.getOHDMGeomID();
                    this.addValidity(this.targetQueue, targetType, 
                            classCodeString, newOldOHDMID, targetIDString, 
                            externalUserID, fromYear, toYear);
                }
            } else {
                targetIDString = element.getOHDMGeomID();
                if(targetIDString != null && targetIDString.length() > 0) {
                    this.addValidity(this.targetQueue, targetType, 
                            classCodeString, newOldOHDMID, targetIDString, 
                            externalUserID, fromYear, toYear);
                }
            }
            
            
        }
        
        if(newOldElements.isEmpty()) return false;
        
        this.targetQueue.forceExecute();
        
        return true;
    }
    
    private int idExternalSystemOSM = -1;
    private int getOHDM_ID_externalSystemOSM() {
        if(this.idExternalSystemOSM == -1) {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("SELECT id FROM ");
                sb.append(DB.getFullTableName(targetSchema, Inter2OHDM.EXTERNAL_SYSTEMS));
                sb.append(" where name = 'OSM' OR name = 'osm';");
                ResultSet result = 
                        this.executeQueryOnTarget(sb.toString());
                
                result.next();
                this.idExternalSystemOSM = result.getInt(1);

            } catch (SQLException ex) {
                Logger.getLogger(Inter2OHDM.class.getName()).log(Level.SEVERE, null, ex);
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
    
    private int getOHDM_ID_ExternalUser(OHDMElement ohdmElement) {
        // create user entry or find user primary key
        String externalUserID = ohdmElement.getUserID();
        String externalUsername = ohdmElement.getUsername();

        return this.getOHDM_ID_ExternalUser(externalUserID, 
                externalUsername);
    }
    
    private int getOHDM_ID_ExternalUser(String externalUserID, String externalUserName) {
        if(!this.validUserID(externalUserID)) return Inter2OHDM.UNKNOWN_USER_ID;
        
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
            sb.append(DB.getFullTableName(this.targetSchema, Inter2OHDM.EXTERNAL_USERS));
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
                s.append(DB.getFullTableName(this.targetSchema, Inter2OHDM.EXTERNAL_USERS));
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
    
    String getOHDMObject(OHDMElement ohdmElement, boolean persist) throws SQLException {
        // already in OHDM DB?
        String ohdmIDString = ohdmElement.getOHDMObjectID();
        if(ohdmIDString != null) return ohdmIDString;
        
        // add entry in object table
        try {
            /* nodes without tags have no identity and are part of a way or relation
            and stored with them. We are done here and return
            */
            if(!this.elementHasIdentity(ohdmElement)) {
                /*
                Anonymous way which are *not* part of a relation are
                refered to the general dummy OHDM object
                */
                if(ohdmElement instanceof OHDMWay && !ohdmElement.isPart()) {
                    ohdmIDString = this.getOSMDummyObject_OHDM_ID();
                    ohdmElement.setOHDMObjectID(ohdmIDString);
                    return ohdmIDString;
                } else {
                    return null;
                }
            } else {
                // create user entry or find user primary key
                String externalUserID = ohdmElement.getUserID();
                String externalUsername = ohdmElement.getUsername();

                int id_ExternalUser = this.getOHDM_ID_ExternalUser(externalUserID, 
                        externalUsername);

                ohdmIDString =  this.addOHDMObject(ohdmElement, id_ExternalUser);
            }
        
            return ohdmIDString;
        }
        catch(Exception e) {
            System.err.println("failure during node import: " + e.getMessage());
        }
        
        return null;
    }
    
    String addOHDMObject(OHDMElement ohdmElement, int externalUserID) throws SQLException {
        // already in OHDM DB?
        String ohdmIDString = ohdmElement.getOHDMObjectID();
        if(ohdmIDString != null) return ohdmIDString;
        
//        SQLStatementQueue targetQueue = new SQLStatementQueue(this.targetConnection);
        
        String name = ohdmElement.getName();
        
        return this.addOHDMObject(name, externalUserID);
    }
    
    String addOHDMObject(String name, int externalUserID) throws SQLException {
        SQLStatementQueue sql = new SQLStatementQueue(this.targetConnection);
        sql.append("INSERT INTO ");
        sql.append(DB.getFullTableName(this.targetSchema, Inter2OHDM.GEOOBJECT));
        sql.append(" (name, source_user_id) VALUES ('");
        sql.append(name);
        sql.append("', ");
        sql.append(externalUserID);
        sql.append(") RETURNING id;");
        
        ResultSet result = sql.executeWithResult();
        result.next();
        return result.getBigDecimal(1).toString();
    }
    
    String addGeometry(OHDMElement ohdmElement, int externalUserID) throws SQLException {
        String wkt = ohdmElement.getWKTGeometry();
        if(wkt == null || wkt.length() < 1) return null;
        
//        SQLStatementQueue targetQueue = new SQLStatementQueue(this.targetConnection);
        
        targetQueue.append("INSERT INTO ");
        
        String fullTableName;
        
        switch(ohdmElement.getGeometryType()) {
            case POINT: 
                fullTableName = DB.getFullTableName(this.targetSchema, Inter2OHDM.POINTS);
                targetQueue.append(fullTableName);
                targetQueue.append(" (point, ");
                break;
            case LINESTRING: 
                fullTableName = DB.getFullTableName(this.targetSchema, Inter2OHDM.LINES);
                targetQueue.append(fullTableName);
                targetQueue.append(" (line, ");
                break;
            case POLYGON: 
                fullTableName = DB.getFullTableName(this.targetSchema, Inter2OHDM.POLYGONS);
                targetQueue.append(fullTableName);
                targetQueue.append(" (polygon, ");
                break;
        }
        
        targetQueue.append(" source_user_id) VALUES ('");
        
        targetQueue.append(wkt);
        targetQueue.append("', ");
        targetQueue.append(externalUserID);
        targetQueue.append(") RETURNING id;");

        try {
            ResultSet result = targetQueue.executeWithResult();
            result.next();
            String geomIDString = result.getBigDecimal(1).toString();
            ohdmElement.setOHDMGeometryID(geomIDString);
            return geomIDString;
        }
        catch(SQLException e) {
            System.err.println("failure when inserting geometry, wkt:\n" + wkt + "\nosm_id: " + ohdmElement.getOSMIDString());
            throw e;
        }
    }
    
    

    void addValidity(OHDMElement ohdmElement, String ohdmIDString, String ohdmGeomIDString, int externalUserID) throws SQLException {
        // what table is reference by id_geometry
        int targetType = 0;
        switch(ohdmElement.getGeometryType()) {
            case POINT: 
                targetType = Inter2OHDM.TARGET_POINT;
                break;
            case LINESTRING: 
                targetType = Inter2OHDM.TARGET_LINESTRING;
                break;
            case POLYGON: 
                targetType = Inter2OHDM.TARGET_POLYGON;
                break;
        }
        
//        SQLStatementQueue targetQueue = new SQLStatementQueue(this.targetConnection);
        this.addValidity(targetQueue, ohdmElement, targetType, ohdmElement.getClassCodeString(), ohdmIDString, ohdmGeomIDString, externalUserID);
        targetQueue.forceExecute();
    }
    
    private String formatDateString(String sinceValue) {
        // assume we have only got the year
        if(sinceValue.length() == 4) {
            return sinceValue + "-01-01";
        } 
        
        // TODO more..
        
        return null;
    }
    
    void addValidity(SQLStatementQueue sq, OHDMElement ohdmElement, int targetType, 
            String classCodeString, String sourceIDString, 
            String targetIDString, int externalUserID) throws SQLException {
        
        String sinceString = null;
        // is there since tag in osm origin?
        String sinceValue = ohdmElement.getValue("since");
        if(sinceValue != null) {
            sinceString = this.formatDateString(sinceValue);
        }
        
        if(sinceString == null) {
            sinceString = this.defaultSince;
        }
        
        this.addValidity(sq, targetType, classCodeString, sourceIDString, targetIDString, externalUserID, sinceString, this.defaultUntil);
    }
    
    void addValidity(SQLStatementQueue sq, int targetType, 
            String classCodeString, String sourceIDString, 
            String targetIDString, int externalUserID, String sinceString, 
            String untilString) throws SQLException {
        
        if(sourceIDString == null) {
            // failure
            System.err.println("source id must not be null");
        }
        
        sq.append("INSERT INTO ");
        sq.append(DB.getFullTableName(this.targetSchema, Inter2OHDM.GEOOBJECT_GEOMETRY));
        sq.append(" (type_target, classification_id, id_geoobject_source, id_target, valid_since, valid_until, source_user_id) VALUES (");

        sq.append(targetType);
        sq.append(", ");
        sq.append(classCodeString);
        sq.append(", ");
        sq.append(sourceIDString);
        sq.append(", ");
        sq.append(targetIDString);
        sq.append(", '");
        sq.append(sinceString);
        sq.append("', '"); 
        sq.append(untilString);
        sq.append("', "); // until
        sq.append(externalUserID);
        sq.append(");");
    }
    
    void addContentAndURL(OHDMElement ohdmElement, String ohdmIDString) {
//        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
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
        String osmID = ohdmElement.getOSMIDString();
        if(osmID.equalsIgnoreCase("188276804") || osmID.equalsIgnoreCase("301835391")) {
            // debug break
            int i = 42;
        }
        
//        ArrayList<TagElement> tags = ohdmElement.getTags();
        
        try {
            // get external user id from ohdm
            int id_ExternalUser = this.getOHDM_ID_ExternalUser(ohdmElement);

            /* create a geomtry in OHDM 
                this call fails (produces null) if this element has no geometry,
                which is a relation that has not only inner / outer member
            */
            String ohdmGeomIDString = this.addGeometry(ohdmElement, id_ExternalUser);
            
            boolean persist = ohdmGeomIDString == null;
//            boolean persist = true;
            
            /* add entry in object table IF this object has an identity
                perist that object ONLY IF there is no geometry. Reduces db access!
            */
            String ohdmObjectIDString = this.getOHDMObject(ohdmElement, persist);
            

            // refer object and geometry to each other
            if(ohdmGeomIDString != null && ohdmObjectIDString != null) {
                // create entry in object_geometry table
                addValidity(ohdmElement, ohdmObjectIDString, ohdmGeomIDString, 
                        id_ExternalUser);
                
                /* now make both object and geom id persistent to intermediate db
                */
                this.intermediateDB.setOHDM_IDs(ohdmElement, ohdmObjectIDString, ohdmGeomIDString);
            }

            // keep some special tags (url etc, see wiki)
            addContentAndURL(ohdmElement, ohdmObjectIDString);
            
            // are there even historical information stored

            return ohdmObjectIDString;
        }
        catch(Exception e) {
            System.err.println("failure during import of intermediate object: " + e.getMessage());
        }
        
        return null;
    }
    
    @Override
    public boolean importNode(OHDMNode node) {
        if(!this.elementHasIdentity(node)) {
            // nodes without an identity are not imported.
            return false;
        }

        return (this.importOHDMElement(node) != null);
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
    public static int TARGET_POINT = 1;
    public static int TARGET_LINESTRING = 2;
    public static int TARGET_POLYGON = 3;
    public static int TARGET_GEOOBJECT = 0;
    
    void dropOHDMTables(Connection targetConnection) throws SQLException {
        // drop
        DB.drop(targetConnection, this.targetSchema, EXTERNAL_SYSTEMS);
        DB.drop(targetConnection, this.targetSchema, EXTERNAL_USERS);
        DB.drop(targetConnection, this.targetSchema, CLASSIFICATION);
        DB.drop(targetConnection, this.targetSchema, CONTENT);
        DB.drop(targetConnection, this.targetSchema, GEOOBJECT);
        DB.drop(targetConnection, this.targetSchema, GEOOBJECT_CONTENT);
        DB.drop(targetConnection, this.targetSchema, GEOOBJECT_GEOMETRY);
        DB.drop(targetConnection, this.targetSchema, GEOOBJECT_URL);
        DB.drop(targetConnection, this.targetSchema, LINES);
        DB.drop(targetConnection, this.targetSchema, POINTS);
        DB.drop(targetConnection, this.targetSchema, POLYGONS);
        DB.drop(targetConnection, this.targetSchema, URL);
    }
    
    void createOHDMTables(Connection targetConnection) throws SQLException {
        String schema = this.targetSchema;
        
        SQLStatementQueue sq;
        
        // EXTERNAL SYSTEMS
        // sequence
        DB.createSequence(targetConnection, schema, EXTERNAL_SYSTEMS);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.EXTERNAL_SYSTEMS));
        // add table specifics
        sq.append(",");
        sq.append("name character varying,");
        sq.append("description character varying");
        sq.append(");");
        sq.forceExecute();
        
        // insert osm as external system !!
        sq.append("INSERT INTO ");
        sq.append(DB.getFullTableName(schema, Inter2OHDM.EXTERNAL_SYSTEMS));
        sq.append(" (name, description) VALUES ('osm', 'Open Street Map');");
        sq.forceExecute();
        
        // EXTERNAL_USERS
        // sequence
        DB.createSequence(targetConnection, schema, EXTERNAL_USERS);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.EXTERNAL_USERS));
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
        DB.createSequence(targetConnection, schema, CLASSIFICATION);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.CLASSIFICATION));
        // add table specifics:
        sq.append(",");
        sq.append("class character varying,");
        sq.append("subclassname character varying");
        sq.append(");");
        sq.forceExecute();
        
        // fill classification
        OSMClassification.getOSMClassification().write2Table(targetConnection, 
                DB.getFullTableName(schema, Inter2OHDM.CLASSIFICATION)
            );
        
        // CONTENT
        // sequence
        DB.createSequence(targetConnection, schema, CONTENT);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.CONTENT));
        // add table specifics:
        sq.append(",");
        sq.append("name character varying,");
        sq.append("value bytea NOT NULL,");
        sq.append("mimetype character varying");
        sq.append(");");
        sq.forceExecute();
        
        // GEOOBJECT
        // sequence
        DB.createSequence(targetConnection, schema, GEOOBJECT);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.GEOOBJECT));
        // add table specifics:
        sq.append(",");
        sq.append("name character varying,");
        sq.append("source_user_id bigint NOT NULL");
        sq.append(");");
        sq.forceExecute();
        
        // insert osm dummy object.. it has no name.. thats important
        sq.append("INSERT INTO ");
        sq.append(DB.getFullTableName(schema, Inter2OHDM.GEOOBJECT));
        sq.append("(id, source_user_id) VALUES (0, 1);");
        sq.forceExecute();
        
        // GEOOBJECT_CONTENT
        // sequence
        DB.createSequence(targetConnection, schema, GEOOBJECT_CONTENT);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.GEOOBJECT_CONTENT));
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
        DB.createSequence(targetConnection, schema, GEOOBJECT_GEOMETRY);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.GEOOBJECT_GEOMETRY));
        // add table specifics:
        sq.append(",");
        sq.append("id_target bigint,");
        sq.append("type_target bigint,");
        sq.append("id_geoobject_source bigint NOT NULL,");
        sq.append("role character varying,");
        sq.append("classification_id bigint NOT NULL,");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("valid_since_offset bigint DEFAULT 0,");
        sq.append("valid_until_offset bigint DEFAULT 0,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.forceExecute();
        
        // GEOOBJECT_URL
        // sequence
        DB.createSequence(targetConnection, schema, GEOOBJECT_URL);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.GEOOBJECT_URL));
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
        DB.createSequence(targetConnection, schema, LINES);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.LINES));
        // add table specifics:
        sq.append(",");
        sq.append("line geometry,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.forceExecute();
        
        // POINTS
        // sequence
        DB.createSequence(targetConnection, schema, POINTS);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.POINTS));
        // add table specifics:
        sq.append(",");
        sq.append("point geometry,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.forceExecute();
        
        // POLYGONS
        // sequence
        DB.createSequence(targetConnection, schema, POLYGONS);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.POLYGONS));
        // add table specifics:
        sq.append(",");
        sq.append("polygon geometry,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.forceExecute();
        
        // URL
        // sequence
        DB.createSequence(targetConnection, schema, URL);
        // table
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, Inter2OHDM.URL));
        // add table specifics:
        sq.append(",");
        sq.append("url character varying,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.forceExecute();
        
    }
    
    void forgetPreviousImport() throws SQLException {
        SQLStatementQueue sql = new SQLStatementQueue(this.sourceConnection);

        System.out.println("remove all ohdm entries from intermediate db - a reset");
        System.out.println("reset nodes");
        sql.append("UPDATE ");
        sql.append(DB.getFullTableName(this.sourceSchema, "nodes"));
        sql.append(" SET ohdm_geom_id=null, ohdm_object_id=null;");
        sql.forceExecute();
        
        System.out.println("reset ways");
        sql.append("UPDATE ");
        sql.append(DB.getFullTableName(this.sourceSchema, "ways"));
        sql.append(" SET ohdm_geom_id=null, ohdm_object_id=null;");
        sql.forceExecute();
        
        System.out.println("reset relations");
        sql.append("UPDATE ");
        sql.append(DB.getFullTableName(this.sourceSchema, "relations"));
        sql.append(" SET ohdm_geom_id=null, ohdm_object_id=null;");
        sql.forceExecute();
    }
    
    public static void main(String args[]) throws IOException {
        // let's fill OHDM database
        System.out.println("Start importing ODHM data from intermediate DB");
        
        try {
            String sourceParameterFileName = "db_inter.txt";
            String targetParameterFileName = "db_ohdm.txt";
            
            if(args.length > 0) {
                sourceParameterFileName = args[0];
            }
            
            if(args.length > 1) {
                targetParameterFileName = args[1];
            }
            
//            Connection sourceConnection = Importer.createLocalTestSourceConnection();
//            Connection targetConnection = Importer.createLocalTestTargetConnection();
            Parameter sourceParameter = new Parameter(sourceParameterFileName);
            Parameter targetParameter = new Parameter(targetParameterFileName);
            
            Connection sourceConnection = Importer.createConnection(sourceParameter);
            Connection targetConnection = Importer.createConnection(targetParameter);
            
            IntermediateDB iDB = new IntermediateDB(sourceConnection, sourceParameter.getSchema());
            
            String sourceSchema = sourceParameter.getSchema();
            String targetSchema = targetParameter.getSchema();
            
            Inter2OHDM ohdmImporter = new Inter2OHDM(iDB, sourceConnection, 
                    targetConnection, sourceSchema, targetSchema);
            
            try {
                ohdmImporter.forgetPreviousImport();
                ohdmImporter.dropOHDMTables(targetConnection);
            }
            catch(Exception e) {
                System.err.println("problems during setting old data (non-fatal): " + e.getLocalizedMessage());
            }
            
            ohdmImporter.createOHDMTables(targetConnection);
            
            String stepLenString = sourceParameter.getReadStepLen();
            int stepLen = 10000;
            try {
                if(stepLenString != null) {
                    stepLen = Integer.parseInt(stepLenString);
                }
            }
            catch(NumberFormatException e) {
                    // ignore and work with default
            }

            ExportIntermediateDB exporter = 
                    new ExportIntermediateDB(sourceConnection, sourceSchema, ohdmImporter, stepLen);
            
            exporter.processNodes();
            exporter.processWays();
            exporter.processRelations();
            
            System.out.println(exporter.getStatistics());
  
        } catch (Exception e) {
            System.err.println("fatal: " + e.getLocalizedMessage());
            e.printStackTrace(System.err);
        }
    }

    private final String osmDummyObjectOHDM_ID = "0";
    
    private String getOSMDummyObject_OHDM_ID() {
        return this.osmDummyObjectOHDM_ID;
    }

    private boolean saveRelationAsRelatedObjects(OHDMRelation relation, 
            String ohdmIDString) throws SQLException {
        
        // get all ohdm ids and store it
        StringBuilder sq = new StringBuilder();

        /**
         * INSERT INTO [geoobject_geometry] 
         * (id_geoobject_source, id_target, type_target, valid_since, 
         * valid_until VALUES (..)
         */

        sq.append("INSERT INTO ");
        sq.append(Inter2OHDM.GEOOBJECT_GEOMETRY);
        sq.append("(id_geoobject_source, id_target, type_target, role,");
        sq.append(" valid_since, valid_until) VALUES ");

        boolean notFirstSet = false;
        for(int i = 0; i < relation.getMemberSize(); i++) {
            OHDMElement member = relation.getMember(i);
            String memberOHDMObjectIDString = this.getOHDMObject(member, true);
            if(memberOHDMObjectIDString == null) continue; // no identity

            // get role of that member in that relation
            String roleName = relation.getRoleName(i);

            if(notFirstSet) {
                sq.append(", ");
            } else {
                notFirstSet = true;
            }

            sq.append("(");
            sq.append(ohdmIDString); // id source
            sq.append(", ");
            sq.append(memberOHDMObjectIDString); // id target
            sq.append(", ");
            if(member instanceof OHDMNode) { // type_target
                sq.append(Inter2OHDM.TARGET_POINT);
            } else if(member instanceof OHDMWay) {
                sq.append(Inter2OHDM.TARGET_LINESTRING);
            } else {
                sq.append(Inter2OHDM.TARGET_GEOOBJECT);
            }
            sq.append(", ");
            sq.append(roleName); // role
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
        return false;
    }

    private boolean saveRelationAsMultipolygon(OHDMRelation relation) throws SQLException {
        /* sometimes (actually quite often) relations contain only an inner
        and outer member but inner comes first which is not compatible with
        definition of a multipolygon.. We correct that problem here
        */
        
        /* 
            if a multipolygon relation has only two member, inner and outer,
            bring them into right order.
        */
        if(!relation.checkMultipolygonMemberOrder()) return false;
        
        // debugging stop
        if(relation.getOSMIDString().equalsIgnoreCase("3323433")) {
            int i = 42;
        }
        
        // option b) it is a polygone or probably a multipolygon
        ArrayList<String> polygonIDs = new ArrayList<>();
        ArrayList<String> polygonWKT = new ArrayList<>();
        
        if(!relation.fillRelatedGeometries(polygonIDs, polygonWKT)) return false;

        /* we have two list with either references to existing
         geometries or to string representing geometries which are 
        to be stored and referenced.
        */
//        SQLStatementQueue targetQueue = new SQLStatementQueue(this.targetConnection);
        for(int i = 0; i < polygonIDs.size(); i++) {
            String pID = polygonIDs.get(i);
            if(pID.equalsIgnoreCase("-1")) {
                // this geometry is not yet in the database.. insert that polygon
                targetQueue.append("INSERT INTO ");
                targetQueue.append(DB.getFullTableName(this.targetSchema, Inter2OHDM.POLYGONS));
                targetQueue.append(" (polygon, source_user_id) VALUES ('");
                targetQueue.append(polygonWKT.get(i));
                targetQueue.append("', ");
                int ohdmUserID = this.getOHDM_ID_ExternalUser(relation);
                targetQueue.append(ohdmUserID);
                targetQueue.append(") RETURNING ID;");
                
//                sq.print("saving polygon wkt");
                try {
                    ResultSet polygonInsertResult = targetQueue.executeWithResult();
                    polygonInsertResult.next();
                    String geomIDString = polygonInsertResult.getBigDecimal(1).toString();
                    polygonIDs.set(i, geomIDString);
                }
                catch(SQLException e) {
                    System.err.println("sql failed: " + targetQueue.toString());
                    throw e;
                }
            }
        }

        if(polygonIDs.size() < 1) return false;
        
        // add relations
        int targetType = Inter2OHDM.TARGET_POLYGON; // all targets are polygons
        String classCodeString = relation.getClassCodeString();
        String sourceIDString = relation.getOHDMObjectID();
        if(sourceIDString == null) {
            // debug stop
            int i = 42;
        }
        int externalUserID = this.getOHDM_ID_ExternalUser(relation);
        
        // void addValidity(int targetType, String classCodeString, String sourceIDString, String targetIDString, int externalUserID) throws SQLException {
        for(String targetIDString : polygonIDs) {
            this.addValidity(targetQueue, relation, targetType, classCodeString, sourceIDString, targetIDString, externalUserID);
        }
        targetQueue.forceExecute();
        return true;
    }
}
