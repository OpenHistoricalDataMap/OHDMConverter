package inter2ohdm;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import util.DB;
import util.OHDM_DB;
import util.FileSQLStatementQueue;
import util.InterDB;
import util.SQLStatementQueue;
import util.Parameter;
import util.Trigger;
import util.Util;

/**
 * That class imports (and updates) data from intermediate database to OHDM.
 * It changes both, ohdm data and intermediate data.
 * 
 * @author thsc
 */
public class OHDMImporter extends Importer {
    private final String sourceSchema;
    private final String targetSchema;
    private final IntermediateDB intermediateDB;
    private final SQLStatementQueue sourceUpdateQueue;
    private final SQLStatementQueue targetSelectQueue;
    private final SQLStatementQueue targetInsertQueue;


    private String defaultSince = "1970-01-01";
    private String defaultUntil = "2017-01-01";
    
    public OHDMImporter(IntermediateDB intermediateDB, String validUntilString,
            Connection sourceConnection, Connection targetConnection, 
            String sourceSchema, String targetSchema, SQLStatementQueue updateQueue) {
        
        super(sourceConnection, targetConnection);
        
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.intermediateDB = intermediateDB;
        
//        this.sourceUpdateQueue = new SQLStatementQueue(sourceConnection);
        this.sourceUpdateQueue = updateQueue;
        
        this.targetSelectQueue = new SQLStatementQueue(targetConnection);
        this.targetInsertQueue = new SQLStatementQueue(targetConnection);
        
        this.defaultSince = "2016-01-01";
        this.defaultUntil = validUntilString;
    }
    
    void close() throws SQLException {
        this.targetInsertQueue.forceExecute();
        this.targetInsertQueue.close();
    }
    
    void forceExecute() throws SQLException {
        this.sourceUpdateQueue.forceExecute();
        this.targetInsertQueue.forceExecute();
    }
    
    private String getTodayString() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, 0);
        
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");

        String formatted = format1.format(cal.getTime());
        return formatted;
    }

    @Override
    public boolean importNode(OSMNode node, boolean importUnnamedEntities) {
        if(!this.elementHasIdentity(node) && !importUnnamedEntities) {
            // nodes without an identity are not imported.
            return false;
        }

        return (this.importOSMElement(node, importUnnamedEntities, false) != null);
    }
     
    @Override
    public boolean importWay(OSMWay way, boolean importUnnamedEntities) {
        if(this.importOSMElement(way, importUnnamedEntities, false) == null) {
            return false; // failure
        }
        
        List<OSMElement> iNodesList = way.getNodesWithIdentity();
        if(iNodesList == null || iNodesList.isEmpty()) return true; // ready
        
        // imported.. fill subsequent table
//        SQLStatementQueue sql = new SQLStatementQueue(this.targetConnection);

        try {
            this.saveSubsequentObjects(this.targetInsertQueue, way.getOHDMObjectID(), 
                    OHDM_DB.POINT, iNodesList.iterator());
        
            this.targetInsertQueue.couldExecute();
        }
        catch(SQLException se) {
            Util.printExceptionMessage(se, this.targetInsertQueue, "writing to subsequent table failed", false);
        }
        
        return true;
    }
    
    private void saveSubsequentObjects(SQLStatementQueue sql, String targetObjectID, int sourceType, 
            Iterator<OSMElement> eIter) throws SQLException {
        
        if(sourceType != OHDM_DB.POINT && sourceType != OHDM_DB.LINESTRING) {
            throw new SQLException("subsequent table only keeps point and ways");
        }
        
        if(targetObjectID == null || targetObjectID.isEmpty()) {
            throw new SQLException("targetObjectID must not be null or empty when inserting into subsequent table");
        }
        
        /*
        INSERT INTO ohdm.subsequent_geom_user(target_id, point_id, line_id, polygon_id)
        */
        sql.append("INSERT INTO ");
        sql.append(DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_SUBSEQUENT_GEOM_USER));
        sql.append(" (target_id, ");
        switch(sourceType) {
            case OHDM_DB.POINT:
                sql.append("point_id");
                break;
            case OHDM_DB.LINESTRING:
                sql.append("line_id");
                break;
        }
        sql.append(") VALUES ");
        
        boolean first = true;
        while(eIter.hasNext()) {
            OSMElement e = eIter.next();
            
            if(first) {
                first = false;
            } else {
                sql.append(",");
            }
            
            sql.append("(");
            sql.append(targetObjectID);
            sql.append(",");
            sql.append(e.getOHDMObjectID());
            sql.append(")");
        }
        sql.append("; ");
    }

    /**
     * TODO handle boundary attribute admin-level!!http://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative#admin_level
     * @param relation
     * @param importUnnamedEntities
     * @return
     * @throws SQLException 
     */
    @Override
    public boolean importRelation(OSMRelation relation, boolean importUnnamedEntities) throws SQLException {
        // debug stop
        if(relation.getOSMIDString().equalsIgnoreCase("173239")) {
            int i = 42;
        }
        
        String ohdmIDString = this.importOSMElement(relation, importUnnamedEntities, true);
        
        if(ohdmIDString == null) return false;
        
        // remember its ohdm incarnation in intermediate database
        relation.setOHDMObjectID(this.sourceUpdateQueue, ohdmIDString);
        
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
    
    int getTargetTypeInt(OSMElement ohdmElement) {
        int targetType = 0;
        switch(ohdmElement.getGeometryType()) {
            case OHDM_DB.POINT: 
                targetType = OHDM_DB.OHDM_POINT_GEOMTYPE;
                break;
            case OHDM_DB.LINESTRING: 
                targetType = OHDM_DB.OHDM_LINESTRING_GEOMTYPE;
                break;
            case OHDM_DB.POLYGON: 
                targetType = OHDM_DB.OHDM_POLYGON_GEOMTYPE;
                break;
        }
        
        return targetType;
    }
    
    @Override
    public boolean importPostProcessing(OSMElement element, boolean importUnnamedEntities) throws SQLException {
        // are there historic names?
        List<OldName> oldNames = element.getOldNames();
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

        HashMap<String,String> newOldObject_Name_ID = new HashMap<>();
        
        Iterator<OldName> oldNamesIter = oldNames.iterator();        
        while(oldNamesIter.hasNext()) {
            OldName oldName = oldNamesIter.next();
            
            // each new old name is a new object
            
            // have we already created a new old object?
            String newOldOHDMID = newOldObject_Name_ID.get(oldName.oldname);
            
            if(newOldOHDMID == null) {
                // not yet created.. do it
                newOldOHDMID = this.addOHDMObject(oldName.oldname, 
                        this.getOHDM_ID_ExternalUser(element));
                
                // remember
                newOldObject_Name_ID.put(oldName.oldname, newOldOHDMID);
            }
            
            String targetIDString;
            if(element instanceof OSMRelation) {
                // relations can have more than one associated geometry
                OSMRelation relation = (OSMRelation) element;
                for(int i = 0; i < relation.getMemberSize(); i++) {
                    OSMElement member = relation.getMember(i);
                    
                    targetIDString = member.getOHDMGeomID();
                    this.addValidity(this.targetSelectQueue, element, targetType, 
                            classCodeString, newOldOHDMID, targetIDString, 
                            externalUserID, oldName.fromYear, oldName.toYear);
                }
            } else {
                targetIDString = element.getOHDMGeomID();
                if(targetIDString != null && targetIDString.length() > 0) {
                    this.addValidity(this.targetSelectQueue, element, targetType, 
                            classCodeString, newOldOHDMID, targetIDString, 
                            externalUserID, oldName.fromYear, oldName.toYear);
                }
            }
        }
        
        if(newOldObject_Name_ID.isEmpty()) return false;
        
        this.targetSelectQueue.forceExecute();
        
        return true;
    }
    
    private int idExternalSystemOSM = -1;
    private int getOHDM_ID_externalSystemOSM() {
        if(this.idExternalSystemOSM == -1) {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("SELECT id FROM ");
                sb.append(DB.getFullTableName(targetSchema, OHDM_DB.TABLE_EXTERNAL_SYSTEMS));
                sb.append(" where name = 'OSM' OR name = 'osm';");
                ResultSet result = 
                        this.executeQueryOnTarget(sb.toString());
                
                result.next();
                this.idExternalSystemOSM = result.getInt(1);

            } catch (SQLException ex) {
                Logger.getLogger(OHDMImporter.class.getName()).log(Level.SEVERE, null, ex);
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
    
    
    private final HashMap<String, Integer> idExternalUsers = new HashMap<>();
    
    private int getOHDM_ID_ExternalUser(OSMElement ohdmElement) {
        // create user entry or find user primary key
        String externalUserID = ohdmElement.getUserID();
        String externalUsername = ohdmElement.getUsername();

        return this.getOHDM_ID_ExternalUser(externalUserID, 
                externalUsername);
    }
    
    private int getOHDM_ID_ExternalUser(String externalUserID, String externalUserName) {
        if(!this.validUserID(externalUserID)) return OHDM_DB.UNKNOWN_USER_ID;
        
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
            sb.append(DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_EXTERNAL_USERS));
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
                s.append(DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_EXTERNAL_USERS));
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
    
    String getOHDMObjectID(OSMElement osmElement, boolean namedEntitiesOnly) throws SQLException {
        // already in OHDM DB?
        String ohdmIDString = osmElement.getOHDMObjectID();
        if(ohdmIDString != null) return ohdmIDString;
        
        // add entry in object table
        try {
            // osm elements don't have necessarily a name. Does this one?
            if(!this.elementHasName(osmElement)) {
                // no name - to be imported anyway?
                if(!namedEntitiesOnly) {
                    // yes - fetch osm dummy object
                    ohdmIDString = this.getOSMDummyObject_OHDM_ID();
                    osmElement.setOHDMObjectID(this.sourceUpdateQueue, ohdmIDString);
                    
                    return ohdmIDString;
                } else {
                    // no identity and we only accept entities with an identity.. null
                    return null;
                }
            } else {
                // object has a name
                
                // create user entry or find user primary key
                String externalUserID = osmElement.getUserID();
                String externalUsername = osmElement.getUsername();

                int id_ExternalUser = this.getOHDM_ID_ExternalUser(externalUserID, 
                        externalUsername);

                ohdmIDString =  this.addOHDMObject(osmElement, id_ExternalUser);
            }
        
            return ohdmIDString;
        }
        catch(Exception e) {
            System.err.println("failure during node import: " + e.getClass().getName() + ":" + e.getMessage());
        }
        
        return null;
    }
    
    String addOHDMObject(OSMElement osmElement, int externalUserID) throws SQLException {
        // already in OHDM DB?
        String ohdmIDString = osmElement.getOHDMObjectID();
        if(ohdmIDString != null) return ohdmIDString;
        
//        SQLStatementQueue targetQueue = new SQLStatementQueue(this.targetConnection);
        
        String name = osmElement.getName();
        
        ohdmIDString = this.addOHDMObject(name, externalUserID);
        
        // remember in element
        osmElement.setOHDMObjectID(this.sourceUpdateQueue, ohdmIDString);
        
        return ohdmIDString;
    }
    
    String addOHDMObject(String name, int externalUserID) throws SQLException {
        SQLStatementQueue sql = new SQLStatementQueue(this.targetConnection);
        sql.append("INSERT INTO ");
        sql.append(DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_GEOOBJECT));
        sql.append(" (name, source_user_id) VALUES ('");
        sql.append(name);
        sql.append("', ");
        sql.append(externalUserID);
        sql.append(") RETURNING id;");
        
        ResultSet result = sql.executeWithResult();
        result.next();
        return result.getBigDecimal(1).toString();
    }
    
    String addGeometry(OSMElement osmElement) throws SQLException {
        return this.addGeometry(osmElement, 
                this.getOHDM_ID_ExternalUser(osmElement));
        
    }
    
    String addGeometry(OSMElement osmElement, int externalUserID) throws SQLException {
        String wkt = osmElement.getWKTGeometry();
        if(wkt == null || wkt.length() < 1) return null;
        
//        SQLStatementQueue targetQueue = new SQLStatementQueue(this.targetConnection);
        
        targetSelectQueue.append("INSERT INTO ");
        
        String fullTableName;
        
        switch(osmElement.getGeometryType()) {
            case OHDM_DB.POINT: 
                fullTableName = DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_POINTS);
                targetSelectQueue.append(fullTableName);
                targetSelectQueue.append(" (point, ");
                break;
            case OHDM_DB.LINESTRING: 
                fullTableName = DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_LINES);
                targetSelectQueue.append(fullTableName);
                targetSelectQueue.append(" (line, ");
                break;
            case OHDM_DB.POLYGON: 
                fullTableName = DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_POLYGONS);
                targetSelectQueue.append(fullTableName);
                targetSelectQueue.append(" (polygon, ");
                break;
        }
        
        targetSelectQueue.append(" source_user_id) VALUES ('");
        
        targetSelectQueue.append(wkt);
        targetSelectQueue.append("', ");
        targetSelectQueue.append(externalUserID);
        targetSelectQueue.append(") RETURNING id;");

        try {
            ResultSet result = targetSelectQueue.executeWithResult();
            result.next();
            String geomIDString = result.getBigDecimal(1).toString();
            osmElement.setOHDMGeometryID(this.sourceUpdateQueue, geomIDString);
            return geomIDString;
        }
        catch(SQLException e) {
            System.err.println("failure when inserting geometry, wkt:\n" + wkt + "\nosm_id: " + osmElement.getOSMIDString());
            throw e;
        }
    }
    
    

    void addValidity(OSMElement osmElement, String ohdmIDString, String ohdmGeomIDString, int externalUserID)
            throws SQLException {
        // what table is reference by id_geometry
        int targetType = 0;
        switch(osmElement.getGeometryType()) {
            case OHDM_DB.POINT: 
                targetType = OHDM_DB.OHDM_POINT_GEOMTYPE;
                break;
            case OHDM_DB.LINESTRING: 
                targetType = OHDM_DB.OHDM_LINESTRING_GEOMTYPE;
                break;
            case OHDM_DB.POLYGON: 
                targetType = OHDM_DB.OHDM_POLYGON_GEOMTYPE;
                break;
        }
        
        // there can be more than one classcode...
        
        this.addValidity(this.targetInsertQueue, osmElement, targetType, 
                osmElement.getClassCodeString(), ohdmIDString, 
                ohdmGeomIDString, externalUserID);
        
//        this.targetSelectQueue.forceExecute();
        this.targetInsertQueue.couldExecute();
    }
    
    private String formatDateString(String sinceValue) {
        // assume we have only got the year
        if(sinceValue.length() == 4) {
            return sinceValue + "-01-01";
        } 
        
        // TODO more..
        
        return null;
    }
    
    void addValidity(SQLStatementQueue sq, OSMElement osmElement, int targetType, 
            String classCodeString, String sourceIDString, 
            String targetIDString, int externalUserID) throws SQLException {
        
        String sinceString = null;
        /* 
        is there since tag in osm origin?
        probably not .. haven't not yet proposed that tag 
        */
        String sinceValue = osmElement.getValue("since");
        if(sinceValue != null) {
            sinceString = this.formatDateString(sinceValue);
        } 

        // take osm timestamp
        if(sinceString == null) {
            sinceString = osmElement.getTimeStampString();
        }
        
        // finally take default
        if(sinceString == null) {
            sinceString = this.defaultSince;
        }
        
        this.addValidity(sq, osmElement, targetType, classCodeString, 
                sourceIDString, targetIDString, externalUserID, 
                sinceString, this.defaultUntil);
    }
    
    void addValidity(SQLStatementQueue sq, OSMElement osmElement, int targetType, 
            String classCodeString, String sourceIDString, 
            String targetIDString, int externalUserID, String sinceString, 
            String untilString) throws SQLException {
        
        if(sourceIDString == null) {
            // failure
            System.err.println("source id must not be null");
        }
        
        // some osm elements are tagged with more than one feature class
        Iterator<String> classIDIter = osmElement.getOtherClassIDs();
        
        boolean again = false;
        do {
            again = false;
            sq.append("INSERT INTO ");
            sq.append(DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_GEOOBJECT_GEOMETRY));
            sq.append(" (type_target, classification_id, id_geoobject_source, id_target, valid_since, valid_until, ");
            sq.append(" source_user_id");
            if(osmElement.hasFreeAttributes()) {
                sq.append(", tags) VALUES (");
            } else {
                sq.append(") VALUES (");
            }
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
            if(osmElement.hasFreeAttributes()) {
                sq.append(", '"); //
                sq.append(osmElement.getFreeAttributesASHStoreValue());
                sq.append("'"); //
            }

            sq.append(");");
            /*
            if(osmElement.hasFreeAttributes()) {
                sq.forceExecute(); // that's for debugging purposes only: TODO
            } else { */
                sq.couldExecute();
            //}
            
            if(classIDIter.hasNext()) {
                classCodeString = classIDIter.next();
                again = true;
            }
        } while(again);
    }

    void addContentAndURL(OSMElement osmElement, String ohdmIDString) {
        SQLStatementQueue sql = this.targetInsertQueue;
        
        String description = osmElement.getValue("description");
        
        /*
        INSERT INTO ohdm.content(id, name, value, mimetype)
            VALUES (42, 'description', 'huhu', 'text/plain');
        */
//        if(description != null) {
//            sql.append("INSERT INTO ");
//            sql.append(DB.getFullTableName(this.targetSchema, OHDM_DB.CONTENT));
//            sql.append(" (name, source_user_id) VALUES ('");
//            sql.append(name);
//            sql.append("', ");
//            sql.append(externalUserID);
//            sql.append(") RETURNING id;");
//        }
        
        /*
         * URLs
        image
        *
        url
        website
         */
//        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection);
    }

    /**
     *
     * @param ohdmElement
     * @return true of element has a name OR a classid (or both)
     */
    private boolean elementHasIdentity(OSMElement ohdmElement) {
        // per definition: anything with a classcode has an identity - even without a name
        if(ohdmElement.getClassCodeString() != null) {
            return true;
        }

        // has no class code
        return this.elementHasName(ohdmElement);
    }

    /**
     *
     * @param ohdmElement
     * @return true if element has a name longer than 0 which is not just a number
     */
    private boolean elementHasName(OSMElement ohdmElement) {
        String name = ohdmElement.getName();

        if(name == null || name.length() < 1) return false;

        // name must not a single number
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
     * @param osmElement element to be imported
     * @param namedEntitiesOnly only import osm entities without a name (identity)?
     * @param importWithoutGeometry import entities even without a geometry (relevant for relations)
     * @return ohdm_id as string
     */
    public String importOSMElement(OSMElement osmElement, boolean namedEntitiesOnly, boolean importWithoutGeometry) {
        String osmID = osmElement.getOSMIDString();
        if(osmID.equalsIgnoreCase("188276804") || osmID.equalsIgnoreCase("4242")) {
            // debug break
            int i = 42;
        }
        
        // beside relations, entities without a geometry are not to be imported
        if(!osmElement.hasGeometry() && !importWithoutGeometry) {
            return null;
        }
        
        try {
            // get external user id from ohdm
            int id_ExternalUser = this.getOHDM_ID_ExternalUser(osmElement);

            /*
            we need to get an ohdm object for this osm object.
            a) We can create a real object if the osm element has a name.
            b) We can get a dummy object if this element has no name but shall be excepted
            without a name (namedEntitiesOnly == false)
            c) We want that osm object in the DB even without a name if it has a classid.
             In that case namedEntitiesOnly can be true but is overruled by this.elementHasIdentity which
             returns true for elements with a classid
            */
            String ohdmObjectIDString = null;
            if(this.elementHasIdentity(osmElement)) {
                ohdmObjectIDString = this.getOHDMObjectID(osmElement,false);
            } else {
                ohdmObjectIDString = this.getOHDMObjectID(osmElement, namedEntitiesOnly);
            }

            if(ohdmObjectIDString == null) {
                /*
                System.err.println("cannot create or find ohdm object id (not even dummy osm) and importing of unnamed entites allowed");
                System.err.println(osmElement);
                */

                // try to add a geometry
                String ohdmGeomIDString = this.addGeometry(osmElement, id_ExternalUser);
                
                if(ohdmGeomIDString != null) {
                    // remeber geometry in inter db
                    this.intermediateDB.setOHDM_IDs(this.sourceUpdateQueue, osmElement, null, ohdmGeomIDString);
                }

                // geometry added but no object.. we are done here
                return null;
            }

            // we have an object.. try to add geometry
            
            // try to add a geometry
            String ohdmGeomIDString = this.addGeometry(osmElement, id_ExternalUser);
            
            // combine object and geometry if there is a geometry
            if(ohdmGeomIDString != null) {
                // create entry in object_geometry table
                this.addValidity(osmElement, ohdmObjectIDString, ohdmGeomIDString, 
                        id_ExternalUser);
                
                /* now make both object and geom id persistent to intermediate db
                */
                this.intermediateDB.setOHDM_IDs(this.sourceUpdateQueue, osmElement, ohdmObjectIDString, ohdmGeomIDString);
            }

            // keep some special tags (url etc, see wiki) for the object
            this.addContentAndURL(osmElement, ohdmObjectIDString);
            
            return ohdmObjectIDString;
        }
        catch(SQLException e) {
            System.err.println("failure during import of intermediate object: " + e.getMessage());
            System.err.println(osmElement);
        }
        
        return null;
    }
    
    void forgetPreviousNodesImport() throws SQLException {
        SQLStatementQueue sql = new SQLStatementQueue(this.sourceConnection);

        System.out.println("reset nodes entries in intermediate db");
        sql.append("UPDATE ");
        sql.append(DB.getFullTableName(this.sourceSchema, "nodes"));
        sql.append(" SET ohdm_geom_id=null, ohdm_object_id=null;");
        sql.forceExecute(true);
        
        sql.join();
    }
      
    void forgetPreviousWaysImport() throws SQLException {
        SQLStatementQueue sql = new SQLStatementQueue(this.sourceConnection);

        System.out.println("reset ways entries in intermediate db");
        sql.append("UPDATE ");
        sql.append(DB.getFullTableName(this.sourceSchema, "ways"));
        sql.append(" SET ohdm_geom_id=null, ohdm_object_id=null;");
        sql.forceExecute(true);
        
        sql.join();
    }
      
    void forgetPreviousRelationsImport() throws SQLException {
        SQLStatementQueue sql = new SQLStatementQueue(this.sourceConnection);

        System.out.println("reset relations entries in intermediate db");
        sql.append("UPDATE ");
        sql.append(DB.getFullTableName(this.sourceSchema, "relations"));
        sql.append(" SET ohdm_geom_id=null, ohdm_object_id=null;");
        sql.forceExecute();
        
        sql.join();
    }
    
    public static void main(String args[]) throws IOException, SQLException {
        // let's fill OHDM database
        System.out.println("Start extracting ODHM data from intermediate DB");
        SQLStatementQueue sourceQueue = null;
        
        Trigger trigger = null;
        OSMExtractor extractor = null;
        OHDMImporter ohdmImporter = null;
        SQLStatementQueue targetQueue = null;
        
        try {
            String sourceParameterFileName = "db_inter.txt";
            String targetParameterFileName = "db_ohdm.txt";
            
            if(args.length > 0) {
                sourceParameterFileName = args[0];
            }
            
            if(args.length > 1) {
                targetParameterFileName = args[1];
            }
            
            Parameter sourceParameter = new Parameter(sourceParameterFileName);
            Parameter targetParameter = new Parameter(targetParameterFileName);
            
            Connection sourceConnection = DB.createConnection(sourceParameter);
            Connection targetConnection = DB.createConnection(targetParameter);
            
            IntermediateDB iDB = new IntermediateDB(sourceConnection, sourceParameter.getSchema());
            
            String sourceSchema = sourceParameter.getSchema();
            String targetSchema = targetParameter.getSchema();
            
            SQLStatementQueue updateQueue = null;
            FileSQLStatementQueue fileUpdateQueue = null;
            
            String currentUpdateFileName = "updateNodes.sql";
            File updateCommmands = new File(currentUpdateFileName);
            
            if(sourceParameter.usePSQL()) {
                // create File to keep update commands
                System.out.println("intermediate update queue uses psql and sql files.");
                fileUpdateQueue = new FileSQLStatementQueue(updateCommmands);
                updateQueue = fileUpdateQueue;
            } else {
                System.out.println("intermediate update queue uses jdbc");
                updateQueue = new SQLStatementQueue(sourceParameter);
            }
            
            ohdmImporter = new OHDMImporter(iDB, targetParameter.getOsmfilecreationdate(), sourceConnection,
                    targetConnection, sourceSchema, targetSchema, updateQueue);

            try {
                if(targetParameter.forgetPreviousImport()) {
                    System.out.println("remove ohdm entries in intermediate database");            
                        ohdmImporter.forgetPreviousNodesImport();
                        ohdmImporter.forgetPreviousWaysImport();
                        ohdmImporter.forgetPreviousRelationsImport();
                }
                
                OHDM_DB.dropOHDMTables(targetConnection, targetSchema);
            }
            catch(Exception e) {
                System.err.println("problems during setting old data (non-fatal): " + e.getLocalizedMessage());
            }
            
            OHDM_DB.createOHDMTables(targetConnection, targetSchema);
            
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

            extractor = new OSMExtractor(sourceConnection, sourceSchema, ohdmImporter, stepLen);
            
            System.out.println("intermediate select queue uses jdbc");
            sourceQueue = DB.createSQLStatementQueue(sourceConnection, sourceParameter);
            
            System.out.println("ohdm insert queue uses jdbc");
            targetQueue = new SQLStatementQueue(targetParameter);
            
            System.out.println("start insert data into ohdm DB from intermediate DB");
        
            // start stats trigger each [interval] minutes (default 5)
            int logMessageInterval = targetParameter.getLogMessageInterval();
            trigger = new Trigger(extractor, 1000 * 60 * logMessageInterval);
            trigger.start();

            // set initial max validity
            OHDM_DB.writeInitialImportDate(targetConnection, targetSchema, targetParameter.getOsmfilecreationdate());
            
//            if(targetParameter.importNodes()) {
                extractor.processNodes(sourceQueue, true);
                ohdmImporter.forceExecute();
                
                if(fileUpdateQueue != null) {
                    // cut off update stream and let psql process that stuff
                    String nextFileName = "updateWays.sql";
                    updateCommmands = new File(nextFileName);
                    fileUpdateQueue.switchFile(updateCommmands);

                    trigger.setMilliseconds(1000 * 60 * 30); // 30 minutes
                    // now process stored updates... that process must be performed before processing ways or relations
                    System.out.println("psql is executing node update commands..");
                    Util.feedPSQL(sourceParameter, currentUpdateFileName, false, true);
                    System.out.println("..done");
                    trigger.setMilliseconds(1000 * 60 * 5); // 5 minutes again
                    trigger.interrupt();

                    // remember new update filename
                    currentUpdateFileName = nextFileName;
                }
//            } else {
//                System.out.println("skip nodes import.. see importNodes in ohdm parameter file");
//            }
            
//            if(targetParameter.importWays()) {
                extractor.processWays(sourceQueue, false);
                ohdmImporter.forceExecute();
                
                if(fileUpdateQueue != null) {
                    // cut off update stream and let psql process that stuff
                    String nextFileName = "updateRelations.sql";
                    updateCommmands = new File(nextFileName);
                    fileUpdateQueue.switchFile(updateCommmands);

                    trigger.setMilliseconds(1000 * 60 * 30); // 30 minutes
                    // now process stored updates... that process must be performed before processing ways or relations
                    System.out.println("psql is executing way update commands..");
                    Util.feedPSQL(sourceParameter, currentUpdateFileName, false, true);
                    System.out.println("..done");
                    trigger.setMilliseconds(1000 * 60 * 5); // 5 minutes
                    trigger.interrupt();

                    // remember new update filename
                    currentUpdateFileName = nextFileName;
                }
                /*
            } else {
                System.out.println("skip ways import.. see importWays in ohdm parameter file");
            }*/

//            if(targetParameter.importRelations()) {
                extractor.processRelations(sourceQueue, false);
                ohdmImporter.forceExecute();
                
                // do some post processing: stop trigger (DEBUGGING)
                trigger.end();

                /*
                In some rare cases, relation are reference by their OSM id an not
                OHDM ID in geoobject_geometry table. That problem can be fixed after
                importing all relations. Do it here.
                */
                ohdmImporter.postProcessGGTable();
                
                if(fileUpdateQueue != null) {
                    // close update stream and let psql process that stuff
                    fileUpdateQueue.close();

                    // now process stored updates... that process must be performed before processing ways or relations
                    trigger.setMilliseconds(1000 * 60 * 30); // 30 minutes
                    System.out.println("psql is executing relation update commands..");
                    Util.feedPSQL(sourceParameter, currentUpdateFileName, false, true);
                    System.out.println("..done");
                    trigger.setMilliseconds(1000 * 60 * 5); // 5 minutes
                    trigger.interrupt();
                }
/*            } else {
                System.out.println("skip relations import.. see importRelations in ohdm parameter file");
            } */

        } catch (IOException | SQLException e) {
            Util.printExceptionMessage(e, sourceQueue, "main method in Inter2OHDM", false);
        }
        finally {
            if(trigger != null) {
                trigger.end();
                trigger.interrupt();
            }
            
            if(targetQueue != null) {
                targetQueue.forceExecute();
                targetQueue.join();
            }
            if(ohdmImporter != null) ohdmImporter.close();
            System.out.println("done importing from intermediate DB to ohdm DB");
            System.out.println(extractor.getStatistics());
        }
    }

    private final String osmDummyObjectOHDM_ID = "0";
    
    private String getOSMDummyObject_OHDM_ID() {
        return this.osmDummyObjectOHDM_ID;
    }

    private boolean saveRelationAsRelatedObjects(OSMRelation relation, 
            String ohdmIDString) throws SQLException {
        
        if(relation.getOSMIDString().equalsIgnoreCase("1216")) {
            int debugStop = 42;
        }
        
        // get all ohdm ids and store it
        StringBuilder sq = new StringBuilder();

        /**
         * INSERT INTO [geoobject_geometry] 
         * (id_geoobject_source, id_target, type_target, valid_since, 
         * valid_until VALUES (..)
         */

        sq.append("INSERT INTO ");
        sq.append(OHDM_DB.TABLE_GEOOBJECT_GEOMETRY);
        sq.append("(id_geoobject_source, source_user_id, id_target, type_target, role,");
        sq.append(" classification_id, valid_since, valid_until) VALUES ");

        boolean notFirstSet = false;
        int ohdm_id_ExternalUser = this.getOHDM_ID_ExternalUser(relation);
        for(int i = 0; i < relation.getMemberSize(); i++) {
            
            // add member information
            OSMElement member = relation.getMember(i);
            
            /*
            now it becomes interesting. That member can
            a) its own identity which means: it has an object id and
            probably a geometry id. We keep that object id
            b) have no identity but a geometry - we take this
            c) nothing at all - we create a geometry
            */
            
            // remember if that member is an object or "only" a geometry
            boolean isObject = false;
            boolean useOSMID = false;
            
            // try to get OHDM ID
            String memberOHDMIDString = member.getOHDMObjectID();
            
            if(memberOHDMIDString != null && memberOHDMIDString.length() > 0) {
                // got one - that an object
                isObject = true;
            } else {
                // maybe we have found a relations that is defined later.
                if(member instanceof OSMRelation) {
                    //get osm id instead and save it.
                    memberOHDMIDString = member.getOSMIDString();
                    useOSMID = true;
                } else {
                    // no relation
                
                    // no object but geometry?
                    memberOHDMIDString = member.getOHDMGeomID();

                    if(memberOHDMIDString == null || memberOHDMIDString.length() == 0) {
                        // that object has not yet save at all.

                        memberOHDMIDString = this.addGeometry(member);

                        if(memberOHDMIDString == null || 
                                memberOHDMIDString.length() == 0) {
                            /* this makes no sense. That thing cannot be written
                            */
                            System.err.println("----------------------------------------------");
                            System.err.print("when saving relation: member cannot be saved: ");
                            System.err.print("relation ohdm object id: " + relation.getOHDMObjectID());
                            System.err.println(" / member osm id: " + member.getOSMIDString());
                            System.err.println("----------------------------------------------");
                            continue;
                        }
                    }
                }
            }
            
            // get role of that member in that relation
            String roleName = relation.getRoleName(i);

            // now construct that insert statement
            
            if(notFirstSet) {
                sq.append(", ");
            } else {
                notFirstSet = true;
            }

            sq.append("(");
            sq.append(ohdmIDString); // id source
            sq.append(", ");
            sq.append(ohdm_id_ExternalUser); // id source
            sq.append(", ");

            sq.append(memberOHDMIDString); // id target (geom or object)
            
            sq.append(", ");
            if(isObject) {
                // we have take the object id instead of geometry
                sq.append(OHDM_DB.OHDM_GEOOBJECT_GEOMTYPE);
            } else if(useOSMID) {
                sq.append(OHDM_DB.OHDM_GEOOBJECT_GEOMTYPE_OSM_ID);
            } else {
                // decide by member type
                if(member instanceof OSMNode) { // type_target
                    sq.append(OHDM_DB.OHDM_POINT_GEOMTYPE);
                } else if(member instanceof OSMWay) {
                    sq.append(OHDM_DB.OHDM_LINESTRING_GEOMTYPE);
                } else {
                    sq.append(OHDM_DB.OHDM_GEOOBJECT_GEOMTYPE);
                }
            }
            sq.append(", '");
            sq.append(roleName); // role
            sq.append("', ");
            sq.append(relation.getClassCodeString()); // classification
            sq.append(", '");
            sq.append(this.defaultSince); // since
            sq.append("', '");
            sq.append(this.defaultUntil); // until
            sq.append("')"); // end that value set
        }
        sq.append(";"); // end that value set

        if(notFirstSet) {
            // there is at least one value set - excecute
            SQLStatementQueue sql = new SQLStatementQueue(this.targetConnection);
            sql.append(sq.toString());
            sql.forceExecute();
            return true;
        }
        return false;
    }

    private boolean saveRelationAsMultipolygon(OSMRelation relation) throws SQLException {
        /* sometimes (actually quite often) relations contain only an inner
        and outer member but inner comes first which is not compatible with
        definition of a multipolygon.. We correct that problem here
        */
        
        // debugging stop
        if(relation.getOSMIDString().equalsIgnoreCase("173239")) {
            int i = 42;
        }
        
        /* 
            if a multipolygon relation has only two member, inner and outer,
            bring them into right order.
        */
        if(!relation.checkMultipolygonMemberOrder()) return false;
        
        // option b) it is a polygone or probably a multipolygon
        ArrayList<String> polygonIDs = new ArrayList<>();
        ArrayList<String> polygonWKT = new ArrayList<>();
        
        ArrayList<OSMElement> waysWithIdentity = new ArrayList<>();
        ArrayList<OSMElement> nodesWithIdentity = new ArrayList<>();
        
        if(!relation.fillRelatedGeometries(polygonIDs, polygonWKT, 
                waysWithIdentity, nodesWithIdentity)) return false;
        
        /* we have two list with either references to existing
         geometries or to string representing geometries which are 
        to be stored and referenced.
        */
//        SQLStatementQueue targetQueue = new SQLStatementQueue(this.targetConnection);
        for(int i = 0; i < polygonIDs.size(); i++) {
            String pID = polygonIDs.get(i);
            if(pID.equalsIgnoreCase("-1")) {
                // this geometry is not yet in the database.. insert that polygon
                targetSelectQueue.append("INSERT INTO ");
                targetSelectQueue.append(DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_POLYGONS));
                targetSelectQueue.append(" (polygon, source_user_id) VALUES ('");
//                targetSelectQueue.append("SRID=4326;"); // make it an ewkt
                targetSelectQueue.append(polygonWKT.get(i));
                targetSelectQueue.append("', ");
                int ohdmUserID = this.getOHDM_ID_ExternalUser(relation);
                targetSelectQueue.append(ohdmUserID);
                targetSelectQueue.append(") RETURNING ID;");
                
//                sq.print("saving polygon wkt");
                try {
                    ResultSet polygonInsertResult = targetSelectQueue.executeWithResult();
                    polygonInsertResult.next();
                    String geomIDString = polygonInsertResult.getBigDecimal(1).toString();
                    polygonIDs.set(i, geomIDString);
                }
                catch(SQLException e) {
                    System.err.println("sql failed: " + targetSelectQueue.toString());
                    throw e;
                }
            }
        }

        if(polygonIDs.size() < 1) return false;
        
        // add relations
        int targetType = OHDM_DB.OHDM_POLYGON_GEOMTYPE; // all targets are polygons
        String classCodeString = relation.getClassCodeString();
        String sourceIDString = relation.getOHDMObjectID();
        
        if(sourceIDString == null) {
            // debug stop
            int i = 42;
        }
        int externalUserID = this.getOHDM_ID_ExternalUser(relation);
        
        // void addValidity(int targetType, String classCodeString, String sourceIDString, String targetIDString, int externalUserID) throws SQLException {
        for(String targetIDString : polygonIDs) {
            this.addValidity(targetSelectQueue, relation, targetType, classCodeString, sourceIDString, targetIDString, externalUserID);
        }
        
        // fill subsequent table if necessary
        if(!waysWithIdentity.isEmpty()) {
            this.saveSubsequentObjects(targetSelectQueue, relation.getOHDMObjectID(), 
                    OHDM_DB.LINESTRING, waysWithIdentity.iterator());
        }
        
        if(!nodesWithIdentity.isEmpty()) {
            this.saveSubsequentObjects(targetSelectQueue, relation.getOHDMObjectID(), 
                    OHDM_DB.POINT, nodesWithIdentity.iterator());
        }
        
        try {
            targetSelectQueue.forceExecute(true);
        }
        catch(SQLException se) {
            Util.printExceptionMessage(se, targetSelectQueue, "when writing relation tables", false);
        }
        return true;
    }

    void forgetPreviousImport() throws SQLException {
        this.forgetPreviousNodesImport();
        this.forgetPreviousWaysImport();
        this.forgetPreviousWaysImport();
    }

    void postProcessGGTable() throws SQLException {
        /*
        In some rare cases, relations are referenced by their OSM id an not
        OHDM ID in geoobject_geometry table. That problem can be fixed after
        importing all relations. Do it here.
        */
        
        this.targetSelectQueue.append("SELECT id_target FROM ");
        this.targetSelectQueue.append(DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_GEOOBJECT_GEOMETRY));
        this.targetSelectQueue.append(" WHERE type_target = ");
        this.targetSelectQueue.append(OHDM_DB.OHDM_GEOOBJECT_GEOMTYPE_OSM_ID);
        this.targetSelectQueue.append(";");
        
        ResultSet qResult = this.targetSelectQueue.executeWithResult();
        
        SQLStatementQueue sourceSelectQueue = new SQLStatementQueue(this.sourceConnection);
        
        StringBuilder sourceSelectStartBuilder = new StringBuilder("SELECT ohdm_object_id FROM ");
        sourceSelectStartBuilder.append(DB.getFullTableName(this.sourceSchema, InterDB.RELATIONTABLE));
        sourceSelectStartBuilder.append(" WHERE osm_id = ");
        String sourceSelectStart = sourceSelectStartBuilder.toString();

        StringBuilder targetUpdateStartBuilder = new StringBuilder("UPDATE ");
        targetUpdateStartBuilder.append(DB.getFullTableName(this.targetSchema, OHDM_DB.TABLE_GEOOBJECT_GEOMETRY));
        targetUpdateStartBuilder.append(" SET type_target = ");
        targetUpdateStartBuilder.append(OHDM_DB.OHDM_GEOOBJECT_GEOMTYPE);
        targetUpdateStartBuilder.append(", id_target = ");
        String targetUpdateStart = targetUpdateStartBuilder.toString();
        
        while(qResult.next()) {
            BigDecimal relationOSMID = qResult.getBigDecimal(1);
            
            sourceSelectQueue.append(sourceSelectStart);
            sourceSelectQueue.append(relationOSMID.toString());
            sourceSelectQueue.append(";");
            
            ResultSet sResult = sourceSelectQueue.executeWithResult();
            if (sResult.next()) {
                BigDecimal ohdmID = sResult.getBigDecimal(1);

                if (ohdmID != null) {
                    this.targetInsertQueue.append(targetUpdateStart);
                    this.targetInsertQueue.append(ohdmID.toString());
                    this.targetInsertQueue.append(" WHERE type_target = ");
                    this.targetInsertQueue.append(OHDM_DB.OHDM_GEOOBJECT_GEOMTYPE_OSM_ID);
                    this.targetInsertQueue.append(" AND id_target = ");
                    this.targetInsertQueue.append(relationOSMID.toString());
                    this.targetInsertQueue.append(";");
                    this.targetInsertQueue.couldExecute();
                }

            } else {
                // failure
                System.err.println("-----------------------------------------------------");
                System.err.println("cannot update object osm by it ohdm id in geoobject_geometry table.. fatal");
                System.err.println("methode OHDMImporter.postProcessGGTable()");
                System.err.println("relation (OSM): " + relationOSMID.toString());
                System.err.println("-----------------------------------------------------");
            }
        }
        
        // force updating
        this.targetInsertQueue.forceExecute();
    }
    
    
    /////////////////////////////////////////////////////////////////////////
    //                            update methods                           //
    /////////////////////////////////////////////////////////////////////////
    
    /**
     * update existing OHDM object based on a existing node
     * which object has changed
     * @param node
     * @param importUnnamedEntities
     * @return 
     */
    public boolean updateNodeGeometry(OSMNode node, boolean importUnnamedEntities) {
        if(!this.elementHasIdentity(node) && !importUnnamedEntities) {
            // nodes without an identity are not imported.
            return false;
        }

        return this.updateOSMElementGeometry(node);
    }
    
    /**
     * Object exists - geometry is new. Assumes onyl one geometry which
     * holds for nodes and ways and relations representing a polygon.
     * Does not hold for relations representing non-polygons (real relations
     * so to say)
     * @param osmElement
     * @return 
     */
    public boolean updateOSMElementGeometry(OSMElement osmElement) {
        String osmID = osmElement.getOSMIDString();
        if(osmID.equalsIgnoreCase("188276804") || osmID.equalsIgnoreCase("301835391")) {
            // debug break
            int i = 42;
        }
        
        if(!osmElement.hasGeometry()) {
            System.err.println("try to update geometry of an osm object "
                    + "without a geometry (didn't update anything): " + osmID);
            
            return false;
        }
        
        // there must be an object id - even the dummy id.
        String ohdmObjectIDString = osmElement.getOHDMObjectID();
        
        if(ohdmObjectIDString == null) {
            System.err.println("try to update geometry of an osm object "
                    + "with no ohdm object id (didn't update anything): " + osmID);
            
            return false;
        }

        try {
            // get external user id from ohdm
            int id_ExternalUser = this.getOHDM_ID_ExternalUser(osmElement);

            // try to add a geometry
            String ohdmGeomIDString = this.addGeometry(osmElement, id_ExternalUser);
            
            if(ohdmGeomIDString == null) {
                System.err.println("tried to update geometry but couldn't "
                        + "create new geometry: " + osmID);

                return false;
            }

            this.updateValidity(osmElement, null, ohdmGeomIDString);

            // update ohdmObjects in intermediate
            this.intermediateDB.setOHDM_IDs(this.sourceUpdateQueue, osmElement, ohdmObjectIDString, ohdmGeomIDString);

            return true;
        }
        catch(SQLException e) {
            System.err.println("failure during updating geometry of existing object: " + e.getMessage());
            System.err.println(osmElement);
        }
        
        return false;
    }
    
    public boolean updateOSMElementObject(OSMElement osmElement) {
        String osmID = osmElement.getOSMIDString();
        if(osmID.equalsIgnoreCase("188276804") || osmID.equalsIgnoreCase("301835391")) {
            // debug break
            int i = 42;
        }
        
        try {
            // get external user id from ohdm
            int id_ExternalUser = this.getOHDM_ID_ExternalUser(osmElement);

            /*
              get ohdm object. That call returns null only if that object has no
            identity and importing of unnamed entities is not yet wished in that call
            null indicates a failure
            */
            String ohdmObjectIDString = this.getOHDMObjectID(osmElement, true);

            if(ohdmObjectIDString == null) {
                System.err.println("try to update object but couldn't "
                        + "create new object (didn't update anything): " + osmID);

                return false;
            }
            
            this.updateValidity(osmElement, ohdmObjectIDString, null);

            // update ohdmObjects in intermediate
            this.intermediateDB.setOHDM_IDs(this.sourceUpdateQueue, osmElement, 
                    ohdmObjectIDString, osmElement.getOHDMGeomID());

            return true;
        }
        
        catch(SQLException e) {
            System.err.println("failure during import of intermediate object: " + e.getMessage());
            System.err.println(osmElement);
        }
        
        return false;
    }

    private void updateValidity(OSMElement osmElement, String newOHDMObjectID, 
            String newOHDMGeometryID /*, boolean multipleLinesExpected */) {
        
        String timeChangedString = osmElement.getTimeStampString();

        // update old validity entr(ies) set until to timeChangedString
        
        // insert validity line(s) with either newobject or new geometry id

    }
}
