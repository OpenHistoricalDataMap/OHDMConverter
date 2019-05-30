package inter2ohdm;

import java.io.PrintStream;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import osm.OSMClassification;
import util.InterDB;
import util.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public abstract class OSMElement extends AbstractElement {
    private final String osmIDString;
    private final String classCodeString;
    List<String> otherClassCodeList;
    
    private int subClassCode;
    
    private String ohdmObjectIDString;
    private String ohdmGeomIDString;
    
    private final boolean valid;
    
    protected boolean isPolygon = false;
    protected final IntermediateDB intermediateDB;
    
    private final boolean isNew;
    private final boolean changed;
    private final boolean deleted;
    private final boolean has_name;
    private final boolean object_new;

    private String tstamp;
    private final Date tstampDate;
    
    protected String wktString = null;
    protected boolean wktStringProduced = false;

    String getTimeStampString() {
        if(this.tstamp == null) return null;
        
        if(tstamp.length() < 10) return null;
        
        // old (current importer produces that that format, return date only
        // 2016-11-04T08:46:37Z

        // remove following two lines (and comments) when importer are fixed
        // new shall produce 1017-01-01 or so
        if(tstamp.length() == 10) return this.tstamp;
        this.tstamp = tstamp.substring(0, 11);
        
        return this.tstamp;
    }

    OSMElement(IntermediateDB intermediateDB, String osmIDString,
               String classCodeString, String otherClassCodes, String sTags,
               String ohdmObjectIDString, String ohdmGeomIDString,
               boolean valid,
               boolean geom_changed, boolean object_changed,
               boolean deleted,
               boolean has_name, Date tstampDate, boolean object_new) {
        
        super(sTags);

        if(osmIDString.equalsIgnoreCase("28245535")) {
            int i = 42; // debug break
        }
        
        this.intermediateDB = intermediateDB;
        this.getUserID();
        this.getUsername();
        this.osmIDString = osmIDString;
        this.classCodeString = classCodeString;
        this.ohdmObjectIDString = ohdmObjectIDString;
        this.ohdmGeomIDString = ohdmGeomIDString;
        this.valid = valid;
        
        this.isNew = geom_changed;
        this.changed = object_changed;
        this.deleted = deleted;
        this.has_name = has_name;
        this.tstampDate = tstampDate;
        
        this.tstamp = tstampDate.toString();

        this.object_new = object_new;

        this.otherClassCodeList = InterDB.getIDList(otherClassCodes);
    }
    
    /**
     * produce a clone: note: this is *not* a deep copy
     * @param orig
     * @return 
     */
    public OSMElement clone(OSMElement orig) {
        return null; // TODO
    }
    
    boolean hasGeometry() {
        if(!this.wktStringProduced) {
            this.produceWKTGeometry();
        }
        
        return this.wktString != null;
    }

    // generate wkt string a store it into wktString! Set wktStringProduced to true!
    abstract protected void produceWKTGeometry();
    
    final String getWKTGeometry() {
        if(!this.wktStringProduced) {
            this.produceWKTGeometry();
        }
        
        if(this.wktString == null || this.wktString.isEmpty()) {
            return null;
        }
        
        if(!this.wktString.startsWith("SRID")) {
            this.wktString = "SRID=4326;" + this.wktString;
        }
        
        return this.wktString;
    }
    
    abstract int getGeometryType();
    
    void setOHDM_IDs(SQLStatementQueue sql, String ohdmObjectIDString, String ohdmGeomIDString, boolean persist) throws SQLException {
        if(persist) {
            this.intermediateDB.setOHDM_IDs(sql, this, ohdmObjectIDString, ohdmGeomIDString);
        }

        if(ohdmObjectIDString != null) {
            this.ohdmObjectIDString = ohdmObjectIDString;
        }
        
        if(ohdmGeomIDString != null) {
            this.ohdmGeomIDString = ohdmGeomIDString;
        }
    }
    
    void setOHDM_IDs(SQLStatementQueue sql, String ohdmObjectIDString, String ohdmGeomIDString) throws SQLException {
        this.setOHDM_IDs(sql, ohdmObjectIDString, ohdmGeomIDString, true);
    }
    
    void setOHDMObjectID(SQLStatementQueue sql, String ohdmObjectIDString) throws SQLException {
        this.setOHDM_IDs(sql, ohdmObjectIDString, null, true);
    }
    
    void setOHDMGeometryID(SQLStatementQueue sql, String geometryIDString) throws SQLException {
        this.setOHDM_IDs(sql, null, geometryIDString, true);
    }
    
    String getOHDMObjectID() {
        return this.ohdmObjectIDString;
    }
    
    boolean hasOHDMObjectID() {
        return (this.ohdmObjectIDString != null 
                && !this.ohdmObjectIDString.isEmpty());
    }
    
    String getOHDMGeomID() {
        return this.ohdmGeomIDString;
    }
    
    /**
     * Remove this object from intermediate db .. use carefully!
     */
    void remove() throws SQLException {
        this.intermediateDB.remove(this);
    }
    
    String getOSMIDString() {
        return osmIDString;
    }
    
    String getClassCodeString() {
        return this.classCodeString;
    }
    
    private String className = null;
    private String subClassName = null;
    
    String getClassName() {
        if(className == null) {
            String fullClassName = OSMClassification.getOSMClassification().
                    getFullClassName(this.classCodeString);
        
            StringTokenizer st = new StringTokenizer(fullClassName, "_");
            this.className = st.nextToken();
            
            if(st.hasMoreTokens()) {
                this.subClassName = st.nextToken();
            } else {
                this.subClassName = "undefined";
            }
        }
        
        return this.className;
    }
  
    String getSubClassName() {
        if(this.subClassName == null) {
            this.getClassName();
        }
        
        return this.subClassName;
    }
    
    private String uid = null;
    /**
     * return osm user id
     * @return 
     */
    final String getUserID() {
        if(this.uid == null) {
            this.uid = this.getValue("uid");

            if(this.uid == null) {
                this.uid = "-1";
            }
        }
        
        return this.uid;
    }
    
    private String username = null;
    final String getUsername() {
        if(this.username == null) {
            this.username = this.getValue("user");

            if(this.username == null) {
                this.username = "unknown";
            }
        }
        return this.username;
    }
    
    protected ArrayList<String> setupIDList(String idString) {
        ArrayList<String> idList = new ArrayList<>();
        if (idString != null) {
            StringTokenizer st = new StringTokenizer(idString, ",");
            while (st.hasMoreTokens()) {
                idList.add(st.nextToken().trim());
            }
        }
        
        return idList;
    }
    
    boolean isEmpty() {
        return false;
    }

    boolean isNew() {
        return this.isNew;
    }
    
    boolean isChanged() {
        return this.changed;
    }
    
    boolean isDeleted() {
        return this.deleted;
    }
    
    boolean hasName() {
        return this.has_name;
    }
    
    Date getTimeStamp() {
        return this.tstampDate;
    }
    
    protected int addMember(OSMElement newElement, ArrayList memberList, ArrayList<String> idList, boolean setall) {
        String idString = newElement.getOSMIDString();
        
        int position = idList.indexOf(idString);
        while(position > -1) {
            /* pay attention! a node can be appeare more than once on a string!
            indexof would produce the smallest index each time. Thus, we have
            to overwrite each entry after its usage
             */
            idList.set(position, "-1");

            if (position > -1) {
                // add
                if (position > memberList.size() - 1) {
                    // list to short?? that's a failure
                    System.err.print("OHDMElement.addMember(): memberList must have same size as memberIDList.. run into exception");
                }

                memberList.set(position, newElement);
            } else {
                // position not found?? TODO
                System.err.print("OHDMElement.addMember(): member not found in id list - must not happen");
            }
            
            // a member can appear more than once.. set all slots?
            if(setall) {
                // find next position, if any
                position = idList.indexOf(idString);
            } else {
                // only one insert, we are done here
                return position;
            }
        }
        
        return position; // last position
    }
    
    boolean isPolygon() {
        return this.isPolygon;
    }
    
    boolean isConsistent(PrintStream p) {
        return true;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append(super.toString());
        
        sb.append("osmIDString: ");
        sb.append(osmIDString);
        sb.append("\t");
        
        sb.append("ohdmGeomIDString: ");
        sb.append(ohdmGeomIDString);
        sb.append("\t");
        
        sb.append("ohdmObjectIDString: ");
        sb.append(ohdmObjectIDString);
        sb.append("\t");
        
        sb.append("classCodeString: ");
        sb.append(classCodeString);
        sb.append("\t");
        
        sb.append("className: ");
        sb.append(className);
        sb.append("\t");
        
        sb.append("isPolygon: ");
        sb.append(isPolygon);
        sb.append("\n");
        
        sb.append("subClassCode: ");
        sb.append(subClassCode);
        sb.append("\t");
        
        sb.append("subClassName: ");
        sb.append(subClassName);
        sb.append("\t");
        
        sb.append("uid: ");
        sb.append(uid);
        sb.append("\t");
        
        sb.append("username: ");
        sb.append(username);
        sb.append("\t");
        
        sb.append("wktString: ");
        sb.append(wktString);
        sb.append("\t");
        
        sb.append("wktStringProduced: ");
        sb.append(wktStringProduced);
        sb.append("\t");
        
        return sb.toString();
    }

    Iterator<String> getOtherClassIDs() {
        return this.otherClassCodeList.iterator();
    }

    boolean noOHDMElement() {
        return (
            this.ohdmGeomIDString == null ||
            this.ohdmGeomIDString.length() == 0 ||
            this.ohdmObjectIDString == null  ||
            this.ohdmObjectIDString.length() == 0);
    }
}
