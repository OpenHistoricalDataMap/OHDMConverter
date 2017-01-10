package inter2ohdm;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import util.AbstractElement;
import osm.OSMClassification;

/**
 *
 * @author thsc
 */
public abstract class OHDMElement extends AbstractElement {
    private final String osmIDString;
    private final String classCodeString;
    private int subClassCode;
    
    private String ohdmObjectIDString;
    private String ohdmGeomIDString;
    
    private final boolean valid;
    
    protected boolean isPolygon = false;
    protected final IntermediateDB intermediateDB;
    private final boolean isPart;

    public static enum GeometryType {POINT, LINESTRING, POLYGON, RELATION};

    OHDMElement(IntermediateDB intermediateDB, String osmIDString, 
            String classCodeString, String sTags, 
            String ohdmObjectIDString, String ohdmGeomIDString, boolean isPart, boolean valid) {
        
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
        this.isPart = isPart;
        this.valid = valid;
    }
    
    /**
     * produce a clone: note: this is *not* a deep copy
     * @param orig
     * @return 
     */
    public OHDMElement clone(OHDMElement orig) {
        return null; // TODO
    }
    
    abstract String getWKTGeometry();
    
    abstract GeometryType getGeometryType();
    
    void setOHDM_IDs(String ohdmObjectIDString, String ohdmGeomIDString, boolean persist) throws SQLException {
        if(persist) {
            this.intermediateDB.setOHDM_IDs(this, ohdmObjectIDString, ohdmGeomIDString);
        }

        if(ohdmObjectIDString != null) {
            this.ohdmObjectIDString = ohdmObjectIDString;
        }
        
        if(ohdmGeomIDString != null) {
            this.ohdmGeomIDString = ohdmGeomIDString;
        }
    }
    
    void setOHDM_IDs(String ohdmObjectIDString, String ohdmGeomIDString) throws SQLException {
        this.setOHDM_IDs(ohdmObjectIDString, ohdmGeomIDString, true);
    }
    
    void setOHDMObjectID(String ohdmObjectIDString) throws SQLException {
        this.setOHDM_IDs(ohdmObjectIDString, null, true);
    }
    
    void setOHDMGeometryID(String geometryIDString) throws SQLException {
        this.setOHDM_IDs(null, geometryIDString, true);
    }
    
    String getOHDMObjectID() {
        return this.ohdmObjectIDString;
    }
    
    String getOHDMGeomID() {
        return this.ohdmGeomIDString;
    }
    
    boolean isPart() {
        return this.isPart;
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
    
    String validSince() {
        return "1970-01-01";
    }

    String validUntil() {
        return "2020-01-01";
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
    
    protected int addMember(OHDMElement newElement, ArrayList memberList, ArrayList<String> idList, boolean setall) {
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
    
    
}