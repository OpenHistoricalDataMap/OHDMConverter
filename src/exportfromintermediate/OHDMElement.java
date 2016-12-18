package exportfromintermediate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import osmupdatewizard.AbstractElement;
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
    private final String ohdmGeomIDString;
    
    private final boolean valid;
    
    protected boolean isPolygon = false;
    protected final IntermediateDB intermediateDB;
    private final boolean isPart;
    
    public static enum GeometryType {POINT, LINESTRING, POLYGON, RELATION};

    OHDMElement(IntermediateDB intermediateDB, String osmIDString, 
            String classCodeString, String sAttributes, String sTags, 
            String ohdmObjectIDString, String ohdmGeomIDString, boolean isPart, boolean valid) {
        
        super(sAttributes, sTags);

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
    
    abstract String getWKTGeometry();
    
    abstract GeometryType getGeometryType();
    
    void setOHDM_IDs(String ohdmObjectIDString, String ohdmGeomIDString, boolean persist) throws SQLException {
        if(persist) {
            this.intermediateDB.setOHDM_IDs(this, ohdmObjectIDString, ohdmGeomIDString);
        }
        
        this.ohdmObjectIDString = ohdmObjectIDString;
    }
    
    void setOHDM_IDs(String ohdmObjectIDString, String ohdmGeomIDString) throws SQLException {
        this.setOHDM_IDs(ohdmObjectIDString, ohdmGeomIDString, true);
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
            this.uid = this.findValue("uid");

            if(this.uid == null) {
                this.uid = "-1";
            }
        }
        
        return this.uid;
    }
    
    private String username = null;
    final String getUsername() {
        if(this.username == null) {
            this.username = this.findValue("user");

            if(this.username == null) {
                this.username = "unknown";
            }
        }
        return this.username;
    }
    
    final String findValue(String key) {
        String value = this.findValueInTags(key);
        if(value != null) return value;
        
        return this.attributes.get(key);
    }
    
    final String getType() {
        return this.findValue("type");
    }
    
    protected ArrayList<String> setupIDList(String idString) {
        ArrayList<String> idList = new ArrayList<>();
        if (idString != null) {
            StringTokenizer st = new StringTokenizer(idString, ",");
            while (st.hasMoreTokens()) {
                idList.add(st.nextToken());
            }
        }
        
        return idList;
    }
    
    protected int addMember(OHDMElement newElement, ArrayList memberList, ArrayList<String> idList) {
        String idString = newElement.getOSMIDString();
        
        int position = idList.indexOf(idString);
        /* pay attention! a node can be appeare more than once on a string!
        indexof would produce the smallest index each time. Thus, we have
        to overwrite each entry after its usage
         */
        idList.set(position, "-1");
        if (position > -1) {
            if (position > memberList.size() - 1) {
                memberList.add(newElement);
            } else {
                memberList.add(position, newElement);
            }
        } else {
            // TODO!
        }
        
        return position;
    }
    
    boolean isPolygon() {
        return this.isPolygon;
    }
    
}
