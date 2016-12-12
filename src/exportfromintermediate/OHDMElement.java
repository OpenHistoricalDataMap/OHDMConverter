package exportfromintermediate;

import java.math.BigDecimal;
import java.util.StringTokenizer;
import osmupdatewizard.AbstractElement;
import osm.OSMClassification;

/**
 *
 * @author thsc
 */
public abstract class OHDMElement extends AbstractElement {
    private final BigDecimal osmID;
    private final int classCode;
    private int subClassCode;
    
    private final BigDecimal ohdmID;
    private final BigDecimal ohdmObjectID;
    private final boolean valid;
    
    protected boolean isPolygone;
    private final IntermediateDB intermediateDB;
    
    public static enum GeometryType {POINT, LINESTRING, POLYGON, RELATION};

    OHDMElement(IntermediateDB intermediateDB, BigDecimal osmID, 
            BigDecimal classCode, String sAttributes, String sTags, 
            BigDecimal ohdmID, BigDecimal ohdmObjectID, boolean valid) {
        
        super(sAttributes, sTags);

        this.intermediateDB = intermediateDB;
        this.getUserID();
        this.getUsername();
        this.osmID = osmID;
        this.classCode = classCode.intValue();
        this.ohdmID = ohdmID;
        this.ohdmObjectID = ohdmObjectID;
        this.valid = valid;
    }
    
    abstract String getWKTGeometry();
    
    abstract GeometryType getGeometryType();
    
    void setOHDM_ID(int ohdmID) {
        this.intermediateDB.setOHDM_ID(this, ohdmID);
    }
    
    /**
     * Remove this object from intermediate db .. use carefully!
     */
    void remove() {
        this.intermediateDB.remove(this);
    }
    
    BigDecimal getOSMID() {
        return osmID;
    }
    
    String validSince() {
        return "1970-01-01";
    }

    String validUntil() {
        return "2020-01-01";
    }
    
    int getClassCode() {
        return this.classCode;
    }
    
    private String className = null;
    private String subClassName = null;
    
    String getClassName() {
        if(className == null) {
            String fullClassName = OSMClassification.getOSMClassification().
                    getFullClassName(this.classCode);
        
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
            this.uid = this.findValueInTags("uid");
            if(this.uid == null) {
                this.uid = this.attributes.get("uid");
            }

            if(this.uid == null) {
                this.uid = "-1";
            }
        }
        
        return this.uid;
    }
    
    private String username = null;
    final String getUsername() {
        if(this.username == null) {
            this.username = this.findValueInTags("user");
            if(this.username == null) {
                this.username = this.attributes.get("user");
            }

            if(this.username == null) {
                this.username = "unknown";
            }
        }
        return this.username;
    }
}
