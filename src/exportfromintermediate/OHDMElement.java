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
    private final BigDecimal osmID;
    private final int classCode;
    private int subClassCode;
    
    private final BigDecimal ohdmID;
    private final BigDecimal ohdmObjectID;
    private final boolean valid;
    
    protected boolean isPolygon = false;
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
    
    void setOHDM_ID(int ohdmID) throws SQLException {
        this.intermediateDB.setOHDM_ID(this, ohdmID);
    }
    
    /**
     * Remove this object from intermediate db .. use carefully!
     */
    void remove() throws SQLException {
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
        BigDecimal nodeOSMID = newElement.getOSMID();
        String idString = nodeOSMID.toString();
        
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
