package exportfromintermediate;

import java.math.BigDecimal;

/**
 *
 * @author thsc
 */
public class OHDMNode extends OHDMElement {
    private String longitude;
    private String latitude;

    OHDMNode(BigDecimal osmID, BigDecimal classCode, String sTags, BigDecimal ohdmID, BigDecimal ohdmObjectID, boolean valid) {
        super(osmID, classCode, null, sTags, null, ohdmID, ohdmObjectID, valid);
    }

    OHDMNode(BigDecimal osmIDBig, BigDecimal classCodeBig, String sTags, String longitude, String latitude, BigDecimal ohdmIDBig, BigDecimal ohdmObjectIDBig, boolean valid) {
        this(osmIDBig, classCodeBig, sTags, ohdmIDBig, ohdmObjectIDBig, valid);
        
        this.longitude = longitude;
        this.latitude = latitude;
    }
    
    @Override
    String getWKTGeometry() {
        StringBuilder sb = new StringBuilder("POINT(");
        sb.append(this.getLongitude());
        sb.append(" ");
        sb.append(this.getLatitude());
        sb.append(")");
        
        return sb.toString();
    }
    
    String getLongitude() {
        return this.longitude;
    }
    
    String getLatitude() {
        return this.latitude;
    }
}
