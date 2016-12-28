package inter2ohdm;

import java.math.BigDecimal;

/**
 *
 * @author thsc
 */
public class OHDMNode extends OHDMElement {
    private String longitude;
    private String latitude;

    OHDMNode(IntermediateDB intermediateDB, String osmIDString, String classCodeString, String sTags, String ohdmObjectIDString, String ohdmGeomIDString, boolean isPart, boolean valid) {
        super(intermediateDB, osmIDString, classCodeString, null, sTags, ohdmObjectIDString, ohdmGeomIDString, isPart, valid);
    }

    OHDMNode(IntermediateDB intermediateDB, String osmIDString, String classCodeString, String sTags, String longitude, String latitude, String ohdmObjectIDString, String ohdmGeomIDString, boolean isPart, boolean valid) {
        this(intermediateDB, osmIDString, classCodeString, sTags, ohdmObjectIDString, ohdmGeomIDString, isPart,valid);
        
        this.longitude = longitude;
        this.latitude = latitude;
    }
    
    @Override
    String getWKTGeometry() {
        StringBuilder sb = new StringBuilder("POINT(");
        sb.append(this.getLatitude());
        sb.append(" ");
        sb.append(this.getLongitude());
        sb.append(")");
        
        return sb.toString();
    }
    
    String getLongitude() {
        return this.longitude;
    }
    
    String getLatitude() {
        return this.latitude;
    }

    @Override
    GeometryType getGeometryType() {
        return GeometryType.POINT;
    }

    boolean identical(OHDMNode node) {
        if(node == null) return false;
        return (
            node.getLatitude().equals(this.getLatitude()) &&
            node.getLongitude().equals(this.getLongitude())
        );
    }
}
