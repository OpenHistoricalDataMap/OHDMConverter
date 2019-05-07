package inter2ohdm;

import java.sql.Date;
import util.OHDM_DB;

/**
 *
 * @author thsc
 */
public class OSMNode extends OSMElement {
    private String longitude;
    private String latitude;

    OSMNode(IntermediateDB intermediateDB, String osmIDString, 
            String classCodeString, String otherClassCodes, String sTags, 
            String ohdmObjectIDString, String ohdmGeomIDString, 
            boolean valid,
            boolean geom_changed, boolean object_changed,
            boolean deleted,
            boolean has_name, Date tstampDate, boolean object_new) {
        
        super(intermediateDB, osmIDString, classCodeString, otherClassCodes, sTags, 
                ohdmObjectIDString, ohdmGeomIDString, valid,
                geom_changed, object_changed, deleted, has_name, tstampDate, object_new);
    }

    OSMNode(IntermediateDB intermediateDB, String osmIDString, 
            String classCodeString, String otherClassCodes, String sTags, String longitude, 
            String latitude, String ohdmObjectIDString, 
            String ohdmGeomIDString, boolean valid,
            boolean geom_changed, boolean object_changed,
            boolean deleted,
            boolean has_name, Date tstampDate, boolean object_new
    ) {
        
        
        this(intermediateDB, osmIDString, classCodeString, otherClassCodes, sTags, 
                ohdmObjectIDString, ohdmGeomIDString, valid,
                geom_changed, object_changed, deleted, has_name, tstampDate, object_new);
        
        this.longitude = longitude;
        this.latitude = latitude;
    }
    
    @Override
    protected void produceWKTGeometry() {
        StringBuilder sb = new StringBuilder("POINT(");
        sb.append(this.getLongitude());
        sb.append(" ");
        sb.append(this.getLatitude());
        sb.append(")");
        
        this.wktString = sb.toString();
        this.wktStringProduced = true;
    }
    
    String getWKTPointsOnly() {
        return this.getLongitude() + " " + this.getLatitude();
    }
    
    String getLongitude() {
        return this.longitude;
    }
    
    String getLatitude() {
        return this.latitude;
    }

    @Override
    int getGeometryType() {
        return OHDM_DB.POINT;
    }

    boolean identical(OSMNode node) {
        if(node == null) return false;
        return (
            node.getLatitude().equals(this.getLatitude()) &&
            node.getLongitude().equals(this.getLongitude())
        );
    }
}
