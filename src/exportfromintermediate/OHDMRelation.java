package exportfromintermediate;

import java.math.BigDecimal;

/**
 *
 * @author thsc
 */
public class OHDMRelation extends OHDMElement {
    private final String memberIDs;
    
    OHDMRelation(BigDecimal osmID, BigDecimal classCode, String sTags, String memberIDs, BigDecimal ohdmID, BigDecimal ohdmObjectID, boolean valid) {
        super(osmID, classCode, null, sTags, ohdmID, ohdmObjectID, valid);
        this.memberIDs = memberIDs;
    }

    @Override
    String getWKTGeometry() {
        return null;
    }

    @Override
    GeometryType getGeometryType() {
        return GeometryType.POLYGON;
    }
    
    // sshould actually never be called..
    void addMember(OHDMElement element, String roleName) {
        if(element instanceof OHDMNode) {
            this.addMember((OHDMNode) element, roleName);
        } else if (element instanceof OHDMWay) {
            this.addMember((OHDMWay) element, roleName);
        } else {
            this.addMember((OHDMRelation) element, roleName);
        }
    }
    
    void addMember(OHDMNode node, String roleName) {
        
    }
    
    void addMember(OHDMWay way, String roleName) {
        
    }
    
    void addRelation(OHDMRelation relation, String roleName) {
        
    }
}
