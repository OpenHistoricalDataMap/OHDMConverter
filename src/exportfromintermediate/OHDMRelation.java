package exportfromintermediate;

import java.math.BigDecimal;
import java.util.ArrayList;

/**
 *
 * @author thsc
 */
public class OHDMRelation extends OHDMElement {
    private final String memberIDs;
    private ArrayList<OHDMElement> members;
    private ArrayList<String> memberRoles;
    private ArrayList<String> memberIDList;
    private ArrayList<String> roleMemberIDList;
    
    OHDMRelation(IntermediateDB intermediateDB, BigDecimal osmID, BigDecimal classCode, String sTags, String memberIDs, BigDecimal ohdmID, BigDecimal ohdmObjectID, boolean valid) {
        super(intermediateDB, osmID, classCode, null, sTags, ohdmID, ohdmObjectID, valid);
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
    
    void addMember(OHDMElement element, String roleName) {
        if (this.members == null) {
            this.members = new ArrayList<>();
            
            // setup position list
            this.memberIDList = this.setupIDList(this.memberIDs);
            this.memberRoles = new ArrayList<>();
        }

        int position = this.addMember(element, this.members, this.memberIDList);
        
        if(position > this.memberRoles.size() -1) {
            this.memberRoles.add(roleName);
        } else {
            this.memberRoles.add(position, roleName);
        }
    }
    
    public static final String INNER_ROLE = "inner";
    public static final String OUTER_ROLE = "outer";
    
    @Override
    boolean isPolygon() {
        // check roles
        for (String roleName : this.memberRoles) {
            if( !roleName.equalsIgnoreCase(OHDMRelation.INNER_ROLE)
                    && !roleName.equalsIgnoreCase(OHDMRelation.OUTER_ROLE)
            )
                return false;
        }
        
        return true;
    }
        
}
