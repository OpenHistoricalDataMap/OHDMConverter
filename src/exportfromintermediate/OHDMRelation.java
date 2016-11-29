package exportfromintermediate;

import java.math.BigDecimal;

/**
 *
 * @author thsc
 */
public class OHDMRelation extends OHDMElement {
    
    OHDMRelation(BigDecimal osmID, BigDecimal classCode, String sAttributes, String sTags, String nodeIDs, BigDecimal ohdmID, BigDecimal ohdmObjectID, boolean valid) {
        super(osmID, classCode, sAttributes, sTags, nodeIDs, ohdmID, ohdmObjectID, valid);
    }

    @Override
    String getWKTGeometry() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
