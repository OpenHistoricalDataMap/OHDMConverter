package exportfromintermediate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 *
 * @author thsc
 */
public class OHDMRelation extends OHDMElement {
    OHDMRelation(BigDecimal osmID, BigDecimal classCode, String sAttributes, String sTags, String nodeIDs, BigDecimal ohdmID, BigDecimal ohdmObjectID, boolean valid) {
        super(osmID, classCode, sAttributes, sTags, ohdmID, ohdmObjectID, valid);
    }

    @Override
    String getWKTGeometry() {
        return null;
    }

    @Override
    GeometryType getGeometryType() {
        return GeometryType.POLYGON;
    }
}
