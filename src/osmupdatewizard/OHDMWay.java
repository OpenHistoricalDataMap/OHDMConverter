package osmupdatewizard;

import java.math.BigDecimal;
import java.util.Iterator;

/**
 *
 * @author thsc
 */
public class OHDMWay extends OHDMElement {

    OHDMWay(BigDecimal osmID, BigDecimal classCode, String sTags, String nodeIDs, BigDecimal ohdmID, BigDecimal ohdmObjectID, boolean valid) {
        super(osmID, classCode, null, sTags, nodeIDs, ohdmID, ohdmObjectID, valid);
    }

    @Override
    String getWKTGeometry() {
        if(this.nodes == null || this.nodes.size() == 0) {
            return "";
        }
        
        StringBuilder wkt = new StringBuilder();
        
        // get first and last node
        OHDMNode firstNode = this.nodes.get(0);
        OHDMNode lastNode = this.nodes.get(this.nodes.size()-1);

        if(firstNode.getOSMID() == lastNode.getOSMID()) {
            // it is a polygone: e.g. POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))
            // it cannot have an inner a hole - that's described by relations
            wkt.append("POLYGONE((");
            this.appendLongLat(wkt);
            wkt.append("))");
        } else {
            // linestring: e.g. LINESTRING (30 10, 10 30, 40 40)
            wkt.append("LINESTRING(");
            this.appendLongLat(wkt);
            wkt.append(")");
        }
        
        return wkt.toString();
    }
    
    private void appendLongLat(StringBuilder wkt) {
        Iterator<OHDMNode> nodeIter = this.getNodeIter();
        boolean first = true;
        while(nodeIter.hasNext()) {
            if(first) {
                first = false;
            } else {
                wkt.append(", ");
            }

            OHDMNode node = nodeIter.next();
            node.getLatitude();

            wkt.append(node.getLongitude());
            wkt.append(" ");
            wkt.append(node.getLatitude());
        }
    }
}
