package exportfromintermediate;

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
        
        if(this.isPolygone) {
            // it is a polygone: e.g. POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))
            // it cannot have an inner a hole - that's described by relations
            wkt.append("POLYGONE((");
            this.appendAllLongLat(wkt);
            // we don't store last duplicate node internally. Add it to the end
            OHDMNode firstNode = this.nodes.get(0);
            wkt.append(", ");
            this.appendAllLongLat(wkt, firstNode);
            wkt.append("))");
        } else {
            // linestring: e.g. LINESTRING (30 10, 10 30, 40 40)
            wkt.append("LINESTRING(");
            this.appendAllLongLat(wkt);
            wkt.append(")");
        }
        
        return wkt.toString();
    }
    
    private void appendAllLongLat(StringBuilder wkt) {
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
            
            this.appendAllLongLat(wkt, node);
        }
    }
    
    private void appendAllLongLat(StringBuilder wkt, OHDMNode node) {
            node.getLatitude();

            wkt.append(node.getLongitude());
            wkt.append(" ");
            wkt.append(node.getLatitude());
    }
}
