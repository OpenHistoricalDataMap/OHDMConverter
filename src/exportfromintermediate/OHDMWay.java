package exportfromintermediate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 *
 * @author thsc
 */
public class OHDMWay extends OHDMElement {
    private ArrayList<OHDMNode> nodes;
    private ArrayList<String> nodeIDList;
    private final String nodeIDs;

    OHDMWay(IntermediateDB intermediateDB, BigDecimal osmID, BigDecimal classCode, String sTags, String nodeIDs, BigDecimal ohdmID, BigDecimal ohdmObjectID, boolean valid) {
        // handle tags as attributes..
        super(intermediateDB, osmID, classCode, null, sTags, ohdmID, ohdmObjectID, valid);
        this.nodeIDs = nodeIDs;
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
            wkt.append("POLYGON((");
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
    
    protected Iterator<OHDMNode> getNodeIter() {
        if(this.nodes == null) return null;
        
        return this.nodes.iterator();
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

    @Override
    GeometryType getGeometryType() {
        if(this.isPolygone) {
            return GeometryType.POLYGON;
        } else {
            return GeometryType.LINESTRING;
        }
    }

    void addNode(OHDMNode node) {
        if (this.nodes == null) {
            this.nodes = new ArrayList<>();
            // setup position list
            this.nodeIDList = new ArrayList<>();
            if (this.nodeIDs != null) {
                StringTokenizer st = new StringTokenizer(this.nodeIDs, ",");
                while (st.hasMoreTokens()) {
                    this.nodeIDList.add(st.nextToken());
                }
            }
            // is it a ring
            String firstElement = this.nodeIDList.get(0);
            String lastElement = this.nodeIDList.get(this.nodeIDList.size() - 1);
            if (firstElement.equalsIgnoreCase(lastElement)) {
                this.isPolygone = true;
            }
        }
        BigDecimal nodeOSMID = node.getOSMID();
        String idString = nodeOSMID.toString();
        int position = this.nodeIDList.indexOf(idString);
        /* pay attention! a node can be appeare more than once on a string!
        indexof would produce the smallest index each time. Thus, we have
        to overwrite each entry after its usage
         */
        this.nodeIDList.set(position, "-1");
        if (position > -1) {
            if (position > this.nodes.size() - 1) {
                this.nodes.add(node);
            } else {
                this.nodes.add(position, node);
            }
        } else {
            // TODO!
        }
    }
}
