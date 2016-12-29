package inter2ohdm;

import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author thsc
 */
public class OHDMWay extends OHDMElement {
    private ArrayList<OHDMNode> nodes;
    private ArrayList<String> nodeIDList;
    private final String nodeIDs;

    OHDMWay(IntermediateDB intermediateDB, String osmIDString, String classCodeString, String sTags, String nodeIDs, String ohdmObjectIDString, String ohdmGeomIDString, boolean isPart, boolean valid) {
        // handle tags as attributes..
        super(intermediateDB, osmIDString, classCodeString, null, sTags, ohdmObjectIDString, ohdmGeomIDString, isPart, valid);
        this.nodeIDs = nodeIDs;
    }

    @Override
    String getWKTGeometry() {
        if(this.nodes == null || this.nodes.size() == 0) {
            return "";
        }
        
        StringBuilder wkt = new StringBuilder();
        
        if(this.isPolygon) {
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
    
    String getWKTPointsOnly() {
        if(this.nodes == null || this.nodes.size() == 0) {
            return "";
        }
        
        StringBuilder wkt = new StringBuilder();
        
        if(this.isPolygon) {
            // it is a polygone: e.g. POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))
            // it cannot have an inner a hole - that's described by relations
            // wkt.append("(");
            this.appendAllLongLat(wkt);
            // we don't store last duplicate node internally. Add it to the end
            OHDMNode firstNode = this.nodes.get(0);
            wkt.append(", ");
            this.appendAllLongLat(wkt, firstNode);
            // wkt.append(")");
        } else {
            // linestring: e.g. LINESTRING (30 10, 10 30, 40 40)
            // wkt.append("(");
            this.appendAllLongLat(wkt);
            // wkt.append(")");
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
//            node.getLatitude();
            
            this.appendAllLongLat(wkt, node);
        }
    }
    
    private void appendAllLongLat(StringBuilder wkt, OHDMNode node) {
//            node.getLatitude();

            wkt.append(node.getLatitude());
            wkt.append(" ");
            wkt.append(node.getLongitude());
    }

    @Override
    GeometryType getGeometryType() {
        if(this.isPolygon) {
            return GeometryType.POLYGON;
        } else {
            return GeometryType.LINESTRING;
        }
    }

    void addNode(OHDMNode node) {
        if (this.nodes == null) {
            // setup position list
            this.nodeIDList = this.setupIDList(this.nodeIDs);

            // is it a ring?
            String firstElement = this.nodeIDList.get(0);
            String lastElement = this.nodeIDList.get(this.nodeIDList.size() - 1);
            if (firstElement.equalsIgnoreCase(lastElement)) {
                this.isPolygon = true;
            }
            
            // setup node list
            /* if this is a polygon, one slot can be spared because copy of
            first node is not kept in this list
            */
            int length = this.isPolygon ? this.nodeIDList.size() - 1 : this.nodeIDList.size();
            this.nodes = new ArrayList<>(length);
            
            // dummy must be added..
            for(int i = 0; i < length; i++) {
                this.nodes.add(null);
            }
            
        }
        
        this.addMember(node, this.nodes, this.nodeIDList);
    }

    OHDMNode getLastPoint() {
        if(this.nodes == null || this.nodes.size() < 1) return null;
        
        if(this.isPolygon) {
            // last point is first point in a polygon
            return this.nodes.get(0);
        } else {
            return this.nodes.get(this.nodes.size()-1);
        }
    }
}
