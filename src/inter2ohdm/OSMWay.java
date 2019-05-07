package inter2ohdm;

import java.io.PrintStream;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import util.OHDM_DB;

/**
 *
 * @author thsc
 */
public class OSMWay extends OSMElement {
    private ArrayList<OSMNode> nodes;
    private ArrayList<String> nodeIDList;
    private final String nodeIDs;

    OSMWay(IntermediateDB intermediateDB, String osmIDString,
           String classCodeString, String otherClassCodes, String sTags, String nodeIDs,
           String ohdmObjectIDString, String ohdmGeomIDString,
           boolean valid,
           boolean geom_changed, boolean object_changed, boolean deleted,
           boolean has_name, Date tstampDate, boolean object_new) {
        
        // handle tags as attributes..
        super(intermediateDB, osmIDString, classCodeString, otherClassCodes, sTags, 
                ohdmObjectIDString, ohdmGeomIDString, valid,
                geom_changed, object_changed, deleted, has_name, tstampDate, object_new);
        
        this.nodeIDs = nodeIDs;
    }
    
    @Override
    boolean isConsistent(PrintStream p) {
        if(this.isEmpty()) {
            
            p.println("isConsistent: way has no nodes in lists");
            return false;
        }
        
        int i = this.isPolygon ? 1 : 0;
        if(this.nodes.size() + i != this.nodeIDList.size()) {
            p.println("isConsistent: nodes list and nodes id list have different length");
            return false;
        }
        
        return super.isConsistent(p);
    }
    
    @Override
    boolean isEmpty() {
        return(this.nodeIDList == null || this.nodeIDList.isEmpty() 
                || this.nodes == null || this.nodes.isEmpty());
        
    }
    
    @Override
    protected void produceWKTGeometry() {
        if(this.nodes == null || this.nodes.isEmpty()) {
            this.wktStringProduced = true;
            return;
        }
        
        StringBuilder wkt = new StringBuilder();
        
        if(this.isPolygon) {
            // it is a polygone: e.g. POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))
            // it cannot have an inner a hole - that's described by relations
            wkt.append("POLYGON((");
            this.appendAllLongLat(wkt);
            // we don't store last duplicate node internally. Add it to the end
            OSMNode firstNode = this.nodes.get(0);
            wkt.append(", ");
            this.appendAllLongLat(wkt, firstNode);
            wkt.append("))");
        } else {
            // linestring: e.g. LINESTRING (30 10, 10 30, 40 40)
            wkt.append("LINESTRING(");
            this.appendAllLongLat(wkt);
            wkt.append(")");
        }
        
        this.wktString = wkt.toString();
        this.wktStringProduced = true;
    }
    
    String getWKTPointsOnly() {
        if(this.nodes == null || this.nodes.isEmpty()) {
            return "";
        }
        
        StringBuilder wkt = new StringBuilder();
        
        if(this.isPolygon) {
            // it is a polygone: e.g. POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))
            // it cannot have an inner a hole - that's described by relations
            // wkt.append("(");
            this.appendAllLongLat(wkt);
            // we don't store last duplicate node internally. Add it to the end
            OSMNode firstNode = this.nodes.get(0);
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
    
    protected Iterator<OSMNode> getNodeIter() {
        if(this.nodes == null) return null;
        
        return this.nodes.iterator();
    }
    
    private void appendAllLongLat(StringBuilder wkt) {
        Iterator<OSMNode> nodeIter = this.getNodeIter();
        boolean first = true;
        while(nodeIter.hasNext()) {
            if(first) {
                first = false;
            } else {
                wkt.append(", ");
            }

            OSMNode node = nodeIter.next();
//            node.getLatitude();
            
            this.appendAllLongLat(wkt, node);
        }
    }
    
    private void appendAllLongLat(StringBuilder wkt, OSMNode node) {
//            node.getLatitude();

            wkt.append(node.getLongitude());
            wkt.append(" ");
            wkt.append(node.getLatitude());
    }

    @Override
    int getGeometryType() {
        if(this.isPolygon) {
            return OHDM_DB.POLYGON;
        } else {
            return OHDM_DB.LINESTRING;
        }
    }
    
    /**
     * return iterator of all nodes which make up that way
     * @return 
     */
    public Iterator<String> getNodeIDs() {
        if(this.nodeIDs == null | this.nodeIDs.length() < 1) return null;
        
        return this.setupIDList(this.nodeIDs).iterator();
    }

    void addNode(OSMNode node) {
        if (this.nodes == null) {
            // setup position list
            this.nodeIDList = this.setupIDList(this.nodeIDs);

            // is it a ring?
            String firstElement = this.nodeIDList.get(0);
            String lastElement = this.nodeIDList.get(this.nodeIDList.size() - 1);
            if (firstElement.equalsIgnoreCase(lastElement)) {
                /*
                there are a number cases in which a way has three (!) nodes and 
                first and third are identical. It a stroke from first to 
                second and back. Thats not a polygon.
                */
                if(this.nodeIDList.size() == 3) {
                    ArrayList<String> realList = new ArrayList<>();
                    realList.add(this.nodeIDList.get(0));
                    realList.add(this.nodeIDList.get(1));
                    this.nodeIDList = realList;
                } else {
                    this.isPolygon = true;
                    // remove last entry in idlist
                    this.nodeIDList.set(this.nodeIDList.size()-1, "-1");
                }
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
        
        this.addMember(node, this.nodes, this.nodeIDList, true);
    }

    OSMNode getLastPoint() {
        if(this.nodes == null || this.nodes.size() < 1) return null;
        
        if(this.isPolygon) {
            // last point is first point in a polygon
            return this.nodes.get(0);
        } else {
            return this.nodes.get(this.nodes.size()-1);
        }
    }
    
    List<OSMElement> getNodesWithIdentity() {
        if(this.nodes == null || this.nodes.isEmpty()) return null;
        
        ArrayList<OSMElement> iNodes = null;
        
        for(OSMNode n : this.nodes) {
            // has identity?
            if( n.hasOHDMObjectID() ) {
                if(iNodes == null) {
                    iNodes = new ArrayList<>();
                }
                iNodes.add(n);
            }
        }
        
        return iNodes;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        
        sb.append("\n");
        sb.append("nodes.size()");
        sb.append(this.nodes.size());
        sb.append("\t");
        sb.append("nodeIDList.size()");
        sb.append(this.nodeIDList.size());
        sb.append("\t");
        
        return sb.toString();
    }
    
}
