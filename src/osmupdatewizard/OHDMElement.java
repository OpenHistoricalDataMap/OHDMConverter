package osmupdatewizard;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 *
 * @author thsc
 */
abstract class OHDMElement extends AbstractElement {
    private final BigDecimal osmID;
    private final BigDecimal classCode;
    private final BigDecimal ohdmID;
    private final BigDecimal ohdmObjectID;
    private final boolean valid;
    
    protected ArrayList<OHDMNode> nodes;
    private ArrayList<String> nodeIDList;
    
    private final String nodeIDs;

    OHDMElement(BigDecimal osmID, BigDecimal classCode, String sAttributes, String sTags, String nodeIDs, BigDecimal ohdmID, BigDecimal ohdmObjectID, boolean valid) {
        super(sAttributes, sTags);
        
        this.nodeIDs = nodeIDs;
        this.osmID = osmID;
        this.classCode = classCode;
        this.ohdmID = ohdmID;
        this.ohdmObjectID = ohdmObjectID;
        this.valid = valid;
    }
    
    abstract String getWKTGeometry();

    BigDecimal getOSMID() {
        return osmID;
    }

    void addNode(OHDMNode node) {
        if(this.nodes == null) {
            this.nodes = new ArrayList<>();
            
            // setup position list
            this.nodeIDList = new ArrayList<>();
            if(this.nodeIDs != null) {
                StringTokenizer st = new StringTokenizer(this.nodeIDs, ",");
                while(st.hasMoreTokens()) {
                    this.nodeIDList.add(st.nextToken());
                }
            }
        }
        
        BigDecimal nodeOSMID = node.getOSMID();
        String idString = nodeOSMID.toString();
        
        int position = this.nodeIDList.indexOf(idString);
        
        if(position > -1) {
            if(position > this.nodes.size()-1) {
                this.nodes.add(node);
            } else {
                this.nodes.add(position, node);
            }
        }
        else {
            // TODO!
        }
    }
    
    protected Iterator<OHDMNode> getNodeIter() {
        if(this.nodes == null) return null;
        
        return this.nodes.iterator();
    }
}
