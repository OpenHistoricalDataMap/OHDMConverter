package exportfromintermediate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import osmupdatewizard.AbstractElement;
import osmupdatewizard.OSMClassification;

/**
 *
 * @author thsc
 */
abstract class OHDMElement extends AbstractElement {
    private final BigDecimal osmID;
    private final int classCode;
    private int subClassCode;
    
    private final BigDecimal ohdmID;
    private final BigDecimal ohdmObjectID;
    private final boolean valid;
    
    protected ArrayList<OHDMNode> nodes;
    private ArrayList<String> nodeIDList;
    
    private final String nodeIDs;
    protected boolean isPolygone;

    OHDMElement(BigDecimal osmID, BigDecimal classCode, String sAttributes, String sTags, String nodeIDs, BigDecimal ohdmID, BigDecimal ohdmObjectID, boolean valid) {
        super(sAttributes, sTags);
        
        this.nodeIDs = nodeIDs;
        this.osmID = osmID;
        this.classCode = classCode.intValue();
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
            
            // is it a ring
            String firstElement = this.nodeIDList.get(0);
            String lastElement = this.nodeIDList.get(this.nodeIDList.size()-1);
            
            if(firstElement.equalsIgnoreCase(lastElement)) {
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
    
    String validSince() {
        return "1970-01-01";
    }

    String validUntil() {
        return "2020-01-01";
    }
    
    int getClassCode() {
        return this.classCode;
    }
    
    private String className = null;
    private String subClassName = null;
    
    String getClassName() {
        if(className == null) {
            String fullClassName = OSMClassification.getOSMClassification().
                    getFullClassName(this.classCode);
        
            StringTokenizer st = new StringTokenizer(fullClassName, "_");
            this.className = st.nextToken();
            
            if(st.hasMoreTokens()) {
                this.subClassName = st.nextToken();
            } else {
                this.subClassName = "undefined";
            }
        }
        
        return this.className;
    }
  
    String getSubClassName() {
        if(this.subClassName == null) {
            this.getClassName();
        }
        
        return this.subClassName;
    }
}
