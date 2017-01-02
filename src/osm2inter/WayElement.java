package osm2inter;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author thsc
 */
class WayElement extends OSMElement {

  private final ArrayList<NodeElement> nds;

    public WayElement(HashMap<String, String> attributes, ArrayList<NodeElement> nds, ArrayList<TagElement> tags) {
        super(attributes, tags);
        this.nds = nds;
    }

    public ArrayList<NodeElement> getNodes(){
        return this.nds;
    }
  
  public boolean hasNodeId(long id) {
    boolean result = false;
    for (NodeElement nd : nds) {
      if (nd.getID() == id) {
        result = true;
      }
    }
    return result;
  }

  @Override
  public void print() {
    System.out.println("Way");
    super.print();
    System.out.println("Nodes");
    for (NodeElement nd : this.nds){
      nd.print();
    }
    /*Iterator<NodeElement> nodeIter = this.getNodes();
    while (nodeIter.hasNext()) {
      NodeElement node = nodeIter.next();
      node.print();
    }*/
    System.out.println("===========================");
  }

}
