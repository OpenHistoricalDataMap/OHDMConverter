package osmupdatewizard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author thsc
 */
class WayElement extends OSMElement {

  private final HashSet<NodeElement> nds;

  public WayElement(HashMap<String, String> attributes, HashSet<NodeElement> nds, HashSet<TagElement> tags) {
    super(attributes, tags);
    this.nds = nds;
  }

  public WayElement(ElementStorage storage, HashMap<String, String> attributes,
          HashSet<NodeElement> nds, HashSet<TagElement> tags) {
    super(storage, attributes, tags);
    this.nds = nds;
  }

  public HashSet<NodeElement> getNodes(){
    return this.nds;
  }
  
  /*public Iterator<NodeElement> getNodes() {
    List<NodeElement> nodes = new ArrayList<>();
    ElementStorage storage = this.getStorage();

    // fill list with real nodes instead of placeholders
    Iterator<NodeElement> ndIter = this.nds.iterator();
    while (ndIter.hasNext()) {
      NodeElement nd = ndIter.next();
      NodeElement node = storage.getNodeByID(String.valueOf(nd.getID()));
      nodes.add(node);
    }
    return nodes.iterator();
  } */

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
