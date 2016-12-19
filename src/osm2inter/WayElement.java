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

  public WayElement(ElementStorage storage, HashMap<String, String> attributes,
          ArrayList<NodeElement> nds, ArrayList<TagElement> tags) {
    super(storage, attributes, tags);
    this.nds = nds;
  }

  public ArrayList<NodeElement> getNodes(){
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
