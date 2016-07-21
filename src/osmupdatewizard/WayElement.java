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

  private final HashSet<NDElement> nds;

  public WayElement(ElementStorage storage, HashMap<String, String> attributes,
          HashSet<NDElement> nds, HashSet<TagElement> tags) {
    super(storage, attributes, tags);
    this.nds = nds;
  }

  public Iterator<NodeElement> getNodes() {
    List<NodeElement> nodes = new ArrayList<>();
    ElementStorage storage = this.getStorage();

    // fill list with real nodes instead of placeholders
    Iterator<NDElement> ndIter = this.nds.iterator();
    while (ndIter.hasNext()) {
      NDElement nd = ndIter.next();
      NodeElement node = storage.getNodeByID(nd.getID());
      nodes.add(node);
    }
    return nodes.iterator();
  }

  @Override
  public void print() {
    System.out.println("Way");
    super.print();
    System.out.println("Nodes");
    Iterator<NodeElement> nodeIter = this.getNodes();
    while (nodeIter.hasNext()) {
      NodeElement node = nodeIter.next();
      node.print();
    }
    System.out.println("===========================");
  }

}
