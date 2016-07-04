package osmupdatewizard;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 *
 * @author thsc
 */
public class OSMElement {

  protected final HashMap<String, String> attributes;
  private HashSet<TagElement> tags = null;

  private String id = null;
  private ElementStorage storage;

  OSMElement(HashMap<String, String> attributes) {
    this(attributes, null);
  }

  OSMElement(HashMap<String, String> attributes, HashSet<TagElement> tags) {
    this(null, attributes, tags);
  }

  OSMElement(ElementStorage storage, HashMap<String, String> attributes, HashSet<TagElement> tags) {
    this.storage = storage;
    this.attributes = attributes;
    this.tags = tags;
  }

  void print() {
    Iterator<String> kIter = this.attributes.keySet().iterator();
    while (kIter.hasNext()) {
      String k = kIter.next();
      String v = this.attributes.get(k);
      System.out.print("k|v: " + k + "|" + v + "\n");
    }
  }

  String getID() {
    if (id == null) {
      this.id = this.attributes.get("id");
    }

    return this.id;
  }

  ElementStorage getStorage() {
    return this.storage;
  }
  
  public HashSet<TagElement> getTags() {
    return this.tags;
  }
}
