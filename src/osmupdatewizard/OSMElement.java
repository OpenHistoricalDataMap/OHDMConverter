package osmupdatewizard;

import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author thsc
 */
public class OSMElement {

  protected final HashMap<String, String> attributes;
  private HashSet<TagElement> tags = null;

  private long id = 0;

  OSMElement(HashMap<String, String> attributes) {
    this(attributes, null);
  }

  OSMElement(HashMap<String, String> attributes, HashSet<TagElement> tags) {
    this(null, attributes, tags);
  }

  OSMElement(ElementStorage storage, HashMap<String, String> attributes, HashSet<TagElement> tags) {
    this.attributes = attributes;
    this.tags = tags;
    try {
      if (id == 0) {
        if (this.attributes.get("id") != null) {
          this.id = Long.valueOf(this.attributes.get("id"));
        } else if (this.attributes.get("ref") != null) {
          this.id = Long.valueOf(this.attributes.get("ref"));
        }
      }
    } catch (NumberFormatException e) {
    }
  }

  void print() {
    attributes.entrySet().stream().forEach((entry) -> {
      System.out.print("k|v: " + entry.getKey() + "|" + entry.getValue() + "\n");
    });
  }

  public long getID() {
    return this.id;
  }

  public HashSet<TagElement> getTags() {
    return this.tags;
  }
}
