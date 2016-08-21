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
  private Integer tagId = null;

  private long id = 0;
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
    if (this.tags != null) {
      this.tags.stream().forEach((t) -> {
        t.attributes.entrySet().stream().filter((entry) -> (Whitelist.getInstance().reduce(entry.getKey(), entry.getValue()) != null)).forEach((entry) -> {
          this.tagId = Whitelist.getInstance().getId(entry.getKey(), entry.getValue());
        });
      });
    }
  }

  void print() {
    attributes.entrySet().stream().forEach((entry) -> {
      System.out.print("k|v: " + entry.getKey() + "|" + entry.getValue() + "\n");
    });
    /*Iterator<String> kIter = this.attributes.keySet().iterator();
     while (kIter.hasNext()) {
     String k = kIter.next();
     String v = this.attributes.get(k);
     System.out.print("k|v: " + k + "|" + v + "\n");
     }*/
  }

  public long getID() {
    if (id == 0) {
      if (this.attributes.get("id") != null) {
        this.id = Long.valueOf(this.attributes.get("id"));
      } else {
        this.id = Long.valueOf(this.attributes.get("ref"));
      }
    }
    return this.id;
  }

  ElementStorage getStorage() {
    return this.storage;
  }

  public HashSet<TagElement> getTags() {
    return this.tags;
  }

  public Integer getTagId() {
    return this.tagId;
  }
}
