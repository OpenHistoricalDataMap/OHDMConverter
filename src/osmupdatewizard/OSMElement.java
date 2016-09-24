package osmupdatewizard;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

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
      this.tagId = this.getClassificationTag();
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

  /**
   * Searches for a valid attribute, that is listed in the classification table.
   * If there is no valid one, null is returned, otherwiese the classcode (id)
   * of it.
   *
   * @return
   */
  private Integer getClassificationTag() {
    Integer tag = null;
    for (TagElement t : tags) {
      if (tag == null) {
        tag = t.getCTagFromAttr();
      }
    }
    return tag;
  }

  public Integer getCTagFromAttr() {
    Integer tag = null;
    for (Entry entry : attributes.entrySet()) {
      if (tag == null) {
      tag = Classification.getInstance().getClasscode(entry.getKey().toString(), entry.getValue().toString());}
    }
    return tag;
  }

  public Integer getTagId() {
    return this.tagId;
  }
}
