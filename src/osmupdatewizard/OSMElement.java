package osmupdatewizard;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author thsc
 */
public class OSMElement {

  protected final HashMap<String, String> attributes;
  private HashSet<TagElement> tags = null;
  private Integer tagId = null;

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
    this.tags.stream().forEach((t) -> {
      t.attributes.entrySet().stream().filter((entry) -> (Whitelist.getInstance().reduce(entry.getKey(), entry.getValue()) != null)).forEach((entry) -> {
        this.tagId = Whitelist.getInstance().getId(entry.getKey(), entry.getValue());
      });
    });
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

  public Integer getTagId() {
    return this.tagId;
  }

}
