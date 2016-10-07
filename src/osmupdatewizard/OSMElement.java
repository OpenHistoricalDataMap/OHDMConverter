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
    if (this.id != 0) {

      this.findClassificationTag();
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
  private void findClassificationTag() {
    if (this.tags != null) {
      this.tags.stream().forEach((t) -> {
        t.attributes.entrySet().stream().filter((entry) -> (Classification.getInstance().getClasscode(entry.getKey(), entry.getValue()) != null)).forEach((entry) -> {
          this.tagId = Classification.getInstance().getClasscode(entry.getKey(), entry.getValue());
        });
      });
    }
  }

  public Integer getTagId() {
    return this.tagId;
  }
}
