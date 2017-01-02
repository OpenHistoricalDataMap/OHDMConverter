package osm2inter;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author thsc
 */
public class OSMElement extends AbstractElement {

    private long id = 0;
    private String osmIDString;

    OSMElement(HashMap<String, String> attributes) {
      this(attributes, null);
    }

    OSMElement(String serializedAttributes, String serializedTags) {
        super(serializedAttributes, serializedTags);
    }
    
    OSMElement(HashMap<String, String> attributes, ArrayList<TagElement> tags) {
        super(attributes, tags);
        try {
            if (id == 0) {
                this.osmIDString = this.attributes.get("id");
                if (this.osmIDString != null) {
                    this.id = Long.valueOf(this.osmIDString);
                    // TODO: else part correct??
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
    
    public String getOSMID() {
        return this.osmIDString;
    }
}
