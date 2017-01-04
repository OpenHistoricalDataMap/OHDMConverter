package osm2inter;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author thsc
 */
public class OSMElement extends AbstractElement {

    private long id;
    private String osmIDString;

    OSMElement(HashMap<String, String> attributes) {
      this(attributes, null);
    }

//    OSMElement(String serializedAttributes, String serializedTags) {
//        super(serializedAttributes, serializedTags);
//    }
    
    OSMElement(HashMap<String, String> attributes, ArrayList<TagElement> tags) {
        super(attributes, tags);
        this.id = 0; // make compiler happy
        try {
            this.osmIDString = this.getAttributes().get("id");
            this.osmIDString = this.getAttributes().get("id");
            if (this.osmIDString != null) {
                this.id = Long.valueOf(this.osmIDString);
                // TODO: else part correct??
            } else if (this.getAttributes().get("ref") != null) {
                this.id = Long.valueOf(this.getAttributes().get("ref"));
            }
        } catch (NumberFormatException e) {
        }
    }

  void print() {
    this.getAttributes().entrySet().stream().forEach((entry) -> {
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
