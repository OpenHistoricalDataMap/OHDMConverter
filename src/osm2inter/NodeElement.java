package osm2inter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author thsc
 */
public class NodeElement extends OSMElement {

  NodeElement(HashMap<String, String> attributes, ArrayList<TagElement> tags) {
    super(attributes, tags);
  }

  public String getLatitude() {
    return this.attributes.get("lat");
  }

  public String getLongitude() {
    return this.attributes.get("lon");
  }
}
