package osmupdatewizard;

import java.util.HashMap;

/**
 *
 * @author thsc
 */
public class MemberElement extends OSMElement {

    private String type;
    private String id;
    private String role;

    MemberElement(HashMap<String, String> attributes) {
        super(attributes);
        attributes.entrySet().stream().forEach((entry) -> {
          if (entry.getKey().equalsIgnoreCase("type")) {
            type = entry.getValue();
          } else if (entry.getKey().equalsIgnoreCase("ref")) {
            id = entry.getValue();
          } else if (entry.getKey().equalsIgnoreCase("role")) {
            role = entry.getValue();
          } else {
            type = "null";
          }
        });
    }

    public String getType() {
        return this.type;
    }

    public String getId() {
        return this.id;
    }

    public String getRole() {
        return this.role;
    }
}
