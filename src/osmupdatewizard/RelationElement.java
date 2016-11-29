package osmupdatewizard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author thsc
 */
class RelationElement extends OSMElement {

  private final ArrayList<MemberElement> members;
  
  public RelationElement(HashMap<String, String> attributes,
          ArrayList<MemberElement> members, ArrayList<TagElement> tags) {
    super(attributes, tags);
    this.members = members;
  }

  public ArrayList<MemberElement> getMember() {
    return members;
  }
}
