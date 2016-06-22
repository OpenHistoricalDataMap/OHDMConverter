package osmupdatewizard;

import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author thsc
 */
class RelationElement extends OSMElement {
    private final HashSet<MemberElement> members;

    public RelationElement(HashMap<String, String> attributes, 
            HashSet<MemberElement> members, HashSet<TagElement> tags) {
        
        super(attributes, tags);
        
        this.members = members;
    }
    
}
