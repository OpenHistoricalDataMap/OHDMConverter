package osmupdatewizard;

import java.util.HashMap;

/**
 *
 * @author thsc
 */
public class TagElement extends OSMElement {
    
    TagElement(HashMap<String, String> attributes) {
        super(attributes);
    }
    
    @Override
    String getSerializedTags() {
        return this.getSerializeAttributes();
    }
}
