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
    protected String getSerializedTagsAndAttributes() {
        return this.getSerializeAttributes();
    }
}
