package osm2inter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 *
 * @author thsc
 */
public abstract class AbstractElement {
    public final HashMap<String, String> attributes;
    public ArrayList<TagElement> tags = null;
    
    public AbstractElement(HashMap<String, String> attributes, ArrayList<TagElement> tags) {
        this.attributes = attributes;
        this.tags = tags;
    }

    public AbstractElement(String serializedAttributes, String serializedTags) {
        this.attributes = this.deserializeAttributes(serializedAttributes);
        this.tags = this.deserializeTags(serializedTags);
    }
  
    protected String serializeAttributes(HashMap<String, String> attributes) {
        if(attributes == null || attributes.isEmpty()) return this.getStringWithLength(null);

        StringBuilder sAttributes = new StringBuilder();

        Iterator<String> keyIter = attributes.keySet().iterator();
        while(keyIter.hasNext()) {
            String key = keyIter.next();
            sAttributes.append(this.getStringWithLength(key));

            String value = attributes.get(key);
            sAttributes.append(this.getStringWithLength(value));
        }

        return sAttributes.toString();

    }

    protected final HashMap<String, String> deserializeAttributes(String serializedAttributes) {
        HashMap<String, String> a = new HashMap<>();
        
        if(emptySerialization(serializedAttributes))return a;
        
        int index = 0;
        while(index < serializedAttributes.length()) {
            String key = this.unwrapStringWithLength(serializedAttributes, index);
            index += this.calculateOffsetFromUnwrappedString(key);
            
            String value = this.unwrapStringWithLength(serializedAttributes, index);
            index += this.calculateOffsetFromUnwrappedString(value);
            
            a.put(key, value);
        }
        
        return a;
    }

    protected String getSerializeAttributes() {
        return this.serializeAttributes(this.attributes);
    }
    
    private boolean emptySerialization(String serializedThing) {
        return (serializedThing == null || 
                serializedThing.length() == 0 ||
                serializedThing.equalsIgnoreCase("0000"));
    }

    protected final ArrayList<TagElement> deserializeTags(String serializedTags) {
        ArrayList<TagElement> t = new ArrayList<>();
        
        if(emptySerialization(serializedTags)) return t;
        
        int index = 0;
        TagElement newTag;
        while(index < serializedTags.length()) {
            String sAttributes = this.unwrapStringWithLength(serializedTags, index);
            index += this.calculateOffsetFromUnwrappedString(sAttributes);
            
            newTag = new TagElement(this.deserializeAttributes(sAttributes));
            t.add(newTag);
        }
        return t;
    }
    
    public static String[] relevantAttributeKeys = new String[] {"uid", "user"};
    
    protected HashMap<String, String> relevantAttributes = null;
    HashMap<String, String> getRelevantAttributes() {
        if(this.relevantAttributes != null) {
            return this.relevantAttributes;
        }
        
        this.relevantAttributes = new HashMap<>();
    
        for (String key : AbstractElement.relevantAttributeKeys) {
            String value = this.attributes.get(key);
            
            if(value != null) {
                this.relevantAttributes.put(key, value);
            }
        }
        
        return this.relevantAttributes;
    }
  
    protected String getSerializedTagsAndAttributes() {
        StringBuilder sTagAttr = new StringBuilder();
        
        // attributes first - take only relevant attributes
        HashMap<String, String> relAttributes = this.getRelevantAttributes();
        if(relAttributes != null && !relAttributes.isEmpty()) {
            String sAttributes = this.serializeAttributes(relAttributes);
            sTagAttr.append(this.getStringWithLength(sAttributes));
        }
        
        // now tags
        if(this.tags != null && !this.tags.isEmpty()) {
            Iterator<TagElement> tagIter = this.tags.iterator();

            while(tagIter.hasNext()) {
                TagElement tag = tagIter.next();

                String sAttributes = this.serializeAttributes(tag.attributes);

                sTagAttr.append(this.getStringWithLength(sAttributes));
            }
        }

        if(sTagAttr.length() < 1) {
            return this.getStringWithLength(null);
        } 
        
        // else: non empty tag / attr list
        return sTagAttr.toString();
    }
  
    protected String getStringWithLength(String s) {
        if(s == null || s.length() == 0) {
            return "0000";
        }
        
        // extract ' signs
        StringTokenizer st = new StringTokenizer(s, "'");
        if (st.countTokens() > 1) {
            StringBuilder newS = new StringBuilder();
            while(st.hasMoreTokens()) {
                newS.append(st.nextToken());
            }
            
            s = newS.toString();
        }
        
        int length = s.length();
        if(length >= HIGHEST_NUMBER_PLUS_ONE) return "0000";

        StringBuilder b = new StringBuilder();

        String lString = Integer.toString(length);
        int hugeNumber = HIGHEST_NUMBER_PLUS_ONE / 10;
        
        while(hugeNumber > 1) {
            if(length >= hugeNumber) {
                break;
            } else {
                b.append("0");
            }
            
            hugeNumber /= 10;
        }
        
        b.append(lString);

        b.append(s);

        return b.toString();
    }
    
    protected String unwrapStringWithLength(String s) {
        return this.unwrapStringWithLength(s, 0);
    }
    
    protected int calculateOffsetFromUnwrappedString(String unwrappedString) {
        return unwrappedString.length() + MAX_DECIMAL_PLACES;
    }
    
    private static final int MAX_DECIMAL_PLACES = 3;
    private static final int HIGHEST_NUMBER_PLUS_ONE = (int) Math.pow(10, MAX_DECIMAL_PLACES);
    
    protected String unwrapStringWithLength(String s, int offset) {
        if(s == null || s.length() - offset < MAX_DECIMAL_PLACES) return null;
        
        String lString = s.substring(offset, offset + MAX_DECIMAL_PLACES);
        
        try {
            int length = Integer.parseInt(lString);
            offset += MAX_DECIMAL_PLACES; // move over length entry
            
            String result = s.substring(offset, offset + length);
            
            return result;
        }
        catch(RuntimeException e) {
            return null;
        }
    }
    
    private String name;
    public String getName() {
        if(this.name == null) {
            // check tags for a name
            this.name = this.findValueInTags("name");
            if(this.name == null) this.name = "";
        }
        
        return this.name;
    }
    
    public String findValueInTags(String key) {
        String nix = null;
        
        if(this.tags == null || tags.isEmpty()) return nix;
        
        Iterator<TagElement> iterator = this.tags.iterator();
        
        for (TagElement tag : this.tags) {
            if(tag.attributes != null) {
                String elementName = tag.attributes.get(key);
                if(elementName != null) return elementName;
            }
        }
        
        return nix;
    }
    
    public ArrayList<TagElement> getTags() {
      return this.tags;
    }
}
