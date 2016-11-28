package osmupdatewizard;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

/**
 *
 * @author thsc
 */
public class OSMElement {

  protected final HashMap<String, String> attributes;
  private HashSet<TagElement> tags = null;

  private long id = 0;

  OSMElement(HashMap<String, String> attributes) {
    this(attributes, null);
  }

  OSMElement(HashMap<String, String> attributes, HashSet<TagElement> tags) {
    this(null, attributes, tags);
  }
  
    OSMElement(String serializedAttributes, String serializedTags) {
        this.attributes = this.deserializeAttributes(serializedAttributes);
        this.tags = this.deserializeTags(serializedTags);
    }

  OSMElement(ElementStorage storage, HashMap<String, String> attributes, HashSet<TagElement> tags) {
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
  }

  void print() {
    attributes.entrySet().stream().forEach((entry) -> {
      System.out.print("k|v: " + entry.getKey() + "|" + entry.getValue() + "\n");
    });
  }

  public long getID() {
    return this.id;
  }

  public HashSet<TagElement> getTags() {
    return this.tags;
  }
  
    String serializeAttributes(HashMap<String, String> attributes) {
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

    private HashMap<String, String> deserializeAttributes(String serializedAttributes) {
        HashMap<String, String> a = new HashMap<>();
        
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

    String getSerializeAttributes() {
        return this.serializeAttributes(this.attributes);
    }
    

    private HashSet<TagElement> deserializeTags(String serializedTags) {
        HashSet<TagElement> t = new HashSet<>();
        
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
  
    String getSerializedTags() {
        if(this.tags == null || this.tags.isEmpty()) {
            return this.getStringWithLength(null);
        }

        Iterator<TagElement> tagIter = this.tags.iterator();
        StringBuilder sTag = new StringBuilder();

        while(tagIter.hasNext()) {
            TagElement tag = tagIter.next();

            String sAttributes = this.serializeAttributes(tag.attributes);

            sTag.append(this.getStringWithLength(sAttributes));
        }

        return sTag.toString();
    }
  
    private String getStringWithLength(String s) {
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
    
    private String unwrapStringWithLength(String s) {
        return this.unwrapStringWithLength(s, 0);
    }
    
    private int calculateOffsetFromUnwrappedString(String unwrappedString) {
        return unwrappedString.length() + MAX_DECIMAL_PLACES;
    }
    
    private static final int MAX_DECIMAL_PLACES = 3;
    private static final int HIGHEST_NUMBER_PLUS_ONE = (int) Math.pow(10, MAX_DECIMAL_PLACES);
    
    private String unwrapStringWithLength(String s, int offset) {
        if(s == null || s.length() - offset < MAX_DECIMAL_PLACES) return null;
        
        String lString = s.substring(offset, offset + MAX_DECIMAL_PLACES);
        
        try {
            int length = Integer.parseInt(lString);
            
            String result = s.substring(offset, offset + length);
            
            return result;
        }
        catch(RuntimeException e) {
            return null;
        }
    }
}
