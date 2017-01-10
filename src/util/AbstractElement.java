package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 *
 * @author thsc
 */
public class AbstractElement {
    private final HashMap<String, String> attributes;
    
    public HashMap<String, String> getAttributes() {
        return this.attributes;
    }
    /**
     * use that constructor only when scanning osm import file - it removes a number of attributes
     * @param attributes
     * @param tags 
     */
//    public AbstractElement(HashMap<String, String> attributes, ArrayList<TagElement> tags) {
//        this.attributes = attributes;
//        
//        this.tags2attributes(tags);
//    }

    public AbstractElement(String serializedAttrAndTags) {
//        this.attributes = new HashMap<>();
        this.attributes = this.deserializeAttributes(serializedAttrAndTags);
        
//        this.tags2attributes();
    }
    
    /**
     * That constructor should not exist (clean up that mess asap!)
     * ... its ought to be an abstract class.. need access 
     * to seriazlation to parse planet.osm.. TODO
     */
    public AbstractElement() {
        this.attributes = null;
        this.name = null;
    }
    
    /**
     * copy all tas to attributes
     */
//    private void tags2attributes(ArrayList<TagElement> tags) {
//        if(tags == null) return;
//        if(tags.size() > 0) {
//            int i = 42; // debug break
//        }
//        for (TagElement tag : tags) {
//            HashMap<String, String> tagAttributes = tag.getAttributes();
//            if(tagAttributes != null) {
//                for(String key : tagAttributes.keySet()) {
//                    String value = tagAttributes.get(key);
//                    this.attributes.put(key, value);
//                }
//            }
//        }
//    }
  
    private String stripForbiddenCharacters(String s) {
        int i = s.indexOf("'");
        
        while(i != -1) {
            // just throw it away
            StringBuilder sb = new StringBuilder();
            sb.append(s.substring(0, i));
            if(i < s.length()-1) {
                sb.append(s.substring(i+1));
            }
            s = sb.toString();
            i = s.indexOf("'");
        }
        
        return s;
    }
  
    public String serializeAttributes(HashMap<String, String> attributes) {
        if(attributes == null || attributes.isEmpty()) return this.getStringWithLength(null);

        StringBuilder sAttributes = new StringBuilder();

        Iterator<String> keyIter = attributes.keySet().iterator();
        while(keyIter.hasNext()) {
            String key = keyIter.next();
            key = this.stripForbiddenCharacters(key);
            sAttributes.append(this.getStringWithLength(key));

            String value = attributes.get(key);
            value = this.stripForbiddenCharacters(value);
            sAttributes.append(this.getStringWithLength(value));
        }

        return sAttributes.toString();

    }
    
    protected final HashMap<String, String> deserializeAttributes(String serializedAttributes) {
        HashMap<String, String> a = new HashMap<>();
        
        if(serializedAttributes == null) return a;
        
        /*
        here comes a string key|value each entry (key or value) has this structure
        [length]content length is a three digit decimal number (expressed as character)
        OR it is "0000" which indicates empty (null) argument.
        */
        
        int index = 0;
        while(index < serializedAttributes.length()) {
            if(serializedAttributes.substring(index).startsWith("0000")) {
                // empty key.. that makes no sense
                System.err.println("\nAbstractElement.deserializeAttributes: found empty key (makes no sense), stop parsing attributes:" + serializedAttributes);
                return a;
            }
            String key = this.unwrapStringWithLength(serializedAttributes, index);
            index += this.calculateOffsetFromUnwrappedString(key);
            
            if(serializedAttributes.substring(index).startsWith("0000")) {
                index+=4;
                System.out.println("null value for key: " + key);
                a.put(key, null);
            } else {
                String value = this.unwrapStringWithLength(serializedAttributes, index);
                if(value != null) {
                    index += this.calculateOffsetFromUnwrappedString(value);
                }
                a.put(key, value);
            }
        }
        
        return a;
    }

    protected String getSerializeAttributes() {
        return this.serializeAttributes(this.attributes);
    }
    
    public static String[] relevantAttributeKeys = new String[] {"uid", "user"};
    
    private HashMap<String, String> getRelevantAttributes(HashMap<String, String> attributes) {
        HashMap<String, String> relevantAttributes = new HashMap<>();
        
        relevantAttributes = new HashMap<>();
    
        for (String key : AbstractElement.relevantAttributeKeys) {
            String value = attributes.get(key);
            
            if(value != null) {
                relevantAttributes.put(key, value);
            }
        }
        
        return relevantAttributes;
    }
  
    protected String getSerializedTagsAndAttributes() {
        // attributes first - take only relevant attributes
        if(this.attributes != null && !this.attributes.isEmpty()) {
            String sAttributes = this.serializeAttributes(this.attributes);
            if(sAttributes != null && sAttributes.length() > 0) {
                return sAttributes;
            }
        }
        
        return null;
        
        // now tags
//        if(this.tags != null && !this.tags.isEmpty()) {
//            Iterator<TagElement> tagIter = this.tags.iterator();
//
//            while(tagIter.hasNext()) {
//                TagElement tag = tagIter.next();
//
//                String sAttributes = this.serializeAttributes(tag.attributes);
//
//                sTagAttr.append(this.getStringWithLength(sAttributes));
//            }
//        }
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
        
        String lengthString = s.substring(offset, offset + MAX_DECIMAL_PLACES);
        
        try {
            int length = Integer.parseInt(lengthString);
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
            this.name = this.getValue("name");
            if(this.name == null) this.name = "";
        }
        
        return this.name;
    }
    
    public final String getValue(String key) {
        String value = this.attributes.get(key);
        return value;
    }
    
    public String getType() {
        return this.getValue("type");
    }
    
    /**
     * duration / names pairs  of old names as there are found in osm
     * @return can return null if no old name is found.
     */
    public HashMap<String, String> getOldNameStrings() {
        //old_name:[lang]:1906-1933
        
        HashMap<String, String> oldNames = null;
        // iterate attributes
        for(String name : this.attributes.keySet()) {
            if(name.startsWith("old_name")) {
                // extract time from key
                int last = name.lastIndexOf(":");
                if(last == -1) continue;
                String durationString = name.substring(last+1);
                String oldName = this.attributes.get(name);
                
                if(oldName != null && oldName.length() > 0) {
                    if(oldNames == null) {
                        oldNames = new HashMap<>();
                    }
                    oldNames.put(durationString, oldName);
                }
            }
        }
        
        return oldNames;
    }
}
