package osmupdatewizard;

import java.util.HashMap;
import java.util.Map;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Singleton patter
 *
 * @author Sven Petsche
 */
public class Whitelist extends DefaultHandler {

  private final static Whitelist instance = new Whitelist();
  private final Map<String, Map<String, String>> list = new HashMap<>();

  private String key = null;
  private String target = null;
  private String value = null;

  private Whitelist() {
  }

  public static Whitelist getInstance() {
    return instance;
  }

  /**
   * Returns a dump of the whitelist entries.
   *
   * @return String which contains the dump
   */
  @Override
  public String toString() {
    return list.toString();
  }

  /**
   * Searches for the key value pair and returns the reducetarget if the pair
   * was found, otherwise null.
   *
   * @param key
   * @param value
   * @return the target
   */
  public String reduce(String key, String value) {
    if (this.list.containsKey(key)) {
      if (this.list.get(key).containsKey(value)) {
        return this.list.get(key).get(value);
      }
    }
    return null;
  }

  /**
   * Saves the current path into private variables.
   *
   * @param uri
   * @param localName
   * @param qName
   * @param attributes
   * @throws SAXException
   */
  @Override
  public void startElement(
          String uri,
          String localName,
          String qName,
          Attributes attributes) throws SAXException {
    switch (qName) {
      case "whitelist":
        break;
      case "key":
        this.key = attributes.getValue("k");
        break;
      case "target":
        this.target = attributes.getValue("t");
        break;
      case "value":
        this.value = attributes.getValue("v");
        break;
      default:
        System.out.println("not catched qName: " + qName);
    }
  }

  /**
   * Saves the target entry into the whitelist.
   *
   * @param uri
   * @param localName
   * @param qName
   * @throws SAXException
   */
  @Override
  public void endElement(
          String uri,
          String localName,
          String qName) throws SAXException {
    if (qName.equals("value")) {
      if (!this.list.containsKey(this.key)) {
        this.list.put(this.key, new HashMap<>());
      }
      this.list.get(this.key).put(this.value, this.target);
    }
  }
}
