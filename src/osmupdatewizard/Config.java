package osmupdatewizard;

import java.util.HashMap;
import java.util.Map;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author Sven Petsche
 */
public class Config extends DefaultHandler {

  private final static Config instance = new Config();
  private final Map<String, String> conf = new HashMap<>();

  private String key = null;
  private String value = null;

  private Config() {
  }

  public static Config getInstance() {
    return instance;
  }

  @Override
  public String toString() {
    return this.conf.toString();
  }

  public String getValue(String key) {
    if (this.conf.containsKey(key)) {
      return this.conf.get(key);
    } else {
      return null;
    }
  }

  public String setEntry(String key, String value) {
    return this.conf.putIfAbsent(key, value);
  }

  @Override
  public void startElement(
          String uri,
          String localName,
          String qName,
          Attributes attributes) throws SAXException {
    switch (qName) {
      case "config":
        break;
      case "entry":
        this.key = attributes.getValue("k");
        this.value = attributes.getValue("v");
        break;
      default:
        System.out.println("not catched qName: " + qName);
    }
  }

  @Override
  public void endElement(
          String uri,
          String localName,
          String qName) throws SAXException {
    if (qName.equals("entry")) {
      this.conf.put(this.key, this.value);
    }
  }

}
