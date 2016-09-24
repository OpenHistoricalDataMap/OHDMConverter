package osmupdatewizard;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author Sven Petsche
 */
public class Classification extends DefaultHandler {

  private final static Classification instance = new Classification();
  private Map<String, Integer> list = new HashMap<>();

  private Classification() {
  }

  public static Classification getInstance() {
    return instance;
  }

  public void put(String myClass, String subclass, Integer classcode) {
    list.put(myClass.toLowerCase() + "|" + subclass.toLowerCase(), classcode);
  }

  public Integer getClasscode(String myClass, String subclass) {
    return list.get(myClass.toLowerCase() + "|" + subclass.toLowerCase());
  }

  // ToDo: need to be implemented
  @Override
  public void startElement(
          String uri,
          String localName,
          String qName,
          Attributes attributes) throws SAXException {

  }

  // ToDo: need to be implemented
  @Override
  public void endElement(
          String uri,
          String localName,
          String qName) throws SAXException {

  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Classification:");
    for (Entry entry : list.entrySet()) {
      sb.append("\nclasscode: ").append(entry.getValue())
              .append("; class|subclass: ").append(entry.getKey());
    }
    return sb.toString();
  }
}
