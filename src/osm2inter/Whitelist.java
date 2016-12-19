package osm2inter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
  private final Map<String, Map<String, WhitelistTarget>> list = new HashMap<>();

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
        return this.list.get(key).get(value).getValue();
      }
    }
    return null;
  }

  /**
   * Creates the sql statement for the Tagtable import.
   *
   * @param tablename the tablename of the tagtable
   * @return
   */
  public String getSQLImport(String tablename) {
    StringBuilder sql = new StringBuilder("INSERT INTO " + tablename + " (key, value) VALUES");
    for (Map.Entry<String, Map<String, WhitelistTarget>> keyEntry : this.list.entrySet()) {
      for (Map.Entry<String, WhitelistTarget> valueEntry : keyEntry.getValue().entrySet()) {
        List<String> l = new ArrayList<>();
        l.add(keyEntry.getKey());
        l.add(valueEntry.getKey());
        sql.append(buildSQLValues(l));
      }
    }
    sql.deleteCharAt(sql.length() - 1);
    return sql.replace(sql.length(), sql.length(), ";").toString();
  }

  private String buildSQLValues(List<String> l) {
    StringBuilder sb = new StringBuilder(" ('");
    l.stream().forEach((s) -> {
      sb.append(s).append("', '");
    });
    return sb.delete(sb.length() - 3, sb.length()).replace(sb.length(), sb.length(), "),").toString();
  }

  /**
   * Adds IDs to the datastructure.
   *
   * @param ids the maps key is the |-seperated key|value pair
   */
  public void feedWithId(Map<String, Integer> ids) {
    ids.entrySet().stream().forEach((entry) -> {
      String[] parts = entry.getKey().split("\\|");
      this.list.get(parts[0]).get(parts[1]).setId(entry.getValue());
    });
  }

  /**
   * Searches for the targets-tag-ID
   *
   * @param key
   * @param value
   * @return null or the targets tag id
   */
  public Integer getId(String key, String value) {
    return this.list.get(key).get(value).getId();
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
      this.list.get(this.key).put(this.value, new WhitelistTarget(this.target));
    }
  }

}
