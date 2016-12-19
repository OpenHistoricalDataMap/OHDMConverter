package osm2inter;

/**
 * Stores a tag-value and its key-value-id in the tag table.
 *
 * @author Sven Petsche
 */
public class WhitelistTarget {

  private Integer id;
  private final String value;

  public WhitelistTarget(String value) {
    this.value = value;
    this.id = null;
  }

  public String getValue() {
    return this.value;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public Integer getId() {
    return this.id;
  }
}
