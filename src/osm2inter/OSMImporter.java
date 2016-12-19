package osm2inter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;
import util.Parameter;

/**
 * @author thsc
 */
public class OSMImporter extends DefaultHandler {

  private ArrayList<NodeElement> nds = null;
  private ArrayList<TagElement> tags = null;
  private ArrayList<MemberElement> members = null;

  private HashMap<String, String> attributes = null;
  private int nA = 0;
  private int wA = 0;
  private int rA = 0;

  private enum Status {

    OUTSIDE, NODE, WAY, RELATION
  };
  Status status;

  ImportCommandBuilder builder;

  public OSMImporter(Parameter parameter) throws Exception {
    this.builder = SQLImportCommandBuilder.getInstance(parameter);
  }

  private HashMap<String, String> parseTagAttributes(Attributes attributes) {
    HashMap<String, String> a = new HashMap<>();
    // there are alternating key values pairs
    int i = 0;
    while (i < attributes.getLength()) {
      a.put(attributes.getValue(i++), attributes.getValue(i++));
    }
    return a;
  }

  @Override
  public void startDocument() {
    status = Status.OUTSIDE;
  }

  @Override
  public void endDocument() {
      try {
          builder.flush();
      } catch (SQLException ex) {
          System.err.printf(ex.getMessage());
      }
    builder.printStatus();
  }

  @Override
  public void startElement(String uri,
          String localName,
          String qName,
          Attributes attributes) {
    switch (qName) {
      case "node": {
        if (status != Status.OUTSIDE) {
          System.err.println("node found but not outside");
        }
        this.status = Status.NODE;
        this.attributes = this.parseAttributes(attributes);
      }
      break; // single original node
      case "way": {
        if (status != Status.OUTSIDE) {
          System.err.println("way found but not outside");
        }
        this.status = Status.WAY;
        this.attributes = this.parseAttributes(attributes);
      }
      break; // original way
      case "relation": {
        if (status != Status.OUTSIDE) {
          System.err.println("relation found but not outside");
        }
        this.status = Status.RELATION;
        this.attributes = this.parseAttributes(attributes);
      }
      break; // original relation
      case "tag": {
        this.addTag(this.parseTagAttributes(attributes));
      }
      break; // inside node, way or relation
      case "nd": {
        this.addND(this.parseAttributes(attributes));
      }
      break; // inside a way or relation
      case "member": {
        this.addMember(this.parseAttributes(attributes));
      }
      break; // inside a relation
      default:
      //System.out.print(qName + ", ");
    }
  }

  private void addTag(HashMap<String, String> a) {
    TagElement newTag = new TagElement(a);
    if (this.tags == null) {
      this.tags = new ArrayList<>();
    }
    this.tags.add(newTag);
  }

  private void addND(HashMap<String, String> a) {
    NodeElement node = new NodeElement(a, null);
    if (this.nds == null) {
      this.nds = new ArrayList<>();
    }
    this.nds.add(node);
  }

  private void addMember(HashMap<String, String> a) {
    MemberElement member = new MemberElement(a);
    if (this.members == null) {
      this.members = new ArrayList<>();
    }
    this.members.add(member);
  }

  @Override
  public void endElement(String uri,
          String localName,
          String qName) {
      
      try {
        switch (qName) {
          case "node": {
            // node finished - save
            if (!this.attributes.isEmpty()) {
              this.nA++;
            }
            this.builder.addNode(this.attributes, this.tags);
            this.status = Status.OUTSIDE;
            // cleanup
            this.tags = null;
          }
          break; // single original node
          case "way": {
            if (!this.attributes.isEmpty()) {
              this.wA++;
            }
            this.builder.addWay(this.attributes, this.nds, this.tags);
            this.status = Status.OUTSIDE;
            // cleanup
            this.tags = null;
            this.nds = null;
          }
          break; // original way
          case "relation": {
            if (!this.attributes.isEmpty()) {
              this.rA++;
            }
            this.builder.addRelation(this.attributes, this.members, this.tags);
            this.status = Status.OUTSIDE;
            // cleanup
            this.tags = null;
            this.nds = null;
            this.members = null;
          }
          break; // original relation
          case "tag":
            break; // inside node, way or relation
          case "nd":
            break; // inside a way or relation
          case "member":
            break; // inside a relation
          default:
          //System.out.print("don't import: " + qName + ", ");
        }
      }
    catch(SQLException e) {
        System.err.println(e.getMessage());
    }
  }

  private HashMap<String, String> parseAttributes(Attributes attributes) {
    HashMap a = new HashMap<>();

    int number = attributes.getLength();
    for (int i = 0; i < number; i++) {
      String key = attributes.getQName(i);
      String value = attributes.getValue(i);
      a.put(key, value);
    }
    return a;
  }
}
