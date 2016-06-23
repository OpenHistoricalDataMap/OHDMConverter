package osmupdatewizard;

import java.util.HashMap;
import java.util.HashSet;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author thsc
 */
public class OSMImporter extends DefaultHandler {

  private int nodeC = 0;
  private int wayC = 0;
  private int relationC = 0;
  private int tagC = 0;
  private int ndC = 0;
  private int memberC = 0;
  private int otherC = 0;

  private HashSet<NDElement> nds = null;
  private HashSet<TagElement> tags = null;
  private HashSet<MemberElement> members = null;

  private HashMap<String, String> attributes = null;
  private int nA = 0;
  private int wA = 0;
  private int rA = 0;

  private final Whitelist whitelist;

  private enum Status {

    OUTSIDE, NODE, WAY, RELATION
  };
  Status status;

  ImportCommandBuilder builder;

  public OSMImporter() throws Exception {
    this.whitelist = Whitelist.getInstance();
    this.builder = SQLImportCommandBuilder.getInstance();
  }

  private HashMap<String, String> parseTagAttributes(Attributes attributes) {
    HashMap<String, String> a = new HashMap<>();

    int length = attributes.getLength();

    // there are alternating key values pairs
    int i = 0;
    while (i < length) {
      String key = attributes.getValue(i++);
      String value = attributes.getValue(i++);

      a.put(key, value);
    }

    return a;
  }

  @Override
  public void startDocument() {
    status = Status.OUTSIDE;
//        System.out.println("Start Document");
  }

  @Override
  public void endDocument() {
    System.out.println("end Document");

    System.out.println("nodes: " + nodeC);
    System.out.println("ways: " + wayC);
    System.out.println("relations: " + relationC);
    System.out.println("tags: " + tagC);
    System.out.println("members: " + memberC);
    System.out.println("others: " + otherC);

    System.out.println("nA|wA|rA: " + nA + "|" + wA + "|" + rA);
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
        nodeC++;
        this.status = Status.NODE;
        this.attributes = this.parseAttributes(attributes);
      }
      break; // single original node

      case "way": {
        if (status != Status.OUTSIDE) {
          System.err.println("way found but not outside");
        }
        wayC++;
        this.status = Status.WAY;
        this.attributes = this.parseAttributes(attributes);
      }
      break; // original way

      case "relation": {
        if (status != Status.OUTSIDE) {
          System.err.println("relation found but not outside");
        }
        relationC++;
        this.status = Status.RELATION;
        this.attributes = this.parseAttributes(attributes);
      }
      break; // original relation

      case "tag": {
        tagC++;
        this.addTag(this.parseTagAttributes(attributes));
      }
      break; // inside node, way or relation

      case "nd": {
        ndC++;
        this.addND(this.parseAttributes(attributes));
      }
      break; // inside a way or relation

      case "member": {
        memberC++;
        this.addMember(this.parseAttributes(attributes));
      }
      break; // inside a relation

      default:
        otherC++;
        System.out.print(qName + ", ");
    }
  }

  private void addTag(HashMap<String, String> a) {
    TagElement newTag = new TagElement(a);
    if (this.tags == null) {
      this.tags = new HashSet<>();
    }
    this.tags.add(newTag);
  }

  private void addND(HashMap<String, String> a) {
    NDElement node = new NDElement(a);
    if (this.nds == null) {
      this.nds = new HashSet<>();
    }
    this.nds.add(node);
  }

  private void addMember(HashMap<String, String> a) {
    MemberElement member = new MemberElement(a);
    if (this.members == null) {
      this.members = new HashSet<>();
    }
    this.members.add(member);
  }

  @Override
  public void endElement(String uri,
          String localName,
          String qName) {
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
      ;
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
        System.out.print("don't import: " + qName + ", ");
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
