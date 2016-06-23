package osmupdatewizard;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;

/**
 *
 * @author thsc
 */
class SQLImportCommandBuilder implements ImportCommandBuilder, ElementStorage {

  private static String NODETABLE = "ImportNodes";
  private static String MAX_ID_SIZE = "10485760";

  private boolean n = true;
  private boolean w = true;
  private boolean r = true;

  private int rCount = 10;
  private int wCount = 10;
  private String user;
  private String pwd;
  private String serverName;
  private String portNumber;
  private Connection connection;
  
  private static SQLImportCommandBuilder instance = null;

  public static SQLImportCommandBuilder getInstance(){
    if (instance == null) {
      instance = new SQLImportCommandBuilder();
    }
    return instance;
  }
  
  private SQLImportCommandBuilder() {
    try {
      this.user = Config.getInstance().getValue("db_user");
      this.pwd = Config.getInstance().getValue("db_password");
      this.serverName = Config.getInstance().getValue("db_serverName");
      this.portNumber = Config.getInstance().getValue("db_portNumber");
      Properties connProps = new Properties();
      connProps.put("user", this.user);
      connProps.put("password", this.pwd);
      this.connection = DriverManager.getConnection(
              "jdbc:postgresql://" + this.serverName
              + ":" + this.portNumber + "/", connProps);
    } catch (SQLException e) {
      System.err.println("cannot connect to database: " + e.getLocalizedMessage());
    }

    if (this.connection == null) {
      System.err.println("cannot connect to database: reason unknown");
    }

    this.setupKB();
  }

  private void setupKB() {
    Statement statement = null;
    try {
      statement = connection.createStatement();

      /**
       * ************ Knowledge base table ****************************
       */
      try {
        statement.execute("SELECT * from " + SQLImportCommandBuilder.NODETABLE);
        System.out.println(SQLImportCommandBuilder.NODETABLE + " already exists");
      } catch (SQLException e) {
        // does not exist: create
        System.out.println(SQLImportCommandBuilder.NODETABLE + " does not exists - create");
        try {
          statement.execute("drop sequence nodeid;");
        } catch (SQLException ee) { /* ignore */ }
        statement.execute("create sequence nodeid;");
        statement.execute("CREATE TABLE " + SQLImportCommandBuilder.NODETABLE
                + " (id integer PRIMARY KEY default nextval('nodeid'), "
                + "long character varying(" + SQLImportCommandBuilder.MAX_ID_SIZE + "), "
                + "lat character varying(" + SQLImportCommandBuilder.MAX_ID_SIZE + "), "
                + "tags character varying(255),"
                + "ohdm_geom_ID integer, "
                + "ohdm_go_ID integer, "
                + "st_type smallint,"
                + "valid boolean default false"
                + ");");
      }
    } catch (SQLException e) {
      System.err.println("error while setting up tables: " + e.getLocalizedMessage());
      System.err.println("error while setting up tables: " + e.getLocalizedMessage());
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
    }
  }

  private void printAttributes(HashMap<String, String> a) {
    System.out.println("\nAttributes");
    if (a == null) {
      System.out.println("Empty");
      return;
    }

    Iterator<String> kIter = a.keySet().iterator();
    while (kIter.hasNext()) {
      String k = kIter.next();
      String v = a.get(k);
      System.out.print("k|v: " + k + "|" + v + "\n");
    }
  }

  private void printTags(HashSet<TagElement> tags) {
    System.out.println("\nTags");
    if (tags == null) {
      System.out.println("Empty");
      return;
    }
    Iterator<TagElement> iterator = tags.iterator();
    while (iterator.hasNext()) {
      TagElement tag = iterator.next();
      tag.print();
    }
  }

  private void printNodes(HashSet<NDElement> nds) {
    System.out.println("\nNodes");
    if (nds == null) {
      System.out.println("Empty");
      return;
    }
    Iterator<NDElement> iterator = nds.iterator();
    while (iterator.hasNext()) {
      System.out.print("node:\n");
      NDElement node = iterator.next();
      node.print();
    }
  }

  private void printMembers(HashSet<MemberElement> members) {
    System.out.println("\nMember");
    if (members == null) {
      System.out.println("Empty");
      return;
    }
    Iterator<MemberElement> iterator = members.iterator();
    while (iterator.hasNext()) {
      System.out.print("member:\n");
      MemberElement member = iterator.next();
      member.print();
    }
  }

  private HashMap<String, NodeElement> nodes = new HashMap<>();

  private NodeElement saveNodeElement(ElementStorage storage, HashMap<String, String> attributes, HashSet<TagElement> tags) {
    NodeElement newNode = new NodeElement(storage, attributes, tags);
    if (newNode == null) {
      System.err.println("node null");
      return null;
    }

    String id = newNode.getID();
    nodes.put(id, newNode);
    return newNode;
  }

  @Override
  public void addNode(HashMap<String, String> attributes, HashSet<TagElement> tags) {
    this.saveNodeElement(this, attributes, tags);

    if (tags == null) {
      /* a node without tags has no identity - it's only stored
       * in temporary database
       */
      return; // debugging
    } else {
      // node has tags - has an identity - also save in OHDM as GO
    }

    // debugging / testing
    if (n) {
      System.out.println("Tag");
      this.printAttributes(attributes);
      this.printTags(tags);
      n = false;
      System.out.println("===========================");
    }
  }

  private final HashMap<String, WayElement> ways = new HashMap<>();

  private WayElement saveWayElement(ElementStorage storage, HashMap<String, String> attributes, HashSet<NDElement> nds, HashSet<TagElement> tags) {
    WayElement wayElement = new WayElement(storage, attributes, nds, tags);
    String id = wayElement.getID();
    ways.put(id, wayElement);

    return wayElement;
  }

  /**
   * http://wiki.openstreetmap.org/wiki/Way
   *
   * @param attributes
   * @param nds
   * @param tags
   */
  @Override
  public void addWay(HashMap<String, String> attributes, HashSet<NDElement> nds, HashSet<TagElement> tags) {
    WayElement wayElement = this.saveWayElement(this, attributes, nds, tags);

    if (this.wCount-- > 0) {
      wayElement.print();
      w = false;
    }

    if (nds == null && nds.isEmpty()) {
      return; // a way without nodes makes no sense.
    }

    // get nodes from temp database 
    // produce geometry
    // save in OHDM
    // debugging / testing
    if (nds == null || tags == null) {
      return; // debugging
    }

  }

  /**
   * OSM Relations are defined here: http://wiki.openstreetmap.org/wiki/Relation
   *
   * @param attributes
   * @param members
   * @param tags
   */
  @Override
  public void addRelation(HashMap<String, String> attributes,
          HashSet<MemberElement> members, HashSet<TagElement> tags) {
    if (members == null && members.isEmpty()) {
      return; // empty relations makes no sense;
    }

    RelationElement relationElement = new RelationElement(attributes, members, tags);

    // get ways and points
    // produce geometry
    // save in OHDM
    // debugging / testing
    if (members == null || tags == null) {
      return;
    }
    if (this.rCount-- > 0) {
      System.out.println("Relation");
      this.printAttributes(attributes);
      this.printTags(tags);
      this.printMembers(members);
      r = false;
      System.out.println("===========================");
    }
  }

  ////////////////////////////////////////////////////////////////////
  //                       element storage                          //
  ////////////////////////////////////////////////////////////////////
  @Override
  public NodeElement getNodeByID(String id) {
    return this.nodes.get(id);
  }
}
