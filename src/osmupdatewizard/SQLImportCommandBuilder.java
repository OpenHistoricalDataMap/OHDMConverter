package osmupdatewizard;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author thsc
 */
class SQLImportCommandBuilder implements ImportCommandBuilder, ElementStorage {

  private static final String TAGTABLE = "Tags";
  private static final String NODETMPTABLE = "NodesTmp";
  private static final String NODEWOTAGTABLE = "NodesWOTag";
  private static final String WAYTMPTABLE = "WaysTmp";
  private static final String RELATIONTMPTABLE = "RelationsTmp";
  private static final String MAX_ID_SIZE = "10485760";

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

  private Logger logger;

  private static SQLImportCommandBuilder instance = null;

  public static SQLImportCommandBuilder getInstance() {
    if (instance == null) {
      instance = new SQLImportCommandBuilder();
    }
    return instance;
  }

  private SQLImportCommandBuilder() {
    this.logger = Logger.getInstance();
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
      logger.print(0, "cannot connect to database: " + e.getLocalizedMessage());
    }

    if (this.connection == null) {
      System.err.println("cannot connect to database: reason unknown");
    }

    this.setupKB();
  }

  private void setupKB() {
    logger.print(4, "--- setting up tables ---", true);
    Statement stmt = null;
    try {
      stmt = connection.createStatement();
      if (Config.getInstance().getValue("db_dropTables").equalsIgnoreCase("yes")) {
        try {
          stmt.execute("DROP TABLE " + SQLImportCommandBuilder.TAGTABLE + ", " + SQLImportCommandBuilder.NODEWOTAGTABLE + ", " + SQLImportCommandBuilder.NODETMPTABLE);
          logger.print(4, "tables dropped");
        } catch (SQLException e) {
          logger.print(4, "failed to drop tables: " + e.getLocalizedMessage());
        }
      }
      /**
       * ************ Knowledge base table ****************************
       */
      try {
        this.checkIfTableExists(stmt, SQLImportCommandBuilder.TAGTABLE);
        logger.print(4, SQLImportCommandBuilder.TAGTABLE + " already exists");
      } catch (SQLException e) {
        logger.print(4, SQLImportCommandBuilder.TAGTABLE + " does not exist - creating...");
        this.resetSequence(stmt, "tagid");
        stmt.execute("CREATE TABLE " + SQLImportCommandBuilder.TAGTABLE + " (id integer PRIMARY KEY default nextval('tagid'), key character varying(255), value character varying(255));");
      }
      this.importWhitelist();
      try {
        this.checkIfTableExists(stmt, SQLImportCommandBuilder.NODEWOTAGTABLE);
        logger.print(4, SQLImportCommandBuilder.NODEWOTAGTABLE + " already exists");
      } catch (SQLException e) {
        logger.print(4, SQLImportCommandBuilder.NODEWOTAGTABLE + " does not exist - creating...");
        stmt.execute("CREATE TABLE " + SQLImportCommandBuilder.NODEWOTAGTABLE + " (id integer PRIMARY KEY, long character varying(" + SQLImportCommandBuilder.MAX_ID_SIZE + "), lat character varying(" + SQLImportCommandBuilder.MAX_ID_SIZE + "));");
      }
      try {
        this.checkIfTableExists(stmt, SQLImportCommandBuilder.NODETMPTABLE);
        logger.print(4, SQLImportCommandBuilder.NODETMPTABLE + " already exists");
      } catch (SQLException e) {
        logger.print(4, SQLImportCommandBuilder.NODETMPTABLE + " does not exist - creating...");
        stmt.execute("CREATE TABLE " + SQLImportCommandBuilder.NODETMPTABLE + " (id integer PRIMARY KEY, long character varying(" + SQLImportCommandBuilder.MAX_ID_SIZE + "), lat character varying(" + SQLImportCommandBuilder.MAX_ID_SIZE + "), tag integer REFERENCES " + SQLImportCommandBuilder.TAGTABLE + " (id));");
      }
    } catch (SQLException e) {
      System.err.println("error while setting up tables: " + e.getLocalizedMessage());
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
    }
    logger.print(4, "--- finished setting up tables ---", true);
  }

  private void importWhitelist() {
    // import whitelist to auto create ids
    try {
      Statement stmt = connection.createStatement();
      stmt.execute(Whitelist.getInstance().getSQLImport(SQLImportCommandBuilder.TAGTABLE));
      logger.print(4, "Whitelist imported", true);
    } catch (SQLException e) {
      logger.print(4, "whitelist import failed: " + e.getLocalizedMessage(), true);
    }
    // select tag table to get ids
    Map<String, Integer> tagtable = new HashMap<>();
    try {
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + SQLImportCommandBuilder.TAGTABLE);
      while (rs.next()) {
        tagtable.put(rs.getString("key") + "|" + rs.getString("value"), rs.getInt("id"));
      }
    } catch (SQLException e) {
      logger.print(4, "select statement failed: " + e.getLocalizedMessage(), true);
    }
    // push ids into whitelistobject
    Whitelist.getInstance().feedWithId(tagtable);
  }

  private void checkIfTableExists(Statement stmt, String table) throws SQLException {
    stmt.execute("SELECT '" + table + "'::regclass;");
  }

  private void resetSequence(Statement stmt, String sequence) throws SQLException {
    try {
      stmt.execute("drop sequence " + sequence + ";");
    } catch (SQLException ee) { /* ignore */ }
    stmt.execute("create sequence " + sequence + ";");
  }

  private HashMap<String, NodeElement> nodes = new HashMap<>();

  private void saveNodeElements() {
    String sqlWO = "INSERT INTO " + SQLImportCommandBuilder.NODEWOTAGTABLE + " (id, long, lat) VALUES";
    String sql = "INSERT INTO " + SQLImportCommandBuilder.NODETMPTABLE + " (id, long, lat, tag) VALUES";
    for (Map.Entry<String, NodeElement> entry : nodes.entrySet()) {
      if (entry.getValue().getTags() == null) {
        sqlWO += " (" + entry.getKey() + ", " + entry.getValue().getLatitude() + ", " + entry.getValue().getLongitude() + "),";
      } else {
        sql += " (" + entry.getKey() + ", " + entry.getValue().getLatitude() + ", " + entry.getValue().getLongitude() + ", " + entry.getValue().getTagId() + "),";
      }
    }
    try {
      Statement stmt = connection.createStatement();
      stmt.execute(sqlWO.substring(0, sqlWO.length() - 1) + ";");
      stmt.execute(sql.substring(0, sql.length() - 1) + ";");
    } catch (SQLException e) {
      logger.print(3, "Error: " + e.getLocalizedMessage(), true);
    }
  }

  @Override
  public void addNode(HashMap<String, String> attributes, HashSet<TagElement> tags) {
    NodeElement newNode = new NodeElement(this, attributes, tags);
    String id = newNode.getID();
    nodes.put(id, newNode);

    if (nodes.size() > Integer.parseInt(Config.getInstance().getValue("tmpStorageSize"))) {
      logger.print(5, "save nodes in db and clear hashmap", true);
      this.saveNodeElements();
      nodes.clear();
    }

    // debugging / testing
    /*if (n && tags != null) {
     System.out.println("Tag");
     this.printAttributes(attributes);
     this.printTags(tags);
     n = false;
     System.out.println("===========================");
     }*/
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
  
  ////////////////////////////////////////////////////////////////////
  //                       print for debug                          //
  ////////////////////////////////////////////////////////////////////

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
}
