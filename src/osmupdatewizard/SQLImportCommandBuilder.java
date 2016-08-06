package osmupdatewizard;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author thsc, Sven Petsche
 */
class SQLImportCommandBuilder implements ImportCommandBuilder, ElementStorage {

  private static final String TAGTABLE = "Tags";
  private static final String NODETABLE = "Nodes";
  private static final String NODEWOTAGTABLE = "NodesWOTag";
  private static final String WAYTABLE = "Ways";
  private static final String RELATIONTMPTABLE = "RelationsTmp";
  private static final String MAX_ID_SIZE = "10485760";

  private boolean n = true;
  private boolean w = true;
  private boolean r = true;

  private long nodesNew = 0;
  private long nodesChanged = 0;
  private long nodesExisting = 0;

  private int rCount = 10;
  private int wCount = 10;
  private String user;
  private String pwd;
  private String serverName;
  private String portNumber;
  private Connection connection;

  private MyLogger logger;

  private static SQLImportCommandBuilder instance = null;

  public static SQLImportCommandBuilder getInstance() {
    if (instance == null) {
      instance = new SQLImportCommandBuilder();
    }
    return instance;
  }

  private SQLImportCommandBuilder() {
    this.logger = MyLogger.getInstance();
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

  private List<String> createTablesList() {
    List<String> l = new ArrayList<>();
    l.add(TAGTABLE);
    l.add(NODETABLE);
    l.add(WAYTABLE);
    return l;
  }

  private void dropTables() throws SQLException {
    if (Config.getInstance().getValue("db_dropTables").equalsIgnoreCase("yes")) {
      StringBuilder sqlDel = new StringBuilder("DROP TABLE ");
      PreparedStatement delStmt = null;
      List<String> tables = this.createTablesList();
      tables.stream().forEach((table) -> {
        sqlDel.append(table).append(", ");
      });
      sqlDel.delete(sqlDel.lastIndexOf(","), sqlDel.length()).replace(sqlDel.length(), sqlDel.length(), ";");
      try {
        delStmt = connection.prepareStatement(sqlDel.toString());
        delStmt.execute();
        logger.print(4, "tables dropped");
      } catch (SQLException e) {
        logger.print(4, "failed to drop table: " + e.getLocalizedMessage());
      } finally {
        if (delStmt != null) {
          delStmt.close();
        }
      }
    }
  }

  private void setupTable(String table, String sqlCreate) throws SQLException {
    try {
      this.checkIfTableExists(table);
      logger.print(4, table + " already exists");
    } catch (SQLException e) {
      logger.print(4, table + " does not exist - creating...");
      PreparedStatement stmt = null;
      StringBuilder sql = new StringBuilder("CREATE TABLE ");
      sql.append(table).append(sqlCreate);
      try {
        stmt = connection.prepareStatement(sql.toString());
        stmt.execute();
      } catch (SQLException ex) {
        logger.print(1, ex.getLocalizedMessage(), true);
      } finally {
        if (stmt != null) {
          stmt.close();
        }
      }
    }
  }

  private void setupKB() {
    logger.print(4, "--- setting up tables ---", true);
    try {
      this.dropTables();
      /**
       * ************ Knowledge base tables ****************************
       */
      StringBuilder sqlTag = new StringBuilder();
      sqlTag.append(" (id SERIAL PRIMARY KEY, ")
              .append("key character varying(255), ")
              .append("value character varying(255));");
      this.setupTable(TAGTABLE, sqlTag.toString());
      this.importWhitelist();
      StringBuilder sqlNode = new StringBuilder();
      sqlNode.append(" (osm_id bigint PRIMARY KEY, ")
              .append("long character varying(").append(MAX_ID_SIZE).append("), ")
              .append("lat character varying(").append(MAX_ID_SIZE).append("), ")
              .append("tag bigint REFERENCES ").append(TAGTABLE).append(" (id), ")
              .append("ohdm_id bigint, ")
              .append("ohdm_object bigint, ")
              .append("valid boolean);");
      this.setupTable(NODETABLE, sqlNode.toString());
      StringBuilder sqlWay = new StringBuilder();
      sqlWay.append(" (osm_id bigint PRIMARY KEY, ")
              .append("tag bigint REFERENCES ").append(TAGTABLE).append(" (id), ")
              .append("ohdm_id bigint, ")
              .append("ohdm_object bigint, ")
              .append("valid boolean);");
      this.setupTable(WAYTABLE, sqlWay.toString());
    } catch (SQLException e) {
      System.err.println("error while setting up tables: " + e.getLocalizedMessage());
    }
    logger.print(4, "--- finished setting up tables ---", true);
  }

  private void importWhitelist() {
    // import whitelist to auto create ids
    Statement stmtImport = null;
    try {
      stmtImport = connection.createStatement();
      String sql = Whitelist.getInstance().getSQLImport(SQLImportCommandBuilder.TAGTABLE);
      stmtImport.execute(sql);
      logger.print(4, "Whitelist imported", true);
    } catch (SQLException e) {
      logger.print(4, "whitelist import failed: " + e.getLocalizedMessage(), true);
    } finally {
      if (stmtImport != null) {
        try {
          stmtImport.close();
        } catch (SQLException e) {
        }
      }
    }
    // select tag table to get ids
    Map<String, Integer> tagtable = new HashMap<>();
    Statement stmtSelect = null;
    try {
      stmtSelect = connection.createStatement();
      try (ResultSet rs = stmtSelect.executeQuery("SELECT * FROM " + SQLImportCommandBuilder.TAGTABLE)) {
        while (rs.next()) {
          tagtable.put(rs.getString("key") + "|" + rs.getString("value"), rs.getInt("id"));
        }
      }
    } catch (SQLException e) {
      logger.print(4, "select statement failed: " + e.getLocalizedMessage(), true);
    } finally {
      if (stmtSelect != null) {
        try {
          stmtSelect.close();
        } catch (SQLException e) {
        }
      }
    }
    // push ids into whitelistobject
    Whitelist.getInstance().feedWithId(tagtable);
  }

  private void checkIfTableExists(String table) throws SQLException {
    StringBuilder sql = new StringBuilder("SELECT '");
    sql.append(table).append("'::regclass;");
    PreparedStatement stmt = connection.prepareStatement(sql.toString());
    stmt.execute();
  }

  @Deprecated
  private void checkIfTableExists(Statement stmt, String table) throws SQLException {
    stmt.execute("SELECT '" + table + "'::regclass;");
  }

  @Deprecated
  private void resetSequence(Statement stmt, String sequence) throws SQLException {
    try {
      stmt.execute("drop sequence " + sequence + ";");
    } catch (SQLException ee) { /* ignore */ }
    stmt.execute("create sequence " + sequence + ";");
  }

  private HashMap<String, NodeElement> nodes = new HashMap<>();

  private void saveNodeElements() {
    logger.print(5, "save nodes in db and clear hashmap", true);
    String sqlWO = "INSERT INTO " + SQLImportCommandBuilder.NODEWOTAGTABLE + " (osm_id, long, lat) VALUES";
    String sql = "INSERT INTO " + SQLImportCommandBuilder.NODETABLE + " (osm_id, long, lat, tag) VALUES";
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
      logger.print(3, e.getLocalizedMessage(), true);
    }
    nodes.clear();
  }

  private void saveNodeElement(NodeElement node) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    if (nodesNew % 100000 == 0) {
      logger.print(1, "step", true);
    }
    if (node.getTagId() == null) {
      sb.append(NODETABLE).append(" (osm_id, long, lat) VALUES (?, ?, ?);");
    } else {
      sb.append(NODETABLE).append(" (osm_id, long, lat, tag) VALUES (?, ?, ?, ?);");
    }
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(sb.toString());
      stmt.setLong(1, node.getID());
      stmt.setString(2, node.getLongitude());
      stmt.setString(3, node.getLatitude());
      if (node.getTagId() != null) {
        stmt.setInt(4, node.getTagId());
      }
      stmt.execute();
    } catch (SQLException e) {
      logger.print(1, e.getLocalizedMessage(), true);
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
      }
    }
  }

  private void deleteNodeById(long osm_id) {
    StringBuilder sb = new StringBuilder("DELETE FROM ");
    sb.append(NODETABLE).append(" WHERE osm_id = ? ;");
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(sb.toString());
      stmt.setLong(1, osm_id);
      stmt.execute();
    } catch (SQLException e) {
      logger.print(1, e.getLocalizedMessage(), true);
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
      }
    }
  }

  @Override
  public void addNode(HashMap<String, String> attributes, HashSet<TagElement> tags) {
    NodeElement newNode = new NodeElement(this, attributes, tags);
    NodeElement dbNode = selectNodeById(newNode.getID());
    switch (matchNodes(newNode, dbNode)) {
      case 0:
        this.nodesNew++;
        this.saveNodeElement(newNode);
        break;
      case 1:
        this.nodesChanged++;
        this.deleteNodeById(newNode.getID());
        this.saveNodeElement(newNode);
        break;
      case 2:
        this.nodesExisting++;
        // do nothing, because node is ok
        break;
      default:
        break;
    }
    /*nodes.put(newNode.getID(), newNode);
     if (nodes.size() > this.tmpStorageSize) {
     this.saveNodeElements();
     }*/
  }

  private NodeElement selectNodeById(long osm_id) {
    StringBuilder sb = new StringBuilder("SELECT * FROM ");
    sb.append(NODETABLE).append(" WHERE osm_id = ?;");
    PreparedStatement stmt = null;
    HashMap<String, String> attributes = null;
    try {
      stmt = connection.prepareStatement(sb.toString());
      stmt.setLong(1, osm_id);
      ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        attributes = new HashMap<>();
        attributes.put("id", String.valueOf(rs.getInt("osm_id")));
        attributes.put("lat", rs.getString("lat"));
        attributes.put("lon", rs.getString("long"));
      }
    } catch (SQLException ex) {
      logger.print(1, ex.getLocalizedMessage(), true);
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
      }
    }
    if (attributes == null) {
      return null;
    } else {
      return new NodeElement(attributes, null);
    }
  }

  /**
   * checks latitude and longitude of node
   *
   * @param newNode
   * @param dbNode
   * @return 0: node does not exist, 1: node has changed, 2: node exists
   */
  private Integer matchNodes(NodeElement newNode, NodeElement dbNode) {
    Integer state = 1;
    if (dbNode == null) {
      return 0;
    } else {
      if (newNode.getLatitude().equals(dbNode.getLatitude())
              && newNode.getLongitude().equals(dbNode.getLongitude())) {
        state = 2;
      }
    }
    return state;
  }

  private final HashMap<Integer, WayElement> ways = new HashMap<>();

  private void saveWayElements() {
    logger.print(5, "save ways in db and clear hashmap", true);
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(WAYTABLE).append("(osm_id, tag) VALUES");
    ways.entrySet().stream().filter((entry) -> (entry.getValue().getTags() != null)).forEach((entry) -> {
      sb.append(" (").append(entry.getKey()).append(", ").append(entry.getValue().getTagId()).append("),");
    });
    try (PreparedStatement stmt = connection.prepareStatement(sb.deleteCharAt(sb.length() - 1).append(";").toString())) {
      stmt.execute();
    } catch (SQLException ex) {
      logger.print(1, ex.getLocalizedMessage(), true);
    }
    ways.clear();
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
    if (nds == null || nds.isEmpty()) {
      return; // a way without nodes makes no sense.
    }
    WayElement newWay = new WayElement(this, attributes, nds, tags);
    //ways.put(newWay.getID(), newWay);
    /*if (ways.size() > this.tmpStorageSize) {
     this.saveWayElements();
     }*/
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
    if (members == null || members.isEmpty() || tags == null) {
      return; // empty relations makes no sense;
    }

    RelationElement relationElement = new RelationElement(attributes, members, tags);

    // get ways and points
    // produce geometry
    // save in OHDM
    // debugging / testing
    if (this.rCount-- > 0) {
      System.out.println("Relation");
      this.printAttributes(attributes);
      this.printTags(tags);
      this.printMembers(members);
      r = false;
      System.out.println("===========================");
    }
  }

  @Override
  public void printStatus() {
    logger.print(0, "\nNodes");
    logger.print(0, "new\t  changed\t  existing");
    logger.print(0, this.nodesNew + "\t| " + this.nodesChanged + "\t| " + this.nodesExisting);
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
