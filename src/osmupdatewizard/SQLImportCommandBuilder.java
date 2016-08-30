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

  public static final String TAGTABLE = "Tags";
  public static final String NODETABLE = "Nodes";
  public static final String WAYTABLE = "Ways";
  public static final String RELATIONTMPTABLE = "Relations";
  public static final String MAX_ID_SIZE = "10485760";

  private boolean n = true;
  private boolean w = true;
  private boolean r = true;

  private long nodesNew = 0;
  private long nodesChanged = 0;
  private long nodesExisting = 0;

  private long waysNew = 0;
  private long waysChanged = 0;
  private long waysExisting = 0;

  private long relNew = 0;
  private long relChanged = 0;
  private long relExisting = 0;

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
      StringBuilder sqlWay = new StringBuilder();
      sqlWay.append(" (osm_id bigint PRIMARY KEY, ")
              .append("tag bigint REFERENCES ").append(TAGTABLE).append(" (id), ")
              .append("ohdm_id bigint, ")
              .append("ohdm_object bigint, ")
              .append("valid boolean);");
      this.setupTable(WAYTABLE, sqlWay.toString());
      StringBuilder sqlNode = new StringBuilder();
      sqlNode.append(" (osm_id bigint PRIMARY KEY, ")
              .append("long character varying(").append(MAX_ID_SIZE).append("), ")
              .append("lat character varying(").append(MAX_ID_SIZE).append("), ")
              .append("tag bigint REFERENCES ").append(TAGTABLE).append(" (id), ")
              .append("ohdm_id bigint, ")
              .append("ohdm_object bigint, ")
              .append("id_way bigint REFERENCES ").append(WAYTABLE).append(" (osm_id), ")
              .append("valid boolean);");
      this.setupTable(NODETABLE, sqlNode.toString());
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

  private void saveNodeElement(NodeElement node) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
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

  private void updateNode(NodeElement newNode, NodeElement dbNode) {
    HashMap<String, String> map = new HashMap<>();
    if (!newNode.getLatitude().equals(dbNode.getLatitude())) {
      map.put("lat", newNode.getLatitude());
    }
    if (!newNode.getLongitude().equals(dbNode.getLongitude())) {
      map.put("lon", newNode.getLongitude());
    }
    this.updateNode(newNode.getID(), map);
  }

  private void updateNode(long id, HashMap<String, String> map) {
    StringBuilder sb = new StringBuilder("UPDATE ");
    sb.append(NODETABLE).append(" SET ");
    map.entrySet().stream().forEach((e) -> {
      sb.append(e.getKey()).append(" = ").append(e.getValue()).append(" ");
    });
    sb.append("WHERE osm_id = ?;");
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(sb.toString());
      stmt.setLong(1, id);
      stmt.executeUpdate();
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
        this.updateNode(newNode, dbNode);
        break;
      case 2:
        this.nodesExisting++;
        // do nothing, because node is ok
        break;
      default:
        break;
    }
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

  @Deprecated
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

  private void saveWayElement(WayElement way) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    if (way.getTagId() == null) {
      sb.append(WAYTABLE).append(" (osm_id) VALUES (?);");
    } else {
      sb.append(WAYTABLE).append(" (osm_id, tag) VALUES (?, ?);");
    }
    PreparedStatement stmt = null;
    try {
      stmt = connection.prepareStatement(sb.toString());
      stmt.setLong(1, way.getID());
      if (way.getTagId() != null) {
        stmt.setInt(2, way.getTagId());
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

  /**
   * http://wiki.openstreetmap.org/wiki/Way
   *
   * @param attributes
   * @param nds
   * @param tags
   */
  @Override
  public void addWay(HashMap<String, String> attributes, HashSet<NodeElement> nds, HashSet<TagElement> tags) {
    if (nds == null || nds.isEmpty()) {
      return; // a way without nodes makes no sense.
    }
    WayElement newWay = new WayElement(this, attributes, nds, tags);
    WayElement dbWay = selectWayById(newWay.getID());
    switch (matchWays(newWay, dbWay)) {
      case 0:
        this.waysNew++;
        this.saveWayElement(newWay);
        newWay.getNodes().stream().forEach((nd) -> {
          HashMap<String, String> map = new HashMap<>();
          map.put("id_way", String.valueOf(newWay.getID()));
          this.updateNode(nd.getID(), map);
        });
        break;
      case 1:
        this.waysChanged++;
        // update nodes column id_way in NODETABLE
        // new Node in this Way
        newWay.getNodes().stream().forEach((nd) -> {
          if (!dbWay.hasNodeId(nd.getID())) {
            HashMap<String, String> newNode = new HashMap<>();
            newNode.put("id_way", String.valueOf(newWay.getID()));
            this.updateNode(nd.getID(), newNode);
          }
        });
        // Node does not exist any more in this Way
        dbWay.getNodes().stream().forEach((nd) -> {
          if (!newWay.hasNodeId(nd.getID())) {
            HashMap<String, String> delNode = new HashMap<>();
            delNode.put("id_way", "null");
            this.updateNode(nd.getID(), delNode);
          }
        });
        break;
      case 2:
        this.waysExisting++;
        break;
      default:
        break;
    }
  }

  private WayElement selectWayById(long osm_id) {
    StringBuilder sqlWay = new StringBuilder("SELECT * FROM ");
    sqlWay.append(WAYTABLE).append(" WHERE osm_id = ? ;");
    StringBuilder sqlNodes = new StringBuilder("SELECT * FROM ");
    sqlNodes.append(NODETABLE).append(" WHERE id_way = ?;");
    PreparedStatement stmtNodes = null;
    PreparedStatement stmtWay = null;
    HashMap<String, String> attrWay = new HashMap<>();
    HashSet<NodeElement> nds = new HashSet<>();
    try {
      stmtNodes = connection.prepareStatement(sqlNodes.toString());
      stmtNodes.setLong(1, osm_id);
      ResultSet rsNodes = stmtNodes.executeQuery();
      while (rsNodes.next()) {
        HashMap<String, String> attrNode = new HashMap<>();
        attrNode.put("id", String.valueOf(rsNodes.getInt("osm_id")));
        attrNode.put("lat", rsNodes.getString("lat"));
        attrNode.put("lon", rsNodes.getString("long"));
        nds.add(new NodeElement(attrNode, null));
      }

      if (!nds.isEmpty()) {
        stmtWay = connection.prepareStatement(sqlWay.toString());
        stmtWay.setLong(1, osm_id);
        ResultSet rsWay = stmtWay.executeQuery();
        if (rsWay.next()) {
          attrWay.put("id", String.valueOf(rsWay.getInt("osm_id")));
        }
      }
    } catch (SQLException e) {
      logger.print(1, e.getLocalizedMessage(), true);
    } finally {
      try {
        if (stmtNodes != null) {
          stmtNodes.close();
        }
      } catch (SQLException e) {
      }
      try {
        if (stmtWay != null) {
          stmtNodes.close();
        }
      } catch (SQLException e) {
      }
    }
    if (nds.isEmpty()) {
      return null;
    } else {
      return new WayElement(attrWay, nds, null);
    }
  }

  /**
   * checks if the ways have the same nodes (compared by their id)
   *
   * @param newWay
   * @param dbWay
   * @return 0: way does not exist, 1: way has changed, 2: ways are the same
   */
  private Integer matchWays(WayElement newWay, WayElement dbWay) {
    Integer state = 2;
    if (dbWay == null) {
      return 0;
    } else {
      // checks if all nodes in the new way are in the stored way
      for (NodeElement nd : newWay.getNodes()) {
        if (!dbWay.hasNodeId(nd.getID())) {
          state = 1;
        }
      }
      // checks if all nodes in the stored way are also in the new way
      if (state != 1) {
        for (NodeElement nd : dbWay.getNodes()) {
          if (!newWay.hasNodeId(nd.getID())) {
            state = 1;
          }
        }
      }
    }
    return state;
  }

  /**
   * OSM Relations are defined here: http://wiki.openstreetmap.org/wiki/Relation
   *
   * @param attributes
   * @param members
   * @param tags
   */
  @Override
  public void addRelation(HashMap<String, String> attributes, HashSet<MemberElement> members, HashSet<TagElement> tags) {
    if (members == null || members.isEmpty() || tags == null) {
      return; // empty relations makes no sense;
    }
    RelationElement newRelation = new RelationElement(attributes, members, tags);
    
  }

  @Override
  public void printStatus() {
    logger.print(0, "\n\t\t|---------------|---------------|---------------|");
    logger.print(0, "\t\t| new\t\t| changed\t| existing\t|");
    logger.print(0, "|---------------|---------------|---------------|---------------|");
    logger.print(0, "| Nodes\t\t| " + this.nodesNew + "\t\t| " + this.nodesChanged + "\t\t| " + this.nodesExisting + "\t\t|");
    logger.print(0, "|---------------|---------------|---------------|---------------|");
    logger.print(0, "| Ways\t\t| " + this.waysNew + "\t\t| " + this.waysChanged + "\t\t| " + this.waysExisting + "\t\t|");
    logger.print(0, "|---------------|---------------|---------------|---------------|");
    logger.print(0, "| Relations\t| " + this.relNew + "\t\t| " + this.relChanged + "\t\t| " + this.relExisting + "\t\t|");
    logger.print(0, "|---------------|---------------|---------------|---------------|");
  }

  public Connection getConnection() {
    return this.connection;
  }

  ////////////////////////////////////////////////////////////////////
  //                       counter                                  //
  ////////////////////////////////////////////////////////////////////
  public void incrementNodeCounter(String type) {
    switch (type) {
      case "new":
        this.nodesNew++;
        break;
      case "changed":
        this.nodesChanged++;
        break;
      case "existing":
        this.nodesExisting++;
        break;
      default:
        break;
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
