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
  public static final String RELATIONTABLE = "Relations";
  public static final String MAPTABLE = "RelationMember";
  public static final String CLASSIFICATIONTABLE = "Classification";
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
  private String path;
  private String schema;
  private Connection connection;

  private String importMode;
  private Integer tmpStorageSize;

  private final MyLogger logger;
  private final Classification classification;
  private final Config config;

  private static SQLImportCommandBuilder instance = null;

  public static SQLImportCommandBuilder getInstance() {
    if (instance == null) {
      instance = new SQLImportCommandBuilder();
    }
    return instance;
  }

  private SQLImportCommandBuilder() {
    this.config = Config.getInstance();
    this.logger = MyLogger.getInstance();
    this.classification = Classification.getInstance();
    importMode = config.getValue("importMode");
    tmpStorageSize = Integer.valueOf(config.getValue("tmpStorageSize")) - 1;
    try {
      this.user = config.getValue("db_user");
      this.pwd = config.getValue("db_password");
      this.serverName = config.getValue("db_serverName");
      this.portNumber = config.getValue("db_portNumber");
      this.path = config.getValue("db_path");
      this.schema = config.getValue("db_schema");
      Properties connProps = new Properties();
      connProps.put("user", this.user);
      connProps.put("password", this.pwd);
      this.connection = DriverManager.getConnection(
              "jdbc:postgresql://" + this.serverName
              + ":" + this.portNumber + "/" + this.path, connProps);
      if (!this.schema.equalsIgnoreCase("")) {
        StringBuilder sql = new StringBuilder("SET search_path = ");
        sql.append(this.schema);
        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
          stmt.execute();
          logger.print(4, "schema altered");
        } catch (SQLException e) {
          logger.print(4, "failed to alter schema: " + e.getLocalizedMessage());
        }
      }
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
    l.add(MAPTABLE); // dependencies to way, node and relations
    l.add(WAYTABLE); // dependencies to node
    l.add(NODETABLE);
    l.add(RELATIONTABLE);
    return l;
  }

  private void dropTables() throws SQLException {
    if (config.getValue("db_dropTables").equalsIgnoreCase("yes")) {
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
    logger.print(4, "creating table if not exists: " + table);
    PreparedStatement stmt = null;
    StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
    sql.append(table).append(sqlCreate);
    try {
      stmt = connection.prepareStatement(sql.toString());
      stmt.execute();
    } catch (SQLException ex) {
      logger.print(1, ex.getLocalizedMessage(), true);
      logger.print(1, sql.toString());
    } finally {
      if (stmt != null) {
        stmt.close();
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
      this.loadClassification();
      StringBuilder sqlWay = new StringBuilder();
      sqlWay.append(" (osm_id bigint PRIMARY KEY, ")
              .append("tag bigint REFERENCES ").append(CLASSIFICATIONTABLE).append(" (classcode), ")
              .append("ohdm_id bigint, ")
              .append("ohdm_object bigint, ")
              .append("valid boolean);");
      this.setupTable(WAYTABLE, sqlWay.toString());
      StringBuilder sqlNode = new StringBuilder();
      sqlNode.append(" (osm_id bigint PRIMARY KEY, ")
              .append("long character varying(").append(MAX_ID_SIZE).append("), ")
              .append("lat character varying(").append(MAX_ID_SIZE).append("), ")
              .append("tag bigint REFERENCES ").append(CLASSIFICATIONTABLE).append(" (classcode), ")
              .append("ohdm_id bigint, ")
              .append("ohdm_object bigint, ")
              .append("id_way bigint REFERENCES ").append(WAYTABLE).append(" (osm_id), ")
              .append("valid boolean);");
      this.setupTable(NODETABLE, sqlNode.toString());
      StringBuilder sqlRelation = new StringBuilder();
      sqlRelation.append(" (osm_id bigint PRIMARY KEY, ")
              .append("tag bigint REFERENCES ").append(CLASSIFICATIONTABLE).append(" (classcode), ")
              .append("ohdm_id bigint, ")
              .append("ohdm_object bigint, ")
              .append("valid boolean);");
      this.setupTable(RELATIONTABLE, sqlRelation.toString());
      StringBuilder sqlRelMember = new StringBuilder();
      sqlRelMember.append(" (relation_id bigint REFERENCES ").append(RELATIONTABLE).append(" (osm_id) NOT NULL, ")
              .append("way_id bigint REFERENCES ").append(WAYTABLE).append(" (osm_id), ")
              .append("node_id bigint REFERENCES ").append(NODETABLE).append(" (osm_id), ")
              .append("member_rel_id bigint REFERENCES ").append(RELATIONTABLE).append(" (osm_id));");
      this.setupTable(MAPTABLE, sqlRelMember.toString());
    } catch (SQLException e) {
      System.err.println("error while setting up tables: " + e.getLocalizedMessage());
    }
    logger.print(4, "--- finished setting up tables ---", true);
  }

  private void loadClassification() {
    if (config.getValue("db_classificationTable").equals("useExisting")) {
      Statement stmt = null;
      try {
        stmt = connection.createStatement();
        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        if (!this.schema.equalsIgnoreCase("")) {
          sb.append(schema).append(".");
        }
        sb.append(CLASSIFICATIONTABLE).append(";");
        try (ResultSet rs = stmt.executeQuery(sb.toString())) {
          while (rs.next()) {
            this.classification.put(rs.getString("class"), rs.getString("subclass"), rs.getInt("classcode"));
          }
        }
      } catch (SQLException e) {
        logger.print(4, "classification loadingt failed: " + e.getLocalizedMessage(), true);
      } finally {
        if (stmt != null) {
          try {
            stmt.close();
          } catch (SQLException e) {
          }
        }
      }
    } else {
      // ToDo needs to be implemented for xml import
    }
  }

  // can later be user for config flag db_classificationTable -> createNew
  @Deprecated
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

  @Deprecated
  private void checkIfTableExists(String table) throws SQLException {
    StringBuilder sql = new StringBuilder("SELECT '");
    sql.append(table).append("'::regclass;");
    PreparedStatement stmt = connection.prepareStatement(sql.toString());
    stmt.execute();
  }

  @Deprecated
  private void resetSequence(Statement stmt, String sequence) throws SQLException {
    try {
      stmt.execute("drop sequence " + sequence + ";");
    } catch (SQLException ee) { /* ignore */ }
    stmt.execute("create sequence " + sequence + ";");
  }

  private Map<String, NodeElement> nodes = new HashMap<>();

  private void saveNodeElements() {
    StringBuilder sqlWO = new StringBuilder("INSERT INTO ");
    sqlWO.append(NODETABLE).append(" (osm_id, long, lat, valid) VALUES");
    StringBuilder sql = new StringBuilder("INSERT INTO ");
    sql.append(NODETABLE).append(" (osm_id, long, lat, tag, valid) VALUES");
    for (Map.Entry<String, NodeElement> entry : nodes.entrySet()) {
      if (entry.getValue().getTags() == null) {
        sqlWO.append(" (").append(entry.getKey()).append(", ")
                .append(entry.getValue().getLatitude()).append(", ")
                .append(entry.getValue().getLongitude()).append(", ")
                .append("true").append("),");
      } else {
        sql.append(" (").append(entry.getKey()).append(", ")
                .append(entry.getValue().getLatitude()).append(", ")
                .append(entry.getValue().getLongitude()).append(", ")
                .append(entry.getValue().getTagId()).append(", ")
                .append("true").append("),");
      }
    }
    try (PreparedStatement stmt = connection.prepareStatement(sqlWO.deleteCharAt(sqlWO.length() - 1).append(";").toString())) {
      stmt.execute();
    } catch (SQLException e) {
      logger.print(1, "saveNodeElements() " + e.getLocalizedMessage(), true);
      logger.print(3, sqlWO.toString());
    }
    try (PreparedStatement stmt = connection.prepareStatement(sql.deleteCharAt(sql.length() - 1).append(";").toString())) {
      stmt.execute();
    } catch (SQLException e) {
      logger.print(1, "saveNodeElements() " + e.getLocalizedMessage(), true);
      logger.print(3, sql.toString());
    }
    nodes.clear();
  }

  private void saveNodeElement(NodeElement node) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    if (node.getTagId() == null) {
      sb.append(NODETABLE).append(" (osm_id, long, lat, valid) VALUES (?, ?, ?, true);");
    } else {
      sb.append(NODETABLE).append(" (osm_id, long, lat, tag, valid) VALUES (?, ?, ?, ?, true);");
    }
    try (PreparedStatement stmt = connection.prepareStatement(sb.toString())) {
      stmt.setLong(1, node.getID());
      stmt.setString(2, node.getLongitude());
      stmt.setString(3, node.getLatitude());
      if (node.getTagId() != null) {
        stmt.setInt(4, node.getTagId());
      }
      stmt.execute();
    } catch (SQLException e) {
      logger.print(1, "saveNodeElement(NodeElement) " + e.getLocalizedMessage(), true);
      logger.print(3, sb.toString());
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
    try (PreparedStatement stmt = connection.prepareStatement(sb.toString())) {
      stmt.setLong(1, id);
      stmt.executeUpdate();
    } catch (SQLException e) {
      logger.print(1, e.getLocalizedMessage(), true);
      logger.print(3, sb.toString());
    }
  }

  @Override
  public void addNode(HashMap<String, String> attributes, HashSet<TagElement> tags) {
    NodeElement newNode = new NodeElement(this, attributes, tags);
    if (importMode.equalsIgnoreCase("update")) {
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
          // todo: set valid to true
          break;
        default:
          break;
      }
    } else if (importMode.equalsIgnoreCase("initial_import")) {
      nodes.put(String.valueOf(newNode.getID()), newNode);
      if (nodes.size() > this.tmpStorageSize) {
        nodesNew += nodes.size();
        this.saveNodeElements();
        this.printStatusShort(4);
      }
    } else {
      logger.print(1, "not supported importMode in config file");
    }
  }

  private NodeElement selectNodeById(long osm_id) {
    StringBuilder sb = new StringBuilder("SELECT * FROM ");
    sb.append(NODETABLE).append(" WHERE osm_id = ?;");
    HashMap<String, String> attributes = null;
    try (PreparedStatement stmt = connection.prepareStatement(sb.toString())) {
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
      logger.print(3, sb.toString());
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

  private final HashMap<String, WayElement> ways = new HashMap<>();

  private void saveWayElements() {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(WAYTABLE).append("(osm_id, tag, valid) VALUES");
    StringBuilder sqlUpdateNodes = new StringBuilder();
    ways.entrySet().stream().filter((entry) -> (entry.getValue().getTags() != null)).forEach((entry) -> {
      sb.append(" (").append(entry.getKey()).append(", ")
              .append(entry.getValue().getTagId()).append(", ")
              .append("true").append("),");
      sqlUpdateNodes.append("UPDATE ").append(NODETABLE)
              .append(" SET id_way = ").append(entry.getKey())
              .append(", valid = true WHERE osm_id IN (");
      entry.getValue().getNodes().stream().forEach((nd) -> {
        sqlUpdateNodes.append(String.valueOf(nd.getID())).append(", ");
      });
      sqlUpdateNodes.delete(sqlUpdateNodes.length() - 2, sqlUpdateNodes.length() - 1).append(");\n");
    });
    sb.deleteCharAt(sb.length() - 1).append(";");
    try (PreparedStatement stmt = connection.prepareStatement(sb.toString())) {
      stmt.execute();
    } catch (SQLException ex) {
      logger.print(1, ex.getLocalizedMessage(), true);
      logger.print(4, sb.toString());
    }
    try (PreparedStatement stmtUpdate = connection.prepareStatement(sqlUpdateNodes.toString())) {
      stmtUpdate.execute();
    } catch (SQLException e) {
      logger.print(1, e.getLocalizedMessage(), true);
      logger.print(4, sqlUpdateNodes.toString());
    }
    ways.clear();
  }

  private void saveWayElement(WayElement way) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    if (way.getTagId() == null) {
      sb.append(WAYTABLE).append(" (osm_id, valid) VALUES (?, true);");
    } else {
      sb.append(WAYTABLE).append(" (osm_id, tag, valid) VALUES (?, ?, true);");
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
    if (!nodes.isEmpty()) {
      nodesNew += nodes.size();
      this.saveNodeElements();
      printStatusShort(1);
      logger.print(1, "finished saving nodes, continuing with ways", true);
    }
    WayElement newWay = new WayElement(this, attributes, nds, tags);

    if (importMode.equalsIgnoreCase("update")) {
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
    } else if (importMode.equalsIgnoreCase("initial_import")) {
      this.waysNew++;
      this.ways.put(String.valueOf(newWay.getID()), newWay);
      if (((ways.size() * 10) + 1) % tmpStorageSize == 0) {
        this.saveWayElements();
        this.printStatusShort(4);
      }
    } else {
      logger.print(1, "not supported importMode in config file");
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
          stmtWay.close();
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

  private final HashMap<String, RelationElement> rels = new HashMap<>();

  private void saveRelElements() {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(RELATIONTABLE).append(" (osm_id, tag, valid) VALUES");
    boolean nodeInside = false;
    StringBuilder sqlMapNode = new StringBuilder("INSERT INTO ");
    sqlMapNode.append(MAPTABLE).append(" (relation_id, node_id) VALUES");
    boolean wayInside = false;
    StringBuilder sqlMapWay = new StringBuilder("INSERT INTO ");
    sqlMapWay.append(MAPTABLE).append(" (relation_id, way_id) VALUES");
    boolean relInside = false;
    StringBuilder sqlMapRel = new StringBuilder("INSERT INTO ");
    sqlMapRel.append(MAPTABLE).append(" (relation_id, member_rel_id) VALUES");
    for (Map.Entry<String, RelationElement> entry : rels.entrySet()) {
      sb.append(" (").append(entry.getKey()).append(", ")
              .append(entry.getValue().getTagId()).append(", ")
              .append("true").append("),");
      for (MemberElement member : entry.getValue().getMember()) {
        switch (member.getType()) {
          case "node":
            sqlMapNode.append(" (").append(entry.getKey()).append(", ")
                    .append(member.getId()).append("),");
            nodeInside = true;
            break;
          case "way":
            sqlMapWay.append(" (").append(entry.getKey()).append(", ")
                    .append(member.getId()).append("),");
            wayInside = true;
            break;
          case "relation":
            sqlMapRel.append(" (").append(entry.getKey()).append(", ")
                    .append(member.getId()).append("),");
            relInside = true;
            break;
          default:
            logger.print(3, "member with incorrect type");
            break;
        }
        member.getId();
      }
    }
    try (PreparedStatement stmt = connection.prepareStatement(
            sb.deleteCharAt(sb.length() - 1).append(";").toString())) {
      stmt.execute();
    } catch (SQLException e) {
      logger.print(1, e.getLocalizedMessage(), true);
      logger.print(4, sb.toString());
    }
    if (nodeInside) {
      try (PreparedStatement stmt = connection.prepareStatement(
              sqlMapNode.deleteCharAt(sqlMapNode.length() - 1).append(";").toString())) {
        stmt.execute();
      } catch (SQLException e) {
        logger.print(1, e.getLocalizedMessage(), true);
        logger.print(4, sqlMapNode.toString());
      }
    }
    if (wayInside) {
      try (PreparedStatement stmt = connection.prepareStatement(
              sqlMapWay.deleteCharAt(sqlMapWay.length() - 1).append(";").toString())) {
        stmt.execute();
      } catch (SQLException e) {
        logger.print(1, e.getLocalizedMessage(), true);
        logger.print(4, sqlMapWay.toString());
      }
    }
    if (relInside) {
      try (PreparedStatement stmt = connection.prepareStatement(
              sqlMapRel.deleteCharAt(sqlMapRel.length() - 1).append(";").toString())) {
        stmt.execute();
      } catch (SQLException e) {
        logger.print(1, e.getLocalizedMessage(), true);
        logger.print(4, sqlMapRel.toString());
      }
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
  public void addRelation(HashMap<String, String> attributes, HashSet<MemberElement> members, HashSet<TagElement> tags
  ) {
    if (members == null || members.isEmpty() || tags == null) {
      return; // empty relations makes no sense;
    }
    if (!ways.isEmpty()) {
      waysNew += ways.size();
      saveWayElements();
      this.printStatusShort(1);
      logger.print(1, "finished saving ways, continuing with relations", true);
    }
    RelationElement newRel = new RelationElement(attributes, members, tags);
    if (importMode.equalsIgnoreCase("update")) {
      // todo
      // todo
      // todo
      // todo
      // todo
      // todo
      // todo
      // todo
      // todo
      // todo
      // todo
      // todo
      // todo
    } else if (importMode.equalsIgnoreCase("initial_import")) {
      this.relNew++;
      this.rels.put(String.valueOf(newRel.getID()), newRel);
      if (((rels.size() * 100) + 1) % tmpStorageSize == 0) {
        this.saveRelElements();
        this.printStatusShort(4);
      }
    } else {
      logger.print(1, "not supported importMode in config file");
    }
  }

  @Override
  public void flush() {
    if (!nodes.isEmpty()) {
      this.saveNodeElements();
    }
    if (!ways.isEmpty()) {
      this.saveWayElements();
    }
    if (!rels.isEmpty()) {
      this.saveRelElements();
    }
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

  public void printStatusShort(int level) {
    logger.print(level, "n: " + nodesNew + " w: " + waysNew, true);
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
    // ToDo Select from db
    return null;
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
