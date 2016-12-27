package osm2inter;

import osm.OSMClassification;
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
import util.Parameter;

/**
 *
 * @author thsc, Sven Petsche
 */
public class SQLImportCommandBuilder implements ImportCommandBuilder, ElementStorage {

  public static final String TAGTABLE = "Tags";
  public static final String NODETABLE = "Nodes";
  public static final String WAYTABLE = "Ways";
  public static final String RELATIONTABLE = "Relations";
  public static final String RELATIONMEMBER = "RelationMember";
  public static final String CLASSIFICATIONTABLE = "Classification";
  public static final String WAYMEMBER = "WayNodes";
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
  private Connection targetConnection;

  private String importMode = "initial_import";
  private Integer tmpStorageSize;

  private final MyLogger logger;
  private final Classification classification;
  private final Config config;

  private static SQLImportCommandBuilder instance = null;

  public static SQLImportCommandBuilder getInstance(Parameter parameter) {
    if (instance == null) {
      instance = new SQLImportCommandBuilder(parameter);
    }
    return instance;
  }
    private final Parameter parameter;

  private SQLImportCommandBuilder(Parameter parameter) {
    this.parameter = parameter;
    this.config = Config.getInstance();
    this.logger = MyLogger.getInstance();
    this.classification = Classification.getInstance();
//    importMode = config.getValue("importMode");
//    tmpStorageSize = Integer.valueOf(config.getValue("tmpStorageSize")) - 1;
    tmpStorageSize = 10000 - 1;
    
    try {
//      this.user = config.getValue("db_user");
//      this.pwd = config.getValue("db_password");
//      this.serverName = config.getValue("db_serverName");
//      this.portNumber = config.getValue("db_portNumber");
//      this.path = config.getValue("db_path");
//      this.schema = config.getValue("db_schema");
      
      this.user = this.parameter.getUserName();
      this.pwd = this.parameter.getPWD();
      this.serverName = this.parameter.getServerName();
      this.portNumber = this.parameter.getPortNumber();
      this.path = this.parameter.getdbName();
      this.schema = this.parameter.getSchema();
      
      Properties connProps = new Properties();
      connProps.put("user", this.user);
      connProps.put("password", this.pwd);
      this.targetConnection = DriverManager.getConnection(
              "jdbc:postgresql://" + this.serverName
              + ":" + this.portNumber + "/" + this.path, connProps);
      if (!this.schema.equalsIgnoreCase("")) {
        StringBuilder sql = new StringBuilder("SET search_path = ");
        sql.append(this.schema);
        try (PreparedStatement stmt = targetConnection.prepareStatement(sql.toString())) {
          stmt.execute();
          logger.print(4, "schema altered");
        } catch (SQLException e) {
          logger.print(4, "failed to alter schema: " + e.getLocalizedMessage());
        }
      }
    } catch (SQLException e) {
      logger.print(0, "cannot connect to database: " + e.getLocalizedMessage());
    }

    if (this.targetConnection == null) {
      System.err.println("cannot connect to database: reason unknown");
    }
    this.setupKB();
  }

  private List<String> createTablesList() {
    List<String> l = new ArrayList<>();
    l.add(RELATIONMEMBER); // dependencies to way, node and relations
    l.add(WAYTABLE); // dependencies to node
    l.add(NODETABLE);
    l.add(RELATIONTABLE);
    l.add(CLASSIFICATIONTABLE);
    l.add(WAYMEMBER);
    return l;
  }

  private void dropTables() throws SQLException {
//    if (config.getValue("db_dropTables").equalsIgnoreCase("yes")) {
      StringBuilder sqlDel = new StringBuilder("DROP TABLE ");
      PreparedStatement delStmt = null;
      List<String> tables = this.createTablesList();
      tables.stream().forEach((table) -> {
        sqlDel.append(table).append(", ");
      });
      sqlDel.delete(sqlDel.lastIndexOf(","), sqlDel.length()).replace(sqlDel.length(), sqlDel.length(), ";");
      try {
        delStmt = targetConnection.prepareStatement(sqlDel.toString());
        delStmt.execute();
        logger.print(4, "tables dropped");
      } catch (SQLException e) {
        logger.print(4, "failed to drop table: " + e.getLocalizedMessage());
      } finally {
        if (delStmt != null) {
          delStmt.close();
        }
      }
//    }
  }

  private void setupTable(String table, String sqlCreate) throws SQLException {
    logger.print(4, "creating table if not exists: " + table);
    PreparedStatement stmt = null;
    StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
    sql.append(table).append(sqlCreate);
    try {
      stmt = targetConnection.prepareStatement(sql.toString());
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
  
    private boolean isClassName(String key) {
        return OSMClassification.getOSMClassification().osmFeatureClasses.keySet().contains(key);
    }
  
    /**
     * @return -1 if no known class and sub class name, a non-negative number 
     * otherwise
     */
    private int getOHDMClassID(String className, String subClassName) {
        String fullClassName = this.createFullClassName(className, subClassName);
        
        // find entry
        Integer id = this.classIDs.get(fullClassName);
        if(id != null) {
            return id;
        }
        
        // try undefined
        fullClassName = this.createFullClassName(className, OSMClassification.UNDEFINED);
        id = this.classIDs.get(fullClassName);
        if(id != null) {
            return id;
        }
        
//        System.out.println("not found: " + this.createFullClassName(className, subClassName));
        
        // else
        return -1;
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
              .append("classcode bigint REFERENCES ").append(CLASSIFICATIONTABLE).append(" (classcode), ")
              .append("serializedTags character varying, ")
              .append("ohdm_geom_id bigint, ")
              .append("ohdm_object_id bigint, ")
              .append("node_ids character varying, ")
              .append("is_part boolean DEFAULT false, ")
              .append("valid boolean);");
      this.setupTable(WAYTABLE, sqlWay.toString());

      StringBuilder sqlNode = new StringBuilder();
      sqlNode.append(" (osm_id bigint PRIMARY KEY, ")
              .append("classcode bigint REFERENCES ").append(CLASSIFICATIONTABLE).append(" (classcode), ")
              .append("serializedTags character varying, ")
              .append("longitude character varying(").append(MAX_ID_SIZE).append("), ")
              .append("latitude character varying(").append(MAX_ID_SIZE).append("), ")
              .append("ohdm_geom_id bigint, ")
              .append("ohdm_object_id bigint, ")
//              .append("id_way bigint REFERENCES ").append(WAYTABLE).append(" (osm_id), ")
              .append("is_part boolean DEFAULT false, ")
              .append("valid boolean);");
      this.setupTable(NODETABLE, sqlNode.toString());

      StringBuilder sqlRelation = new StringBuilder();
      sqlRelation.append(" (osm_id bigint PRIMARY KEY, ")
              .append("classcode bigint REFERENCES ").append(CLASSIFICATIONTABLE).append(" (classcode), ")
              .append("serializedTags character varying, ")
              .append("ohdm_geom_id bigint, ")
              .append("ohdm_object_id bigint, ")
              .append("member_ids character varying, ")
              .append("valid boolean);");
      this.setupTable(RELATIONTABLE, sqlRelation.toString());
      
      StringBuilder sqlWayMember = new StringBuilder();
      sqlWayMember.append(" (way_id bigint, ");
      sqlWayMember.append("node_id bigint");
      sqlWayMember.append(");");
      this.setupTable(WAYMEMBER, sqlWayMember.toString());
      
      StringBuilder sqlRelMember = new StringBuilder();
      sqlRelMember.append(" (relation_id bigint REFERENCES ").append(RELATIONTABLE).append(" (osm_id) NOT NULL, ")
//              .append("way_id bigint REFERENCES ").append(WAYTABLE).append(" (osm_id), ")
              .append("way_id bigint, ")
//              .append("node_id bigint REFERENCES ").append(NODETABLE).append(" (osm_id), ")
              .append("node_id bigint, ")
//              .append("member_rel_id bigint REFERENCES ").append(RELATIONTABLE).append(" (osm_id));");
              .append("member_rel_id bigint, ")
              .append("role character varying);");
      this.setupTable(RELATIONMEMBER, sqlRelMember.toString());
      
    } catch (SQLException e) {
      System.err.println("error while setting up tables: " + e.getLocalizedMessage());
    }
    logger.print(4, "--- finished setting up tables ---", true);
  }
  
  private HashMap<String, Integer> classIDs = new HashMap<>();

  private void loadClassification() throws SQLException {      
      String db_classificationTable = config.getValue("db_classificationTable");
    if (db_classificationTable!= null && db_classificationTable.equalsIgnoreCase("useExisting")) {
      Statement stmt = null;
      try {
        stmt = targetConnection.createStatement();
        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        if (!this.schema.equalsIgnoreCase("")) {
          sb.append(schema).append(".");
        }
        sb.append(CLASSIFICATIONTABLE).append(";");
        try (ResultSet rs = stmt.executeQuery(sb.toString())) {
          while (rs.next()) {
            this.classification.put(rs.getString("class"), rs.getString("subclassname"), rs.getInt("classcode"));
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
        StringBuilder sqlRelMember = new StringBuilder();
        sqlRelMember.append("(classcode bigint PRIMARY KEY, ")
          .append("classname character varying, ")
          .append("subclassname character varying);");

        this.setupTable(CLASSIFICATIONTABLE, sqlRelMember.toString());

        // fill that table
        // set up classification table from scratch
        OSMClassification osmClassification = OSMClassification.getOSMClassification();
        
        // init first line: unknown classification
        StringBuilder insertStatement = new StringBuilder();
        insertStatement.append("INSERT INTO ")
                .append(CLASSIFICATIONTABLE)
                .append(" VALUES (-1, 'no_class', 'no_subclass');");

        PreparedStatement stmt = null;
        try {
            stmt = targetConnection.prepareStatement(insertStatement.toString());
            stmt.execute();
        } catch (SQLException ex) {
            logger.print(1, ex.getLocalizedMessage(), true);
            logger.print(1, insertStatement.toString());
        } finally {
          if (stmt != null) {
            stmt.close();
          }
        }

        // no append real data
        int id = 0;

        // create classification table
        // iterate classes
        Iterator<String> classIter = osmClassification.osmFeatureClasses.keySet().iterator();

        while(classIter.hasNext()) {
            String className = classIter.next();
            List<String> subClasses = osmClassification.osmFeatureClasses.get(className);
            Iterator<String> subClassIter = subClasses.iterator();

            while(subClassIter.hasNext()) {
                String subClassName = subClassIter.next();
                
                // keep in memory
                Integer idInteger = id;
                String fullClassName = this.createFullClassName(className, subClassName);
                
                this.classIDs.put(fullClassName, idInteger);
                
                // add to database
                insertStatement = new StringBuilder();
                insertStatement.append("INSERT INTO ")
                        .append(CLASSIFICATIONTABLE)
                        .append(" VALUES (")
                        .append(id++)
                        .append(", '")
                        .append(className)
                        .append("', '")
                        .append(subClassName)
                        .append("');");
            
                stmt = null;
                try {
                    stmt = targetConnection.prepareStatement(insertStatement.toString());
                    stmt.execute();
                } catch (SQLException ex) {
                    logger.print(1, ex.getLocalizedMessage(), true);
                    logger.print(1, insertStatement.toString());
                } finally {
                  if (stmt != null) {
                    stmt.close();
                  }
                }
            }
            
        }
    }
  }

  // can later be user for config flag db_classificationTable -> createNew
  @Deprecated
  private void importWhitelist() {
    // import whitelist to auto create ids
    Statement stmtImport = null;
    try {
      stmtImport = targetConnection.createStatement();
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
      stmtSelect = targetConnection.createStatement();
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
    PreparedStatement stmt = targetConnection.prepareStatement(sql.toString());
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
  
    private String createFullClassName(String className, String subclassname) {
        return className + "_" + subclassname;
    }
  
    /**
     * TODO: Add here translation of unused OSM types to OHDM types etc.
     * @param osmElement
     * @return 
     */
    private int getOHDMClassID(OSMElement osmElement) {
      // a node can have tags which can describe geometrie feature classe
        ArrayList<TagElement> tags = osmElement.getTags();
        if(tags == null) return -1;
        
        Iterator<TagElement> tagIter = tags.iterator();
        if(tagIter == null) return -1;
        
        while(tagIter.hasNext()) {
            TagElement tag = tagIter.next();

            // get attributes of that tag
            Iterator<String> keyIter = tag.attributes.keySet().iterator();
            while(keyIter.hasNext()) {
                String key = keyIter.next();

                // is this key name of a feature class?
                if(this.isClassName(key)) {
                    String value = tag.attributes.get(key);

                    // find id of class / subclass
                    return this.getOHDMClassID(key, value);
                }
            }
        }

        // there is no class description - sorry
        return -1;
    }
  
  private void saveNodeElements() throws SQLException {
      /*
      Iterator<NodeElement> nodeIter = this.nodes.values().iterator();
      while(nodeIter.hasNext()) {
          this.saveNodeElement(nodeIter.next());
      }
      */
      
    SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection, this.logger);
      
    for (Map.Entry<String, NodeElement> entry : nodes.entrySet()) {
        NodeElement node = entry.getValue();
        int classID = OSMClassification.getOSMClassification().getOHDMClassID(node);
        String sTags = node.getSerializedTagsAndAttributes();
        
//        StringBuilder sq = new StringBuilder();
        
        sq.append("INSERT INTO ");
        sq.append(NODETABLE);
        sq.append(" (osm_id, longitude, latitude, classcode, serializedtags, valid) VALUES");

        sq.append(" (");
        sq.append(entry.getKey());
        sq.append(", ");
        sq.append(entry.getValue().getLatitude());
        sq.append(", ");
        sq.append(entry.getValue().getLongitude());
        sq.append(", ");
        sq.append(classID);
        sq.append(", '");
        sq.append(sTags);
        sq.append("', ");
        sq.append("true");
        sq.append("); ");

//        sq.flush();
        
        /*
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(NODETABLE).append(" (osm_id, longitude, latitude, classcode, valid) VALUES");
        
//        if (entry.getValue().getTags() == null) {
        sql.append(" (").append(entry.getKey()).append(", ")
                .append(entry.getValue().getLatitude()).append(", ")
                .append(entry.getValue().getLongitude()).append(", ")
                .append(classID).append(", ")
                .append("true").append(");");
        */
        /*
        try (PreparedStatement stmt = connection.prepareStatement(sq.toString())) {
          stmt.execute();
        } catch (SQLException e) {
          logger.print(1, "saveNodeElements() " + e.getLocalizedMessage(), true);
          logger.print(3, sq.toString());
        }
        catch(RuntimeException re) {
            logger.print(1, "CHAOS with " + sq.toString());
            logger.print(1, re.getLocalizedMessage());
        }
        */
    }
    
    sq.forceExecute();
      
    nodes.clear();
  }

  private void saveNodeElement(NodeElement node) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    int classID = OSMClassification.getOSMClassification().getOHDMClassID(node);
    if (classID > -1) {
      sb.append(NODETABLE).append(" (osm_id, longitude, latitude, valid) VALUES (?, ?, ?, true);");
    } else {
      sb.append(NODETABLE).append(" (osm_id, longitude, latitude, classcode, valid) VALUES (?, ?, ?, ?, true);");
    }
    try (PreparedStatement stmt = targetConnection.prepareStatement(sb.toString())) {
      stmt.setLong(1, node.getID());
      stmt.setString(2, node.getLongitude());
      stmt.setString(3, node.getLatitude());
      if (classID > -1) {
        stmt.setInt(4, classID);
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
      stmt = targetConnection.prepareStatement(sb.toString());
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
    try (PreparedStatement stmt = targetConnection.prepareStatement(sb.toString())) {
      stmt.setLong(1, id);
      stmt.executeUpdate();
    } catch (SQLException e) {
      logger.print(1, e.getLocalizedMessage(), true);
      logger.print(3, sb.toString());
    }
  }

  @Override
  public void addNode(HashMap<String, String> attributes, ArrayList<TagElement> tags) throws SQLException {
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
    try (PreparedStatement stmt = targetConnection.prepareStatement(sb.toString())) {
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

    private void saveWayElements() throws SQLException {
        // set up a sql queue
        SQLStatementQueue sqlQueue = new SQLStatementQueue(this.targetConnection, this.logger);
        SQLStatementQueue nodeIsPartSql = new SQLStatementQueue(this.targetConnection, this.logger);
        
          // figure out classification id.. which describes the
          // type of geometry (building, highway, those kind of things
        Iterator<Map.Entry<String, WayElement>> wayElementIter = ways.entrySet().iterator();

        int counter = 0;
        int rows = 0;
        logger.print(4, "saving ways.. set a star after 20 saved ways when flushing; max 50 stars each line");
        while(wayElementIter.hasNext()) {
            // NOTE FLUSH on sql queue is essentiell!! rest of the code is for debugging
            if(counter++ >= 50) {
                // IMPORTANT:
                sqlQueue.forceExecute();
                
                // debugging
                System.out.print("*");
                counter = 0;
                System.out.flush();
                
                if(rows++ >= 50) {
                    System.out.print("\n");
                    rows = 0;
                }
            }

            sqlQueue.append("INSERT INTO ");
            sqlQueue.append(WAYTABLE);
            sqlQueue.append("(osm_id, classcode, serializedtags, node_ids, valid) VALUES");
            
            /*
            StringBuilder sb = new StringBuilder("INSERT INTO ");
            sb.append(WAYTABLE).append("(osm_id, classcode, valid) VALUES");
            */

            Map.Entry<String, WayElement> wayElementEntry = wayElementIter.next();

            WayElement wayElement = wayElementEntry.getValue();

            // figure out geometry class (highway, building or such a thing
            int wayID = OSMClassification.getOSMClassification().getOHDMClassID(wayElement);

            String wayOSMID = wayElementEntry.getKey();

            String sTags = wayElement.getSerializedTagsAndAttributes();
            
            // serialize node ids
            StringBuilder nodeString = new StringBuilder();
            if(wayElement.getNodes() != null) {
                Iterator<NodeElement> wayNodeIter = wayElement.getNodes().iterator();
                boolean first = true;
                while(wayNodeIter.hasNext()) {
                    NodeElement wayNode = wayNodeIter.next();
                    
                    if(first) {
                        first = false;
                    } else {
                        nodeString.append(",");
                    }

                    nodeString.append(wayNode.getID());
                }
            }
            
            // lets add values to sql statement

            sqlQueue.append(" (");
            sqlQueue.append(wayOSMID);
            sqlQueue.append(", "); // osm_id
            sqlQueue.append(wayID);
            sqlQueue.append(", '");
            sqlQueue.append(sTags);
            sqlQueue.append("', '");
            sqlQueue.append(nodeString.toString());
            sqlQueue.append("', ");
            sqlQueue.append("true");
            sqlQueue.append(");"); // it's a valid way... it's still in OSM
            
            // add related nodes to way_node table and mark node to be part of something
            
            // iterate nodes
            if(wayElement.getNodes() != null) {
                // set up first part of sql statement
                String sqlStart = "INSERT INTO " + WAYMEMBER 
                        + "(way_id, node_id) VALUES ( " + wayOSMID + ", ";
                
                nodeIsPartSql.append("UPDATE nodes SET is_part=true WHERE ");
                Iterator<NodeElement> wayNodeIter = wayElement.getNodes().iterator();
                boolean first = true;
                while(wayNodeIter.hasNext()) {
                    NodeElement wayNode = wayNodeIter.next();

                    long nodeOSMID = wayNode.getID();

                    // set up sql statement - use queue for performace reasons
                    sqlQueue.append(sqlStart);
                    sqlQueue.append(nodeOSMID); // add node ID
                    sqlQueue.append(");"); // finish statement
                    
                    // update
                    if(!first) {
                        nodeIsPartSql.append(" OR ");
                    } else {
                        first = false;
                    }
                    nodeIsPartSql.append("osm_id=");
                    nodeIsPartSql.append(nodeOSMID); // add node ID
                }
                nodeIsPartSql.append("; ");
//                // flush remaining sql statements
//                sqlQueue.flush();
            }
        }
        nodeIsPartSql.forceExecute();
        // flush sql statements (required when using append variant)
        sqlQueue.forceExecute();
        this.ways.clear();
    }

  private void saveWayElement(WayElement way) {
      int classID = OSMClassification.getOSMClassification().getOHDMClassID(way);
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    if (classID < 0) {
      sb.append(WAYTABLE).append(" (osm_id, valid) VALUES (?, true);");
    } else {
      sb.append(WAYTABLE).append(" (osm_id, classcode, valid) VALUES (?, ?, true);");
    }
    PreparedStatement stmt = null;
    try {
      stmt = targetConnection.prepareStatement(sb.toString());
      stmt.setLong(1, way.getID());
      if (classID > -1) {
        stmt.setInt(2, classID);
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
  public void addWay(HashMap<String, String> attributes, ArrayList<NodeElement> nds, ArrayList<TagElement> tags) throws SQLException {
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
    ArrayList<NodeElement> nds = new ArrayList<>();
    try {
      stmtNodes = targetConnection.prepareStatement(sqlNodes.toString());
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
        stmtWay = targetConnection.prepareStatement(sqlWay.toString());
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

    private void saveRelElements() throws SQLException {
        SQLStatementQueue sq = new SQLStatementQueue(this.targetConnection, this.logger);
        
        for (Map.Entry<String, RelationElement> entry : rels.entrySet()) {
            RelationElement relationElement = entry.getValue();
            
            String osm_id = entry.getKey();
            int classID = OSMClassification.getOSMClassification().getOHDMClassID(relationElement);
            String sTags = relationElement.getSerializedTagsAndAttributes();
    
            String memberIDs = "";
            
            if(relationElement.getMember() != null) {
                // create member_ids
                StringBuilder memberIDsb = new StringBuilder();

                boolean first = true;
                for (MemberElement member : entry.getValue().getMember()) {
                    if(first) {
                        first = false;
                    } else {
                        memberIDsb.append(",");
                    }

                    memberIDsb.append(member.getID());
                }
                
                memberIDs = memberIDsb.toString();
            }

            sq.append("INSERT INTO ");
            sq.append(RELATIONTABLE);
            sq.append(" (osm_id, classcode, serializedtags, member_ids, valid) VALUES (");
            sq.append(osm_id);
            sq.append(", ");
            sq.append(classID);
            sq.append(", '");
            sq.append(sTags);
            sq.append("', '");
            sq.append(memberIDs);
            sq.append("', true);");
            
//            sq.flush();

        // add entry to member table and set flag in nodes or way tables
        ArrayList<Long> nodeMemberIDs = new ArrayList<>();
        ArrayList<Long> wayMemberIDs = new ArrayList<>();
            
        for (MemberElement member : entry.getValue().getMember()) {
            sq.append("INSERT INTO ");
            sq.append(RELATIONMEMBER);
            sq.append(" (relation_id, role, ");
            
            switch (member.getType()) {
            case "node":
                sq.append(" node_id)");  
                nodeMemberIDs.add(member.getID());
                break;
            case "way":
                sq.append(" way_id)"); 
                wayMemberIDs.add(member.getID());
                break;
            case "relation":
                sq.append(" member_rel_id)"); break;
            default:
                logger.print(3, "member with incorrect type"); break;
            }
            
            // add values
            sq.append(" VALUES (");
            sq.append(osm_id);
            sq.append(", '");
            sq.append(member.getRole());
            sq.append("', ");
            sq.append(member.getId());
            sq.append(");");
            
            // sq.flush();
            
            // member.getId();
          }
            sq.forceExecute(); // after each relation

            // update nodes and ways
            if(nodeMemberIDs.size() > 0) {
                sq.append("UPDATE nodes SET is_part=true WHERE ");
                boolean first = true;
                for (Long id : nodeMemberIDs) {
                    if(!first) {
                        sq.append(" OR ");
                    } else {
                        first = false;
                    }
                    sq.append("osm_id = ");
                    sq.append(id);
                }
                sq.append("; ");
            }
            
            if(wayMemberIDs.size() > 0) {
                sq.append("UPDATE ways SET is_part=true WHERE ");
                boolean first = true;
                for (Long id : wayMemberIDs) {
                    if(!first) {
                        sq.append(" OR ");
                    } else {
                        first = false;
                    }
                    sq.append("osm_id = ");
                    sq.append(id);
                }
                sq.append("; ");
            }
            sq.forceExecute();
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
  public void addRelation(HashMap<String, String> attributes, ArrayList<MemberElement> members, ArrayList<TagElement> tags
  ) throws SQLException {
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
  public void flush() throws SQLException {
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
    return this.targetConnection;
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
