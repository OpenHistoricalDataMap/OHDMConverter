package osm2inter_v2;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;
import osm.OSMClassification;
import osm2inter.AbstractElement;
import osm2inter.Classification;
import util.Parameter;
import util.SQLStatementQueue;
import util.Util;

/**
 * @author thsc
 */
public class SQL_OSMImporter extends DefaultHandler {
    public static final String TAGTABLE = "Tags";
    public static final String NODETABLE = "Nodes";
    public static final String WAYTABLE = "Ways";
    public static final String RELATIONTABLE = "Relations";
    public static final String RELATIONMEMBER = "RelationMember";
    public static final String CLASSIFICATIONTABLE = "Classification";
    public static final String WAYMEMBER = "WAYNODES";
    public static final String MAX_ID_SIZE = "10485760";
    
    private StringBuilder sAttributes;
    
    private StringBuilder nodeIDs;
    private StringBuilder memberIDs;
    
    private int nA = 0;
    private int wA = 0;
    private int rA = 0;
    
    private int all = 0;
    private int flushSteps = 100;
    
    private static final int STATUS_OUTSIDE = 0;
    private static final int STATUS_NODE = 1;
    private static final int STATUS_WAY = 2;
    private static final int STATUS_RELATION = 3;
    
    private int status = STATUS_OUTSIDE;
    
    private Connection targetConnection;
    private final Parameter parameter;
    private final Classification classification;
    private String user;
    private String pwd;
    private String serverName;
    private String portNumber;
    private String path;
    private String schema;
    private File recordFile;
    private int maxThreads;
    private final SQLStatementQueue insertQueue;
    private final SQLStatementQueue memberQueue;
    private final SQLStatementQueue updateNodesQueue;
    private final SQLStatementQueue updateWaysQueue;
    private final SQLStatementQueue indexQueue;
    private String currentElementID;

    public SQL_OSMImporter(Parameter parameter) throws Exception {
        this.parameter = parameter;
        this.classification = Classification.getInstance();
    
    try {
        this.user = this.parameter.getUserName();
        this.pwd = this.parameter.getPWD();
        this.serverName = this.parameter.getServerName();
        this.portNumber = this.parameter.getPortNumber();
        this.path = this.parameter.getdbName();
        this.schema = this.parameter.getSchema();
        this.recordFile = new File(this.parameter.getRecordFileName());
        try {
            String v = this.parameter.getMaxThread();
            this.maxThreads = Integer.parseInt(v.trim()) / 4; // there are four parallel queues
            this.maxThreads = this.maxThreads > 0 ? this.maxThreads : 1; // we have at least 4 threads
        }
        catch(NumberFormatException e) {
            System.err.println("no integer value (run single threaded instead): " + this.parameter.getMaxThread());
            this.maxThreads = 1;
        }
      
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
            } catch (SQLException e) {
            }
        }
    } catch (SQLException e) {
    }

    if (this.targetConnection == null) {
        System.err.println("cannot connect to database: reason unknown");
    }
    this.insertQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
    this.memberQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
    this.updateNodesQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
    this.updateWaysQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
    
    this.indexQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
    
    this.setupKB();
    }
    
    /*
    there are following different sql statements:
    node:
        insert into nodes (valid, osm_id, longitude, latitude, classcode, serializedtags) VALUES (..);
    
    way:
        insert into ways (valid, osm_id, classcode, serializedtags, node_ids) VALUES ();

        INSERT INTO WAYMEMBER (way_id, node_id) VALUES ();
        UPDATE nodes SET is_part=true WHERE id = id_nodes OR ...
    
    relation:
        insert into relations (valid, osm_id, classcode, serializedtags, member_ids) VALUES ();

        insert into relationmember (relation_id, role, [node_id | way_id | member_rel_id]) VALUES ();
        UPDATE nodes SET is_part=true WHERE osm_id = ?? OR osm_id = ??;
        UPDATE ways SET is_part=true WHERE osm_id = ?? OR osm_id = ??;
    */

    private boolean wayProcessed = false;
    private boolean relationProcessed = false;
    
    /**
     * each element has its attributes which are read after the start event
     * @param attributes 
     */
    private void newElement(Attributes attributes) {
        // initialize
        this.ndFound = false;
        this.wayFound = false;
        this.memberFound = false;
        this.currentClassID = -1;
        
        // reset attributes
        this.sAttributes = new StringBuilder();
        
        // serialize uid and user into this.sAttributes
        Util.serializeAttributes(this.sAttributes, "uid", attributes.getValue("uid"));
        Util.serializeAttributes(this.sAttributes, "user", attributes.getValue("user"));
        
        this.insertQueue.append("INSERT INTO ");
        switch(this.status) {
            case STATUS_NODE: 
                this.insertQueue.append(NODETABLE);
                this.insertQueue.append("(valid, longitude, latitude, osm_id, classcode, serializedtags) VALUES (true, ");
                this.insertQueue.append(attributes.getValue("lon"));
                this.insertQueue.append(", ");
                this.insertQueue.append(attributes.getValue("lat"));
                this.insertQueue.append(", ");
                break;
            case STATUS_WAY:
                if(this.nodeIDs == null || this.nodeIDs.length() > 0) {
                    this.nodeIDs = new StringBuilder();
                }
                if(!this.wayProcessed) {
                    // first way! create index in nodes
                    this.wayProcessed = true;
                }
                this.insertQueue.append(WAYTABLE);
                this.insertQueue.append("(valid, osm_id, classcode, serializedtags, node_ids) VALUES (true, ");
                
                this.memberQueue.append("INSERT INTO ");
                this.memberQueue.append(WAYMEMBER); 
                this.memberQueue.append(" (way_id, node_id) VALUES ");
                
                break;
            case STATUS_RELATION: 
                if(this.nodeIDs == null || this.nodeIDs.length() > 0) {
                    this.nodeIDs = new StringBuilder();
                }
                if(this.memberIDs == null || this.memberIDs.length() > 0) {
                    this.memberIDs = new StringBuilder();
                }
                
                if(!this.relationProcessed) {
                    // first relation! create index in ways
                    this.relationProcessed = true;
                }
                this.insertQueue.append(RELATIONTABLE);
                this.insertQueue.append("(valid, osm_id, classcode, serializedtags, member_ids) VALUES (true, ");

                break;
        }
        
        this.currentElementID = attributes.getValue("id");
        this.insertQueue.append(this.currentElementID);
        this.insertQueue.append(", ");
    }
    
    OSMClassification osmClassification = OSMClassification.getOSMClassification();

    private int currentClassID = -1;
    // just a set of new attributes.. add serialized to sAttrib builder
    private void addAttributesFromTag(Attributes attributes) {
        if(this.currentElementID.equalsIgnoreCase("28237510")) {
            int i = 42;
        }
        
        String key;
        
        int number = attributes.getLength();
        
        // they come as key value pairs        
        // extract key (k) value (v) pairs
        
        int i = 0;
        while(i < number) {
            // handle key: does it describe a osm class
            if(this.osmClassification.osmFeatureClasses.keySet().
                        contains(attributes.getValue(i))) {
                /* yes: next value is the subclass
                    value describes subclass
                */
                this.currentClassID = this.getOHDMClassID(
                      attributes.getValue(i), 
                      attributes.getValue(i+1)
                );
            } else {
                // its an ordinary key/value pair
                Util.serializeAttributes(this.sAttributes, 
                      attributes.getValue(i), 
                      attributes.getValue(i+1)
                );
            }
            i+=2;
        } 
    }

    boolean ndFound = false;
    private void addND(Attributes attributes) {
        // a new node reference like this: <nd ref='4406823158' />
        // only be found inside way
        
        /*
        add to node_ids builder
        add to member queue
        */
        if(!this.ndFound) {
            this.ndFound = true;
            // init update queue
            this.updateNodesQueue.append("UPDATE ");
            this.updateNodesQueue.append(NODETABLE);
            this.updateNodesQueue.append(" SET is_part=true WHERE ");
        } else {
            this.memberQueue.append(", ");
            this.updateNodesQueue.append(" OR ");
            
            this.nodeIDs.append(", ");
        }
        this.nodeIDs.append(attributes.getValue("ref"));
        
        this.memberQueue.append("(");
        this.memberQueue.append(this.currentElementID);
        this.memberQueue.append(", ");
        this.memberQueue.append(attributes.getValue("ref"));
        this.memberQueue.append(")");
        
        this.updateNodesQueue.append("osm_id = ");
        this.updateNodesQueue.append(attributes.getValue("ref"));
    }
    
    boolean wayFound = false;
    boolean memberFound = false;
    private void addMember(Attributes attributes) {
        //insert into relationmember (relation_id, role, [node_id | way_id | member_rel_id]) VALUES ();        
        // a new member like: <member type='way' ref='23084475' role='forward' />
        
        // remember id in member list first due to those to found-flags
        if(!attributes.getValue("type").equalsIgnoreCase("relation")) {
            if(this.ndFound || this.wayFound) {
                this.memberIDs.append(", ");                
            }
            this.memberIDs.append(attributes.getValue("ref")); 
        }
        
        this.memberQueue.append("INSERT INTO ");
        this.memberQueue.append(RELATIONMEMBER);
        this.memberQueue.append(" (relation_id, role, ");
        switch(attributes.getValue("type")) {
            case "node":
                this.memberQueue.append(" node_id) ");
                
                // update nodes
                if(!this.ndFound) {
                    this.ndFound = true;
                    // init update node queue
                    this.updateNodesQueue.append("UPDATE ");
                    this.updateNodesQueue.append(NODETABLE);
                    this.updateNodesQueue.append(" SET is_part=true WHERE ");
                } else {
                    this.updateNodesQueue.append(" OR ");
                }
                this.updateNodesQueue.append("osm_id = ");
                this.updateNodesQueue.append(attributes.getValue("ref"));
                break;
            case "way":
                this.memberQueue.append(" way_id) ");
                
                // update ways
                if(!this.wayFound) {
                    this.wayFound = true;
                    // init update way queue
                    this.updateWaysQueue.append("UPDATE ");
                    this.updateWaysQueue.append(WAYTABLE);
                    this.updateWaysQueue.append(" SET is_part=true WHERE ");
                } else {
                    this.updateWaysQueue.append(" OR ");
                }
                this.updateWaysQueue.append("osm_id = ");
                this.updateWaysQueue.append(attributes.getValue("ref"));
                break;
            case "relation":
                this.memberQueue.append(" member_rel_id) ");
                break;
        }
        
        // end member statement
        this.memberQueue.append(" VALUES ( ");
        this.memberQueue.append(this.currentElementID);
        this.memberQueue.append(", '");
        this.memberQueue.append(attributes.getValue("role"));
        this.memberQueue.append("', ");
        this.memberQueue.append(attributes.getValue("ref"));
        this.memberQueue.append("); ");
    }

    private AbstractElement dummyElement = new AbstractElement();
    
    private void endNode() {
        /*
        insert into nodes (osm_id, longitude, latitude, classcode, serializedtags, valid) VALUES (..);
        */
//        try {
            this.insertQueue.append(this.currentClassID);
            this.insertQueue.append(", '");
            this.insertQueue.append(this.sAttributes.toString());
            this.insertQueue.append("');");
//        } catch (SQLException ex) {
//            System.err.println("while saving node: " + ex.getMessage() + "\n" + this.insertQueue.toString());
//        } catch (IOException ex) {
//            System.err.println("while saving node: " + ex.getClass().getName() + "\n" + ex.getMessage());
//        }
    }

    private void endWay() {
        /*
        insert into ways (valid, osm_id, classcode, serializedtags, node_ids) VALUES ();

        INSERT INTO WAYMEMBER (way_id, node_id) VALUES ();
        UPDATE nodes SET is_part=true WHERE id = id_nodes OR ...
        */
        
        try {
            // TODO add remaining parameter; 
            this.insertQueue.append(this.currentClassID);
            this.insertQueue.append(", '");
            this.insertQueue.append(this.sAttributes.toString());
            this.insertQueue.append("', '");
            this.insertQueue.append(this.nodeIDs.toString());
            this.insertQueue.append("');");
//            this.insertQueue.forceExecute(this.currentElementID);
            
            this.memberQueue.append(";");
            this.memberQueue.forceExecute(this.currentElementID);
            
            this.updateNodesQueue.append(";");
            this.updateNodesQueue.forceExecute(this.currentElementID);
            
        } catch (SQLException ex) {
            System.err.println("while saving node: " + ex.getMessage() + "\n" + this.insertQueue.toString());
        } catch (IOException ex) {
            System.err.println("while saving node: " + ex.getClass().getName() + "\n" + ex.getMessage());
        }
        
    }

    private void endRelation() {
        /*
        insert into relations (valid, osm_id, classcode, serializedtags, member_ids) VALUES ();

        insert into relationmember (relation_id, role, [node_id | way_id | member_rel_id]) VALUES ();
        UPDATE nodes SET is_part=true WHERE osm_id = ?? OR osm_id = ??;
        UPDATE ways SET is_part=true WHERE osm_id = ?? OR osm_id = ??;
        */
        
        try {
            this.insertQueue.append(this.currentClassID);
            this.insertQueue.append(", '");
            this.insertQueue.append(this.sAttributes.toString());
            this.insertQueue.append("', '");
            this.insertQueue.append(this.memberIDs.toString());
            this.insertQueue.append("');");
//            this.insertQueue.forceExecute(this.currentElementID);
            
            this.memberQueue.append(";");
            this.memberQueue.forceExecute(this.currentElementID);
            
            this.updateNodesQueue.append(";");
            this.updateNodesQueue.forceExecute(this.currentElementID);
            
            this.updateWaysQueue.append(";");
            this.updateWaysQueue.forceExecute(this.currentElementID);
            
        } catch (SQLException ex) {
            System.err.println("while saving node: " + ex.getMessage() + "\n" + this.insertQueue.toString());
        } catch (IOException ex) {
            System.err.println("while saving node: " + ex.getClass().getName() + "\n" + ex.getMessage());
        }
    }

    @Override
    public void startDocument() {
        status = STATUS_OUTSIDE;
    }

    @Override
    public void endDocument() {
        System.out.println("\nOSM Import ended\n");
        this.printStatus();
    }

    @Override
    public void startElement(String uri, String localName, String qName, 
            Attributes attributes) {
        
        switch (qName) {
        case "node": {
            if (status != STATUS_OUTSIDE) {
                System.err.println("node found but not outside");
            }
            this.status = STATUS_NODE;
            this.newElement(attributes);
        }
        break; // single original node
        case "way": {
            if (status != STATUS_OUTSIDE) {
                System.err.println("way found but not outside");
            }
            this.status = STATUS_WAY;
            this.newElement(attributes);
        }
        break; // original way
        case "relation": {
            if (status != STATUS_OUTSIDE) {
                System.err.println("relation found but not outside");
            }
            this.status = STATUS_RELATION;
            this.newElement(attributes);
        }
        break; // original relation
        case "tag": {
            this.addAttributesFromTag(attributes);
        }
        break; // inside way
        case "nd": {
            this.addND(attributes);
        }
        break; // inside relation
        case "member": {
            this.addMember(attributes);
        }
        break; // inside a relation
        default:
    }
}
    
    @Override
    public void endElement(String uri, String localName, String qName) {
        try {
            switch (qName) {
                case "node":
                    // node finished - save
                    this.nA++;
                    this.endNode();
                    this.status = STATUS_OUTSIDE;
                    this.flush();
                    break; // single original node
                case "way":
                    if(!this.wayProcessed) {
                        // first way.. create an index on nodes
                        this.indexQueue.exec("create index node_osm_id on nodes (osm_id);");
                    }
                    this.wA++;
                    
//                    this.flushSteps = 1; // debugging
                    
                    this.endWay();
                    this.status = STATUS_OUTSIDE;
                    this.flush();
                    break; // original way

                case "relation":
                    if(!this.relationProcessed) {
                        // first relation.. create index on ways
                        this.indexQueue.exec("create index way_osm_id on ways (osm_id);");
                    }
                    this.rA++;
                    
//                    this.flushSteps = 1; // debugging
                    
                    this.endRelation();
                    this.status = STATUS_OUTSIDE;
                    this.flush();
                    break; // original relation
                case "tag":
                    break; // inside node, way or relation
                case "nd":
                    break; // inside a way or relation
                case "member":
                    break; // inside a relation
            }
        } catch (Exception eE) {
            System.err.println("while saving node: " + eE.getClass().getName() + "\n" + eE.getMessage());
            eE.printStackTrace(System.err);
        }
    }
    
    private int era = 0;
    private long startTime = System.currentTimeMillis();
    private void flush() {
        try {
            this.all++;
            if(this.flushSteps <= this.all) {
                this.all = 0;
                this.insertQueue.forceExecute(this.currentElementID);
                this.memberQueue.forceExecute(this.currentElementID);
                this.updateNodesQueue.forceExecute(this.currentElementID);
                this.updateWaysQueue.forceExecute(this.currentElementID);
                if(++this.era >= this.flushSteps) {
                    this.era = 0;
                    this.printStatus();
                } else {
                    System.out.print("*");
                }
            }
        } catch (SQLException sqlE) {
            System.err.println("while saving node: " + sqlE.getMessage() + "\n" + this.insertQueue.toString());
            sqlE.printStackTrace(System.err);
        } catch (IOException ioE) {
            System.err.println("while saving node: " + ioE.getClass().getName() + "\n" + ioE.getMessage());
            ioE.printStackTrace(System.err);
        } catch (Exception eE) {
            System.err.println("while saving node: " + eE.getClass().getName() + "\n" + eE.getMessage());
            eE.printStackTrace(System.err);
        }
    }
    
    private void printStatus() {
        System.out.print("\n");
        System.out.print("nodes: " + this.nA);
        System.out.print(" | ways: " + this.wA);
        System.out.print(" | relations: " + this.rA);
        System.out.print(" | entries per star: " + this.flushSteps);

        int days = 0;
        int hours = 0;
        int min = 0;
        long now = System.currentTimeMillis();
        long sec = (now - this.startTime) / 1000;

        if(sec >= 60) {
            min = (int) (sec / 60);
            sec = sec % 60;
        }
        if(min >= 60) {
            hours = min / 60;
            min = min % 60;
        }
        if(hours >= 24) {
            days = hours / 24;
            hours = hours % 24;
        }
        System.out.print(" | elapsed time:  ");
        System.out.print(days + " : ");
        System.out.print(hours + " : ");
        System.out.print(min + " : ");
        System.out.println(sec);
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

  private void setupTable(String table, String sqlCreate) throws SQLException {
    PreparedStatement stmt = null;
    StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
    sql.append(table).append(sqlCreate);
    try {
      stmt = targetConnection.prepareStatement(sql.toString());
      stmt.execute();
    } catch (SQLException ex) {
        System.err.println(ex.getClass().getName() + "\n" + ex.getMessage());
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
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
      } catch (SQLException e) {
      } finally {
        if (delStmt != null) {
          delStmt.close();
        }
      }
//    }
  }
  
  private void setupKB() {
    System.out.println("--- setting up tables ---");
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
//      sqlRelMember.append(" (relation_id bigint REFERENCES ").append(RELATIONTABLE).append(" (osm_id) NOT NULL, ")
      sqlRelMember.append(" (relation_id bigint NOT NULL, ")
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
  }
  
    private HashMap<String, Integer> classIDs = new HashMap<>();

    private String createFullClassName(String className, String subclassname) {
        return className + "_" + subclassname;
    }
  
    private void loadClassification() throws SQLException {      
//            Statement stmt = null;
//            try {
//                stmt = targetConnection.createStatement();
//                StringBuilder sb = new StringBuilder("SELECT * FROM ");
//                if (!this.schema.equalsIgnoreCase("")) {
//                    sb.append(schema).append(".");
//                }
//                sb.append(CLASSIFICATIONTABLE).append(";");
//                try (ResultSet rs = stmt.executeQuery(sb.toString())) {
//                    while (rs.next()) {
//                        this.classification.put(rs.getString("class"), rs.getString("subclassname"), rs.getInt("classcode"));
//                    }
//                }
//            } catch (SQLException e) {
//            } finally {
//                if (stmt != null) {
//                    try {
//                      stmt.close();
//                    } catch (SQLException e) {
//                    }
//                }
//            }
//        } else {
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
                    } finally {
                      if (stmt != null) {
                        stmt.close();
                      }
                    }
                }

            }
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
        
        return -1;
    }
}
