package osm2inter_v2;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;
import osm.OSMClassification;
import util.AbstractElement;
import util.DB;
import util.Parameter;
import util.SQLStatementQueue;
import util.Util;

/**
 * @author thsc
 */
public class SQL_OSMImporter extends DefaultHandler {
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
    private String currentElementID;

    public SQL_OSMImporter(Parameter parameter, OSMClassification osmClassification) throws Exception {
        this.parameter = parameter;
        this.osmClassification = osmClassification;
    
    try {
        this.targetConnection = DB.createConnection(parameter);
        this.schema = parameter.getSchema();
        
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
      
        } catch (SQLException e) {
        }

        if (this.targetConnection == null) {
            System.err.println("cannot connect to database: reason unknown");
        }
        this.insertQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
        this.memberQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
        this.updateNodesQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
        this.updateWaysQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);

        InterDB.createTables(insertQueue, schema);
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
        this.relationMemberFound = false;
        this.currentClassID = -1;
        
        // reset attributes
        this.sAttributes = new StringBuilder();
        
        // serialize uid and user into this.sAttributes
        Util.serializeAttributes(this.sAttributes, "uid", attributes.getValue("uid"));
        Util.serializeAttributes(this.sAttributes, "user", attributes.getValue("user"));
        
        this.insertQueue.append("INSERT INTO ");
        switch(this.status) {
            case STATUS_NODE: 
                
                this.insertQueue.append(DB.getFullTableName(schema, InterDB.NODETABLE));
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
                this.insertQueue.append(DB.getFullTableName(schema, InterDB.WAYTABLE));
                this.insertQueue.append("(valid, osm_id, classcode, serializedtags, node_ids) VALUES (true, ");
                
                this.memberQueue.append("INSERT INTO ");
                this.memberQueue.append(DB.getFullTableName(schema, InterDB.WAYMEMBER));
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
                this.insertQueue.append(DB.getFullTableName(schema, InterDB.RELATIONTABLE));
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
                this.currentClassID = this.osmClassification.getOHDMClassID(
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
            this.updateNodesQueue.append(DB.getFullTableName(schema, InterDB.NODETABLE));
            this.updateNodesQueue.append(" SET is_part=true WHERE ");
        } else {
            this.memberQueue.append(", ");
            this.updateNodesQueue.append(" OR ");
            
            this.nodeIDs.append(",");
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
    boolean relationMemberFound = false;
    private void addMember(Attributes attributes) {
        if(this.currentElementID.equalsIgnoreCase("14984")) {
            int i = 42;
        }
        
        //insert into relationmember (relation_id, role, [node_id | way_id | member_rel_id]) VALUES ();        
        // a new member like: <member type='way' ref='23084475' role='forward' />
        
        // remember id in member list first due to those to found-flags
//        if(!attributes.getValue("type").equalsIgnoreCase("relation")) {
            if(this.ndFound || this.wayFound || this.relationMemberFound) {
                this.memberIDs.append(",");                
            }
            this.memberIDs.append(attributes.getValue("ref")); 
//        }
        
        this.memberQueue.append("INSERT INTO ");
        this.memberQueue.append(DB.getFullTableName(schema, InterDB.RELATIONMEMBER));
        this.memberQueue.append(" (relation_id, role, ");
        switch(attributes.getValue("type")) {
            case "node":
                this.memberQueue.append(" node_id) ");
                
                // update nodes
                if(!this.ndFound) {
                    this.ndFound = true;
                    // init update node queue
                    this.updateNodesQueue.append("UPDATE ");
                    this.updateNodesQueue.append(DB.getFullTableName(schema, InterDB.NODETABLE));
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
                    this.updateWaysQueue.append(DB.getFullTableName(schema, InterDB.WAYTABLE));
                    this.updateWaysQueue.append(" SET is_part=true WHERE ");
                } else {
                    this.updateWaysQueue.append(" OR ");
                }
                this.updateWaysQueue.append("osm_id = ");
                this.updateWaysQueue.append(attributes.getValue("ref"));
                break;
            case "relation":
                this.relationMemberFound = true;
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
        this.insertQueue.append(this.currentClassID);
        this.insertQueue.append(", '");
        this.insertQueue.append(this.sAttributes.toString());
        this.insertQueue.append("');");
    }

    private void endWay() {
        /*
        insert into ways (valid, osm_id, classcode, serializedtags, node_ids) VALUES ();

        INSERT INTO WAYMEMBER (way_id, node_id) VALUES ();
        UPDATE nodes SET is_part=true WHERE id = id_nodes OR ...
        */
        
        try {
            // add remaining parameter; 
            this.insertQueue.append(this.currentClassID);
            this.insertQueue.append(", '");
            this.insertQueue.append(this.sAttributes.toString());
            this.insertQueue.append("', '");
            this.insertQueue.append(this.nodeIDs.toString());
            this.insertQueue.append("');");

            // finish insert member statement
            this.memberQueue.append(";");
            this.memberQueue.forceExecute(this.currentElementID);
            
            // finish update nodes statement
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
                        // join with all inserts
                        this.insertQueue.join();
                    }
                    this.wA++;
                    
//                    this.flushSteps = 1; // debugging
                    
                    this.endWay();
                    this.status = STATUS_OUTSIDE;
                    this.flush();
                    break; // original way

                case "relation":
                    if(!this.relationProcessed) {
                        // join with all inserts
                        this.insertQueue.join();
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
                if(++this.era >= 100) {
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
}
