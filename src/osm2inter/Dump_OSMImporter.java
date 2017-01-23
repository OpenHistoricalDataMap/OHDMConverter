package osm2inter;

import java.io.File;
import java.io.PrintStream;
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
public class Dump_OSMImporter extends DefaultHandler {
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
    private SQLStatementQueue insertQueue;
    private SQLStatementQueue memberQueue;
    private SQLStatementQueue updateNodesQueue;
    private SQLStatementQueue updateWaysQueue;
    private String currentElementID;

    private static final int LOG_STEPS = 100000;
    private static final long RECONNECTIONTIME = 1000*60*30; // 30 minutes in milliseconds
//    private static final long RECONNECTIONTIME = 1000; // 30 minutes in milliseconds
    private long lastReconnect;
    private int era = 0;
    private final long startTime;
    private final PrintStream logStream;
    private final PrintStream errStream;
    private final SQLStatementQueue sqlSetup;
    
    private int nodes_table_id = 0;
    private int ways_table_id = 0;
    private int waynodes_table_id = 0;
    private String uid;
    private String changeset;
    private String timestamp;
    
    public Dump_OSMImporter(Parameter parameter, OSMClassification osmClassification) throws Exception {
        this.parameter = parameter;
        this.osmClassification = osmClassification;
        
        this.logStream = parameter.getLogStream();
        this.errStream = parameter.getErrStream();
    
        this.schema = parameter.getSchema();
        
        this.recordFile = new File(this.parameter.getRecordFileName());
        try {
            String v = this.parameter.getMaxThread();
            this.maxThreads = Integer.parseInt(v.trim()) / 4; // there are four parallel queues
            this.maxThreads = this.maxThreads > 0 ? this.maxThreads : 1; // we have at least 4 threads
        }
        catch(NumberFormatException e) {
            this.errStream.println("no integer value (run single threaded instead): " + this.parameter.getMaxThread());
            this.maxThreads = 1;
        }
        
        // send statements to stdout
        this.insertQueue = new SQLStatementQueue(this.parameter, this.maxThreads, System.out, "insert");
        this.memberQueue = new SQLStatementQueue(this.parameter, this.maxThreads, null, "member");
        this.updateNodesQueue = new SQLStatementQueue(this.parameter, this.maxThreads, null, "update_nodes");
        this.updateWaysQueue = new SQLStatementQueue(this.parameter, this.maxThreads, null, "update_ways");

        this.sqlSetup = new SQLStatementQueue(this.parameter, this.maxThreads);
        InterDB.createTables(sqlSetup, schema);
        
        this.startTime = System.currentTimeMillis();
        this.lastReconnect = this.startTime;
        
    }
    
//    private void resetDBConnection() throws SQLException {
//        // wait for open statements
//        this.insertQueue.join();
//        this.memberQueue.join();
//        this.updateNodesQueue.join();
//        this.updateWaysQueue.join();
//
//        // close db connection
//        if(this.targetConnection != null) {
//            this.targetConnection.close();
//        }
//        
//        // reset connection
//        this.targetConnection = DB.createConnection(parameter);
//        
//        // re-establish queues
//        this.insertQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
//        this.memberQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
//        this.updateNodesQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
//        this.updateWaysQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
//    }
    
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
    
    private String lon; 
    private String lat; 
    
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
        
        /**
         * COPY nodes (id, osm_id, classcode, serializedtags, longitude, latitude, ohdm_geom_id, ohdm_object_id, is_part, new, changed, valid) FROM stdin;
62924	1693516194	-1	003uid00543566004user004anbr	13.5166148	52.4681357	\N	\N	f	\N	\N	t
         */
        switch(this.status) {
            case STATUS_NODE: 
                this.insertQueue.append(this.nodes_table_id++);
                this.insertQueue.append("\t");
                
                this.lon= attributes.getValue("lon");
                this.lat= attributes.getValue("lat");
                
                // for updater!
                this.timestamp = attributes.getValue("timestamp");
                        
                break;
                
            case STATUS_WAY:
                
                if(this.nodeIDs == null || this.nodeIDs.length() > 0) {
                    this.nodeIDs = new StringBuilder();
                }
                if(!this.wayProcessed) {
                    // first way! create index in nodes
                    this.wayProcessed = true;
                    
                    /* create an index on nodes
                     CREATE INDEX node_osm_id ON intermediate.nodes (osm_id);
                    */
                    this.sqlSetup.append("CREATE INDEX node_osm_id ON ");
                    this.sqlSetup.append(DB.getFullTableName(this.schema, InterDB.NODETABLE));
                    this.sqlSetup.append(" (osm_id);");
                    
                    this.logStream.println("----------------------------------------------------------------");
                    this.logStream.println("End of nodes import.. create index on nodes table over osm_id");
                    this.printStatus();
                    this.logStream.println("----------------------------------------------------------------");
                    try {
                        this.sqlSetup.forceExecute(); // exec and wait.. no new thread
                        this.logStream.println("index on node tables created.. go ahead with ways import");
                        this.printStatus();
                    }
                    catch(SQLException se) {
                        Util.printExceptionMessage(se, sqlSetup, "error while creating index on nodes");
                    }
                }
                this.insertQueue.append(DB.getFullTableName(schema, InterDB.WAYTABLE));
                this.insertQueue.append("(valid, osm_id, classcode, serializedtags, node_ids) VALUES (true, ");
                
                this.memberQueue.append("INSERT INTO ");
                this.memberQueue.append(DB.getFullTableName(schema, InterDB.WAYMEMBER));
                this.memberQueue.append(" (way_id, node_id) VALUES ");
                
                break;
                
            case STATUS_RELATION: 
                
                // do we need nodeIDs in a relation - dont think so.. TODO
//                if(this.nodeIDs == null || this.nodeIDs.length() > 0) {
//                    this.nodeIDs = new StringBuilder();
//                }
                // member ids required
                if(this.memberIDs == null || this.memberIDs.length() > 0) {
                    this.memberIDs = new StringBuilder();
                }
                
                if(!this.relationProcessed) {
                    // first relation! create index in ways
                    /*
                    on ways:
                    CREATE INDEX way_osm_id ON intermediate.ways (osm_id);

                    on waynodes:
                    CREATE INDEX waynodes_node_id ON intermediate.waynodes (node_id);
                    CREATE INDEX waynodes_way_id ON intermediate.waynodes (way_id);
                    */
                    this.logStream.println("----------------------------------------------------------------");
                    this.logStream.println("End of ways import.. create indexes");
                    this.printStatus();
                    this.logStream.println("----------------------------------------------------------------");
                    this.sqlSetup.append("CREATE INDEX way_osm_id ON ");
                    this.sqlSetup.append(DB.getFullTableName(this.schema, InterDB.WAYTABLE));
                    this.sqlSetup.append(" (osm_id);");
                    this.logStream.println("index on way table over osm_id");
                    try {
                        this.sqlSetup.forceExecute(true);
                    }
                    catch(Exception e) {
                        Util.printExceptionMessage(e, sqlSetup, "exception when starting index creation on way table over osm_id");
                    }

                    this.sqlSetup.append("CREATE INDEX waynodes_node_id ON ");
                    this.sqlSetup.append(DB.getFullTableName(this.schema, InterDB.WAYMEMBER));
                    this.sqlSetup.append(" (node_id);");
                    this.logStream.println("index on waymember table over node_id");
                    try {
                        this.sqlSetup.forceExecute(true);
                    }
                    catch(Exception e) {
                        Util.printExceptionMessage(e, sqlSetup, "exception when starting index creation on waymember table over node_id");
                    }

                    this.sqlSetup.append("CREATE INDEX waynodes_way_id ON ");
                    this.sqlSetup.append(DB.getFullTableName(this.schema, InterDB.WAYMEMBER));
                    this.sqlSetup.append(" (way_id);");
                    this.logStream.println("index on waymember table over way_id");
                    try {
                        this.sqlSetup.forceExecute();
                    }
                    catch(Exception e) {
                        Util.printExceptionMessage(e, sqlSetup, "exception during index creation on waymember table over node_id");
                    }

//                    this.sqlSetup.join();
                    this.logStream.println("indexes creation on way tables is on its way.. go ahead with relations import");
                    this.printStatus();
                    
                    this.relationProcessed = true;
                }
                
                this.insertQueue.append(DB.getFullTableName(schema, InterDB.RELATIONTABLE));
                this.insertQueue.append("(valid, osm_id, classcode, serializedtags, member_ids) VALUES (true, ");

                break;
        }
        
        this.currentElementID = attributes.getValue("id");
        this.insertQueue.append(this.currentElementID);
        this.insertQueue.append("\t");
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
         * COPY nodes (id, osm_id, classcode, serializedtags, longitude, latitude, ohdm_geom_id, ohdm_object_id, is_part, new, changed, valid) FROM stdin;
62924	1693516194	-1	003uid00543566004user004anbr	13.5166148	52.4681357	\N	\N	f	\N	\N	t
        */
        this.insertQueue.append(this.currentClassID);
        this.insertQueue.append("\t");
        this.insertQueue.append(this.sAttributes.toString());
        this.insertQueue.append("\t");
        this.insertQueue.append(this.lon);
        this.insertQueue.append("\t");
        this.insertQueue.append(this.lat);
        this.insertQueue.append("\t\\N\t\\N\tf\t\\N\t\\N\tt\r\n");
    }

    private void endWay() {
        /*
        insert into ways (valid, osm_id, classcode, serializedtags, node_ids) VALUES ();

        INSERT INTO WAYMEMBER (way_id, node_id) VALUES ();
        UPDATE nodes SET is_part=true WHERE id = id_nodes OR ...
        */
        
//        try {
            // add remaining parameter; 
            this.insertQueue.append(this.currentClassID);
            this.insertQueue.append(", '");
            this.insertQueue.append(this.sAttributes.toString());
            this.insertQueue.append("', '");
            this.insertQueue.append(this.nodeIDs.toString());
            this.insertQueue.append("');");

            // finish insert member statement
            this.memberQueue.append(";");
//            this.memberQueue.forceExecute(this.currentElementID);
            
            // finish update nodes statement
            this.updateNodesQueue.append(";");
        
    }

    private void endRelation() {
        /*
        insert into relations (valid, osm_id, classcode, serializedtags, member_ids) VALUES ();

        insert into relationmember (relation_id, role, [node_id | way_id | member_rel_id]) VALUES ();
        UPDATE nodes SET is_part=true WHERE osm_id = ?? OR osm_id = ??;
        UPDATE ways SET is_part=true WHERE osm_id = ?? OR osm_id = ??;
        */
        
            this.insertQueue.append(this.currentClassID);
            this.insertQueue.append(", '");
            this.insertQueue.append(this.sAttributes.toString());
            this.insertQueue.append("', '");
            this.insertQueue.append(this.memberIDs.toString());
            this.insertQueue.append("');");
            
            this.memberQueue.append(";");
//            this.memberQueue.forceExecute(this.currentElementID);
            
            this.updateNodesQueue.append(";");
//            this.updateNodesQueue.forceExecute(this.currentElementID);
            
            this.updateWaysQueue.append(";");
//            this.updateWaysQueue.forceExecute(this.currentElementID);
            
    }

    @Override
    public void startDocument() {
        status = STATUS_OUTSIDE;
        this.logStream.println("----------------------------------------------------------------");
        this.logStream.println("Start import from OSM file.. ");
        this.printStatus();
        this.logStream.println("----------------------------------------------------------------");
    }

    @Override
    public void endDocument() {
        this.logStream.print("----------------------------------------------------------------");
        this.logStream.print("\nRelation import ended.. wait for import threads to end..\n");
        this.printStatus();
        this.logStream.println("----------------------------------------------------------------");

        try {
            insertQueue.close();
            memberQueue.close();
            updateNodesQueue.close();
            
            this.printStatus();
            this.logStream.println("create indexes on relation tables");
        
            this.updateWaysQueue.append("CREATE INDEX relation_osm_id ON ");
            this.updateWaysQueue.append(DB.getFullTableName(this.schema, InterDB.RELATIONTABLE));
            this.updateWaysQueue.append(" (osm_id);");
            this.logStream.println("index on relation table over osm_id");
            this.updateWaysQueue.forceExecute(true);

            /*
            CREATE INDEX relationmember_member_rel_id ON 
            intermediate.relationmember (member_rel_id);
            */
            this.updateWaysQueue.append("CREATE INDEX relationmember_member_rel_id ON ");
            this.updateWaysQueue.append(DB.getFullTableName(this.schema, InterDB.RELATIONMEMBER));
            this.updateWaysQueue.append(" (member_rel_id);");
            this.logStream.println("index on relation member table over member_rel_id");
            this.logStream.flush();
            this.updateWaysQueue.forceExecute(true);
                        
            /*
            CREATE INDEX relationmember_node_id ON 
            intermediate.relationmember (node_id);            
            */
            this.updateWaysQueue.append("CREATE INDEX relationmember_node_id ON ");
            this.updateWaysQueue.append(DB.getFullTableName(this.schema, InterDB.RELATIONMEMBER));
            this.updateWaysQueue.append(" (node_id);");
            this.logStream.println("index on relation member table over node_id");
            this.logStream.flush();
            this.updateWaysQueue.forceExecute(true);

            /*
            CREATE INDEX relationmember_way_id ON 
            intermediate.relationmember (node_id);            
            */
            this.updateWaysQueue.append("CREATE INDEX relationmember_way_id ON ");
            this.updateWaysQueue.append(DB.getFullTableName(this.schema, InterDB.RELATIONMEMBER));
            this.updateWaysQueue.append(" (way_id);");
            this.logStream.println("index on relation member table over way_id");
            this.logStream.flush();
            this.updateWaysQueue.forceExecute();
            
            this.updateWaysQueue.close();
            this.logStream.println("index creation successfully");
            this.logStream.println("----------------------------------------------------------------");
            this.logStream.println("OSM import ended");
            this.printStatus();
            this.logStream.println("----------------------------------------------------------------");
        }
        catch(SQLException se) {
            Util.printExceptionMessage(se, this.updateWaysQueue, "error while creating index");
        }
    }
    
    @Override
    public void startElement(String uri, String localName, String qName, 
            Attributes attributes) {
        
        switch (qName) {
        case "node": {
            if (status != STATUS_OUTSIDE) {
                this.errStream.println("node found but not outside");
            }
            this.status = STATUS_NODE;
            this.newElement(attributes);
        }
        break; // single original node
        case "way": {
            if (status != STATUS_OUTSIDE) {
                this.errStream.println("way found but not outside");
            }
            this.status = STATUS_WAY;
            this.newElement(attributes);
        }
        break; // original way
        case "relation": {
            if (status != STATUS_OUTSIDE) {
                this.errStream.println("relation found but not outside");
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
            this.errStream.println("while saving element: " + eE.getClass().getName() + "\n" + eE.getMessage());
            eE.printStackTrace(this.errStream);
        }
    }
    
    private void flush() {
        try {
            this.all++;
            if(this.flushSteps <= this.all) {
                this.all = 0;
                this.insertQueue.forceExecute(this.currentElementID);
                this.memberQueue.forceExecute(this.currentElementID);
                this.updateNodesQueue.forceExecute(this.currentElementID);
                this.updateWaysQueue.forceExecute(this.currentElementID);
                if(++this.era >= LOG_STEPS / this.flushSteps) {
                    this.era = 0;
                    this.printStatus();
                
                } 
            }
        } catch (Throwable eE) {
            this.errStream.println("while saving element: " + eE.getClass().getName() + "\n" + eE.getMessage());
            eE.printStackTrace(this.errStream);
        }
    }
    
    private void printStatus() {
        this.logStream.print("nodes: " + Util.getIntWithDots(this.nA));
        this.logStream.print(" | ways: " + Util.getIntWithDots(this.wA));
        this.logStream.print(" | relations: " + Util.getIntWithDots(this.rA));

        this.logStream.print(" | elapsed time:  ");
        this.logStream.println(Util.getElapsedTime(this.startTime));
    }
}
