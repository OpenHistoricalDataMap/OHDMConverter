package osm2inter;

import util.InterDB;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;
import osm.OSMClassification;
import inter2ohdm.AbstractElement;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import util.DB;
import util.ManagedFileSQLStatementQueue;
import util.Parameter;
import util.SQLStatementQueue;
import util.Util;

/**
 * @author thsc
 */
public class FileSQL_OSMImporter extends DefaultHandler {
    private StringBuilder sAttributes;
    private StringBuilder nodeIDs;
    private StringBuilder memberIDs;
    
    private long nA = 0;
    private long wA = 0;
    private long rA = 0;
    
    private long all = 0;
    private int flushSteps = 10;
    
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
    private SQLStatementQueue memberQueue;
    
    private String currentElementID;

    private static final int LOG_STEPS = 100000;

    private long lastReconnect;
    private int era = 0;
    private final long startTime;
    private boolean hasName = false;
    private final PrintStream errStream;
    private final PrintStream outStream;
    private SQLStatementQueue managementQueue;
    private int admin_level;
    private int currentClassID = -1;
    private int boundaryAdminClassID = -1;
    
    public FileSQL_OSMImporter(Parameter parameter, OSMClassification osmClassification) throws Exception {
        this.parameter = parameter;
        this.osmClassification = osmClassification;
        this.boundaryAdminClassID = osmClassification.getOHDMClassID("boundary", "administrative");
    
        this.schema = parameter.getSchema();
        
        this.errStream = parameter.getErrStream();
        this.outStream = parameter.getOutStream();
        
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
      
        /*
        this.insertQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
        this.memberQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
        this.updateNodesQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
        this.updateWaysQueue = new SQLStatementQueue(this.targetConnection, this.recordFile, this.maxThreads);
        */
        
        this.managementQueue = new SQLStatementQueue(this.parameter, this.maxThreads);
        
        this.insertQueue = new ManagedFileSQLStatementQueue("sql_O2I_insertOSM2Inter", parameter);
//        this.insertQueue = new SQLStatementQueue(this.parameter, this.maxThreads);
        this.memberQueue = new ManagedFileSQLStatementQueue("sql_O2I_memberOSM2Inter", parameter);
//        this.memberQueue = new SQLStatementQueue(this.parameter, this.maxThreads);

        InterDB.createTables(managementQueue, schema);
        
        this.managementQueue.join();
        this.managementQueue.close();
        
        this.startTime = System.currentTimeMillis();
        this.lastReconnect = this.startTime;
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
        this.hasName = false;
        this.admin_level = 0;
        
        // could flush sql streams
        
        
        // reset attributes
        this.sAttributes = new StringBuilder();
        
        // serialize uid and user into this.sAttributes
        Util.serializeAttributes(this.sAttributes, "uid", attributes.getValue("uid"));
        Util.serializeAttributes(this.sAttributes, "user", attributes.getValue("user"));
        
        this.insertQueue.append("INSERT INTO ");
        switch(this.status) {
            case STATUS_NODE: 
                
                this.insertQueue.append(DB.getFullTableName(schema, InterDB.NODETABLE));
                this.insertQueue.append("(valid, longitude, latitude, osm_id, tstamp, classcode, serializedtags, has_name) VALUES (true, ");
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
                    // could to initial stuff here
                    this.wayProcessed = true;
                }
                this.insertQueue.append(DB.getFullTableName(schema, InterDB.WAYTABLE));
                this.insertQueue.append("(valid, osm_id, tstamp, classcode, serializedtags, has_name, node_ids) VALUES (true, ");
                
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
                
                if(this.currentElementID.equalsIgnoreCase("2343466")) {
                    int i = 42;
                }
                
                if(!this.relationProcessed) {
                    // could to initial stuff here
                    this.relationProcessed = true;
                }
                this.insertQueue.append(DB.getFullTableName(schema, InterDB.RELATIONTABLE));
                this.insertQueue.append("(valid, osm_id, tstamp, classcode, serializedtags, has_name, member_ids) VALUES (true, ");

                break;
        }
        
        this.currentElementID = attributes.getValue("id");
        this.insertQueue.append(this.currentElementID);
        this.insertQueue.append(", '");
        this.insertQueue.append(attributes.getValue("timestamp"));
        this.insertQueue.append("', ");
    }
    
    OSMClassification osmClassification = OSMClassification.getOSMClassification();

    // just a set of new attributes.. add serialized to sAttrib builder
    private void addAttributesFromTag(Attributes attributes) {
//        if(this.currentElementID.equalsIgnoreCase("28237510")) {
//            int i = 42;
//        }
        
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
                // describes an admin level
            } else if(attributes.getValue(i).equalsIgnoreCase("admin_level")) {
                try {
                    this.admin_level = Integer.parseInt(attributes.getValue(i+1));
                }
                catch(NumberFormatException nfe) {
                    this.errStream.println("not an integer in admin_level: " + attributes.getValue(i+1));
                }
            } else {
                // its an ordinary key/value pair
                Util.serializeAttributes(this.sAttributes, 
                      attributes.getValue(i), 
                      attributes.getValue(i+1)
                );
                
                // is it even a name
                if(attributes.getValue(i).equalsIgnoreCase("name")) {
                    this.hasName = true;
                }
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
//            this.updateNodesQueue.append("UPDATE ");
//            this.updateNodesQueue.append(DB.getFullTableName(schema, InterDB.NODETABLE));
//            this.updateNodesQueue.append(" SET is_part=true WHERE ");
        } else {
            this.memberQueue.append(", ");
//            this.updateNodesQueue.append(" OR ");
            
            this.nodeIDs.append(",");
        }
        this.nodeIDs.append(attributes.getValue("ref"));
        
        this.memberQueue.append("(");
        this.memberQueue.append(this.currentElementID);
        this.memberQueue.append(", ");
        this.memberQueue.append(attributes.getValue("ref"));
        this.memberQueue.append(")");
        
//        this.updateNodesQueue.append("osm_id = ");
//        this.updateNodesQueue.append(attributes.getValue("ref"));
    }
    
    boolean wayFound = false;
    boolean relationMemberFound = false;
    private void addMember(Attributes attributes) {
        if(this.currentElementID.equalsIgnoreCase("2343466")) {
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
//                    this.updateNodesQueue.append("UPDATE ");
//                    this.updateNodesQueue.append(DB.getFullTableName(schema, InterDB.NODETABLE));
//                    this.updateNodesQueue.append(" SET is_part=true WHERE ");
                } else {
//                    this.updateNodesQueue.append(" OR ");
                }
//                this.updateNodesQueue.append("osm_id = ");
//                this.updateNodesQueue.append(attributes.getValue("ref"));
                break;
            case "way":
                this.memberQueue.append(" way_id) ");
                
                // update ways
                if(!this.wayFound) {
                    this.wayFound = true;
                    // init update way queue
//                    this.updateWaysQueue.append("UPDATE ");
//                    this.updateWaysQueue.append(DB.getFullTableName(schema, InterDB.WAYTABLE));
//                    this.updateWaysQueue.append(" SET is_part=true WHERE ");
                } else {
//                    this.updateWaysQueue.append(" OR ");
                }
//                this.updateWaysQueue.append("osm_id = ");
//                this.updateWaysQueue.append(attributes.getValue("ref"));
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
        this.insertQueue.append("', ");
        this.insertQueue.append(Boolean.toString(this.hasName));
        this.insertQueue.append(");");
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
            this.insertQueue.append("', ");
            this.insertQueue.append(Boolean.toString(hasName));
            this.insertQueue.append(", '");
            this.insertQueue.append(this.nodeIDs.toString());
            this.insertQueue.append("');");

            // finish insert member statement
            this.memberQueue.append(";");
//            this.memberQueue.forceExecute(this.currentElementID);
            
            // finish update nodes statement
//            this.updateNodesQueue.append(";");
//            this.updateNodesQueue.forceExecute(this.currentElementID);
//        } catch (SQLException ex) {
//            this.errStream.println("while saving node: " + ex.getMessage() + "\n" + this.insertQueue.toString());
//        } catch (IOException ex) {
//            this.errStream.println("while saving node: " + ex.getClass().getName() + "\n" + ex.getMessage());
//        }
        
    }

    private void endRelation() {
        /*
        insert into relations (valid, osm_id, classcode, serializedtags, member_ids) VALUES ();

        insert into relationmember (relation_id, role, [node_id | way_id | member_rel_id]) VALUES ();
        UPDATE nodes SET is_part=true WHERE osm_id = ?? OR osm_id = ??;
        UPDATE ways SET is_part=true WHERE osm_id = ?? OR osm_id = ??;
        */
        
//        try {
            this.insertQueue.append(this.currentClassID);
            this.insertQueue.append(", '");
            this.insertQueue.append(this.sAttributes.toString());
            this.insertQueue.append("', ");
            this.insertQueue.append(Boolean.toString(hasName));
            this.insertQueue.append(", '");
            this.insertQueue.append(this.memberIDs.toString());
            this.insertQueue.append("');");
            
//            this.memberQueue.append(";");
//            this.memberQueue.forceExecute(this.currentElementID);
            
//            this.updateNodesQueue.append(";");
//            this.updateNodesQueue.forceExecute(this.currentElementID);
            
//            this.updateWaysQueue.append(";");
//            this.updateWaysQueue.forceExecute(this.currentElementID);
            
//        } catch (SQLException ex) {
//            this.errStream.println("while saving node: " + ex.getMessage() + "\n" + this.insertQueue.toString());
//        } catch (IOException ex) {
//            this.errStream.println("while saving node: " + ex.getClass().getName() + "\n" + ex.getMessage());
//        }
    }

    @Override
    public void startDocument() {
        status = STATUS_OUTSIDE;
        this.outStream.println("----------------------------------------------------------------");
        this.outStream.println("Start import from OSM file.. ");
        this.printStatus();
        this.outStream.println("----------------------------------------------------------------");
    }

    @Override
    public void endDocument() {
        this.outStream.print("----------------------------------------------------------------");
        this.outStream.print("\nRelation import ended.. wait for import threads to end..\n");
        this.printStatus();
        this.outStream.println("----------------------------------------------------------------");
        
        try {
//            this.outStream.println("last member queue sql query");
//            this.outStream.println(this.memberQueue);
            this.memberQueue.close(); // executes psql process
            this.insertQueue.close();
    
            // do the rest with jdbc
            this.managementQueue = new SQLStatementQueue(this.parameter, this.maxThreads);

            this.printStatus();
            this.outStream.println("create indexes...");

            this.managementQueue.append("CREATE INDEX node_osm_id ON ");
            this.managementQueue.append(DB.getFullTableName(this.schema, InterDB.NODETABLE));
            this.managementQueue.append(" (osm_id);");
            this.outStream.println("----------------------------------------------------------------");
            this.outStream.println("created index on nodes table over osm_id");
            this.printStatus();
            this.outStream.println("----------------------------------------------------------------");
            
            this.managementQueue.append("CREATE INDEX way_osm_id ON ");
            this.managementQueue.append(DB.getFullTableName(this.schema, InterDB.WAYTABLE));
            this.managementQueue.append(" (osm_id);");
            this.outStream.println("index on way table over osm_id");
            try {
                this.managementQueue.forceExecute(true);
            }
            catch(Exception e) {
                Util.printExceptionMessage(e, managementQueue, "exception when starting index creation on way table over osm_id");
            }

            this.managementQueue.append("CREATE INDEX waynodes_node_id ON ");
            this.managementQueue.append(DB.getFullTableName(this.schema, InterDB.WAYMEMBER));
            this.managementQueue.append(" (node_id);");
            this.outStream.println("index on waymember table over node_id");
            
            this.managementQueue.append("CREATE INDEX waynodes_way_id ON ");
            this.managementQueue.append(DB.getFullTableName(this.schema, InterDB.WAYMEMBER));
            this.managementQueue.append(" (way_id);");
            this.outStream.println("index on waymember table over way_id");
            try {
                this.managementQueue.forceExecute(true);
            }
            catch(Exception e) {
                Util.printExceptionMessage(e, managementQueue, "exception when starting index creation on waymember table over node_id");
            }
            
//            this.insertQueue.append("CREATE INDEX waynodes_way_id ON ");
//            this.insertQueue.append(DB.getFullTableName(this.schema, InterDB.WAYMEMBER));
//            this.insertQueue.append(" (way_id);");
//            this.outStream.println("index on waymember table over way_id");
//            try {
//                this.insertQueue.forceExecute(true);
//            }
//            catch(Exception e) {
//                Util.printExceptionMessage(e, insertQueue, "exception during index creation on waymember table over node_id");
//            }

            this.managementQueue.append("CREATE INDEX relation_osm_id ON ");
            this.managementQueue.append(DB.getFullTableName(this.schema, InterDB.RELATIONTABLE));
            this.managementQueue.append(" (osm_id);");
            this.outStream.println("index on relation table over osm_id");
            this.managementQueue.forceExecute(true);
            
            /*
            CREATE INDEX relationmember_member_rel_id ON 
            intermediate.relationmember (member_rel_id);
            */
            this.managementQueue.append("CREATE INDEX relationmember_member_rel_id ON ");
            this.managementQueue.append(DB.getFullTableName(this.schema, InterDB.RELATIONMEMBER));
            this.managementQueue.append(" (member_rel_id);");
            this.outStream.println("index on relation member table over member_rel_id");
            this.outStream.flush();
            this.managementQueue.forceExecute(true);
                        
            /*
            CREATE INDEX relationmember_node_id ON 
            intermediate.relationmember (node_id);            
            */
            this.managementQueue.append("CREATE INDEX relationmember_ids ON ");
            this.managementQueue.append(DB.getFullTableName(this.schema, InterDB.RELATIONMEMBER));
            this.managementQueue.append(" (relation_id, node_id, way_id, member_rel_id);");
            this.outStream.println("index on relation member table over node_id");
            this.outStream.flush();
            this.managementQueue.forceExecute(true);
            

            /*
            CREATE INDEX relationmember_way_id ON 
            intermediate.relationmember (node_id);            
            */
//            this.insertQueue.append("CREATE INDEX relationmember_way_id ON ");
//            this.insertQueue.append(DB.getFullTableName(this.schema, InterDB.RELATIONMEMBER));
//            this.insertQueue.append(" (way_id);");
//            this.outStream.println("index on relation member table over way_id");
//            this.outStream.flush();
//            this.insertQueue.forceExecute();
            
            this.managementQueue.close();
            
            this.outStream.println("index creation successfully");
            
            // wait for outstanding psql processes
            this.memberQueue.join();
            this.insertQueue.join();

            this.outStream.println("----------------------------------------------------------------");
            this.outStream.println("OSM import ended");
            this.printStatus();
            this.outStream.println("----------------------------------------------------------------");
        }
        catch(SQLException se) {
            Util.printExceptionMessage(se, this.managementQueue, "error while creating index");
        } catch (FileNotFoundException ex) {
            Util.printExceptionMessage(ex, this.managementQueue, "error while creating managementQueue");
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

    private void adjustClasscode() {
        /* 
        boundary / adminstrative / admin_level becomes 
        bondary / admin_level_[level]
        */
        if(this.boundaryAdminClassID == this.currentClassID) {
            if(this.admin_level > 0) { // adminlevel_1
                this.currentClassID = this.osmClassification.getOHDMClassID(
                            "boundary", 
                            "adminlevel_" + this.admin_level);
            }
        }
    }
    
    @Override
    public void endElement(String uri, String localName, String qName) {
        // maybe adjust something, like boundary / admin-level
        this.adjustClasscode();
        
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
            this.insertQueue.couldExecute();
            this.memberQueue.couldExecute();
            
            if(this.flushSteps <= this.all) {
                this.all = 0;
//                this.insertQueue.forceExecute(this.currentElementID);
//                this.memberQueue.forceExecute(this.currentElementID);
                
                if(++this.era >= LOG_STEPS / this.flushSteps) {
                    this.era = 0;
                    this.printStatus();
                } 
            }
        } catch (SQLException sqlE) {
            this.errStream.println("while saving element: " + sqlE.getMessage() + "\n" + this.insertQueue.toString());
            sqlE.printStackTrace(this.errStream);
        } catch (Throwable eE) {
            this.errStream.println("while saving element: " + eE.getClass().getName() + "\n" + eE.getMessage());
            eE.printStackTrace(this.errStream);
        }
    }
    
    private void printStatus() {
        this.outStream.print("nodes: " + Util.getValueWithDots(this.nA));
        this.outStream.print(" | ways: " + Util.getValueWithDots(this.wA));
        this.outStream.print(" | relations: " + Util.getValueWithDots(this.rA));
//        this.outStream.print(" | entries per star: " + this.flushSteps);

        this.outStream.print(" | elapsed time:  ");
        this.outStream.println(Util.getElapsedTime(this.startTime));
    }
}
