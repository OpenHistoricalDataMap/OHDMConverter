package inter2ohdm;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import util.InterDB;
import static util.InterDB.NODETABLE;
import static util.InterDB.RELATIONMEMBER;
import static util.InterDB.RELATIONTABLE;
import static util.InterDB.WAYTABLE;
import util.DB;
import static util.InterDB.WAYMEMBER;
import util.OHDM_DB;
import util.SQLStatementQueue;
import util.TriggerRecipient;
import util.Util;

/**
 *
 * @author thsc
 */
public class OSMExtractor extends IntermediateDB implements TriggerRecipient {
    private final Importer importer;
    
    private final String schema;
    
    private int printEra = 0;
    private final static int PRINT_ERA_LENGTH = 100000;
    private static final int DEFAULT_STEP_LEN = 1000;
    
    // for statistics
    private long number;
    private String nodesTableEntries = "?";
    private String waysTableEntries = "?";
    private String relationsTableEntries = "?";
    private long numberCheckedNodes = 0;
    private long numberCheckedWays = 0;
    private long numberCheckedRelations = 0;
    private long numberImportedNodes = 0;
    private long numberImportedWays = 0;
    private long numberImportedRelations = 0;
    private long historicInfos = 0;
    private final long startTime;
    
    static final int NODE = 0;
    static final int WAY = 1;
    static final int RELATION = 2;
    private int steplen;
    private String upperIDString;
    private String lowerIDString;
    
    OSMExtractor(Connection sourceConnection, String schema, Importer importer, int steplen) {
        super(sourceConnection, schema);
        
        this.startTime = System.currentTimeMillis();
        this.number = 0;
        
        this.schema = schema;
        this.importer = importer;
        
        this.steplen = steplen;
        
        if(this.steplen < 1) {
            this.steplen = DEFAULT_STEP_LEN;
        }
        
        this.steps = new BigDecimal(this.steplen);
    }

    private BigDecimal initialLowerID;
    private BigDecimal initialUpperID;
    private BigDecimal initialMaxID;
    private final BigDecimal steps;
    
    private String calculateInitialIDs(SQLStatementQueue sql, String tableName) {
        // first: figure out min and max osm_id in nodes table
        
        String resultString = "unknown";
        
        try {
            sql.append("SELECT min(id), max(id) FROM ");
            sql.append(DB.getFullTableName(this.schema, tableName));
            sql.append(";");

            ResultSet result = sql.executeWithResult();
            result.next();
            
            BigDecimal minID = result.getBigDecimal(1);
            if(minID == null) {
                this.initialMaxID = null;

                this.initialLowerID = null;
                this.initialUpperID = null;
                throw new SQLException("table is empty: " + DB.getFullTableName(this.schema, tableName));
            }
/*
            sql.append("SELECT max(id) FROM ");
            sql.append(DB.getFullTableName(this.schema, tableName));
            sql.append(";");

            result = sql.executeWithResult();
            result.next();
*/            
            this.initialMaxID = result.getBigDecimal(2);

            this.initialLowerID = minID.subtract(new BigDecimal(1));
            this.initialUpperID = minID.add(this.steps);
            
            resultString = initialMaxID.toPlainString();
        }
        catch(SQLException se) {
            Util.printExceptionMessage(se, sql, "when calculating initial min max ids for select of nodes, ways or relations", true);
        }
        
        return resultString;
    }
    
    void processNode(ResultSet qResult, SQLStatementQueue sql, boolean importUnnamedEntities) {
        OSMNode node = null;
        try {
            node = this.createOHDMNode(qResult);
            this.currentElement = node;
            
            if(node.getOSMIDString().equalsIgnoreCase("20246240")) {
                int i = 42;
            }
            
            this.numberCheckedNodes++;

            if(node.isConsistent(System.err)) {
                // now process that stuff
                if(this.importer.importNode(node, importUnnamedEntities)) {
                    this.numberImportedNodes++;
                }

                if(this.importer.importPostProcessing(node, importUnnamedEntities)) {
                    this.historicInfos++;
                }
            } else {
                this.printError(System.err, "node not consistent:\n" + node);
            }
        }
        catch(SQLException se) {
            System.err.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            System.err.println("exception when handling node osm_id: " + node.getOSMIDString());
            Util.printExceptionMessage(se, sql, "failure when processing node.. non fatal", true);
            System.err.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        }
    }
    
    void printError(PrintStream p, String s) {
        p.println(s);
        p.println("-------------------------------------");
    }
    
    void processWay(ResultSet qResult, SQLStatementQueue sql, boolean importUnnamedEntities) {
        OSMWay way = null;
        try {
            way = this.createOHDMWay(qResult);
            this.currentElement = way;
            
            if(way.getOSMIDString().equalsIgnoreCase("4557344")) {
                int i = 42;
            }

//            if(!way.isPart() && way.getName() == null) notPartNumber++;

            this.addNodes2OHDMWay(way);
            
            this.numberCheckedWays++;

            if(way.isConsistent(System.err)) {
                // process that stuff
                if(this.importer.importWay(way, importUnnamedEntities)) {
                    this.numberImportedWays++;
                }

                if(this.importer.importPostProcessing(way, importUnnamedEntities)) {
                    this.historicInfos++;
                }
            } else {
                this.printError(System.err, "way not consistent:\n" + way);
            }
        }
        catch(SQLException se) {
            System.err.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            System.err.println("exception when processing way: " + way);
            Util.printExceptionMessage(se, sql, "failure when processing way.. non fatal", true);
            System.err.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        }
    }
    
    void processRelation(ResultSet qResult, SQLStatementQueue sql, boolean importUnnamedEntities) {
        OSMRelation relation = null;
        
        try {
            relation = this.createOHDMRelation(qResult);
            
            String r_id = relation.getOSMIDString();
            if(r_id.equalsIgnoreCase("1611842")) {
                int i = 42;
            }

            this.currentElement = relation;

            // find all associated nodes and add to that relation
            sql.append("select * from ");
            sql.append(DB.getFullTableName(this.schema, RELATIONMEMBER));
            sql.append(" where relation_id = ");            
            sql.append(relation.getOSMIDString());
            sql.append(";");  

            ResultSet qResultRelation = sql.executeWithResult();

            boolean relationMemberComplete = true; // assume we find all member

            while(qResultRelation.next()) {
                String roleString =  qResultRelation.getString("role");

                // extract member objects from their tables
                BigDecimal id;
                int type = -1;

                sql.append("SELECT * FROM ");

                id = qResultRelation.getBigDecimal("node_id");
                if(id != null) {
                    sql.append(DB.getFullTableName(this.schema, NODETABLE));
                    type = OHDM_DB.POINT;
                } else {
                    id = qResultRelation.getBigDecimal("way_id");
                    if(id != null) {
                        sql.append(DB.getFullTableName(this.schema, WAYTABLE));
                        type = OHDM_DB.LINESTRING;
                    } else {
                        id = qResultRelation.getBigDecimal("member_rel_id");
                        if(id != null) {
                            sql.append(DB.getFullTableName(this.schema, RELATIONTABLE));
                            type = OHDM_DB.RELATION;
                        } else {
                            // we have a serious problem here.. or no member
                        }
                    }
                }
                sql.append(" where osm_id = ");
                sql.append(id.toString());
                sql.append(";");

                // debug stop
                if(id.toString().equalsIgnoreCase("245960580")) {
                    int i = 42;
                }

                ResultSet memberResult = sql.executeWithResult();
                if(memberResult.next()) {
                    // this call can fail, see else branch
                    OSMElement memberElement = null;
                    switch(type) {
                        case OHDM_DB.POINT: 
                            memberElement = this.createOHDMNode(memberResult);
                            break;
                        case OHDM_DB.LINESTRING:
                            memberElement = this.createOHDMWay(memberResult);
                            break;
                        case OHDM_DB.RELATION:
                            memberElement = this.createOHDMRelation(memberResult);
                            break;
                    }
                    
                    relation.addMember(memberElement, roleString);
                } else {
                    /* this call can fail
                    a) if this program is buggy - which is most likely :) OR
                    b) intermediate DB has not imported whole world. In that
                    case, relation can refer to data which are not actually 
                    stored in intermediate db tables.. 
                    in that case .. remove whole relation: parts of it are 
                    outside our current scope
                    */
//                            System.out.println("would removed relation: " + relation.getOSMIDString());
//                    debug_alreadyPrinted = true;
                    //relation.remove();
                    relationMemberComplete = false; 
                }
                memberResult.close();

                if(!relationMemberComplete) break;
            }
            
            this.numberCheckedRelations++;
            
            if(relation.isConsistent(System.err)) {
                // process that stuff
                if(relationMemberComplete && this.importer.importRelation(relation, importUnnamedEntities)) {
                    this.numberImportedRelations++;

                    if(this.importer.importPostProcessing(relation, importUnnamedEntities)) {
                        this.historicInfos++;
                    }

                } 
            } else {
                this.printError(System.err, "inconsistent relation\n" + relation);
            }
        }
        catch(SQLException se) {
            System.err.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            System.err.println("relation osm_id: " + relation.getOSMIDString());
            Util.printExceptionMessage(se, sql, "failure when processing relation.. non fatal", true);
            System.err.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        }
    }
    
    void processNodes(SQLStatementQueue sql, boolean namedEntitiesOnly) {
        this.processElements(sql, NODE, namedEntitiesOnly);
    }
    
    void processWays(SQLStatementQueue sql, boolean namedEntitiesOnly) {
        this.processElements(sql, WAY, namedEntitiesOnly);
    }
    
    void processRelations(SQLStatementQueue sql, boolean namedEntitiesOnly) {
        this.processElements(sql, RELATION, namedEntitiesOnly);
    }
    
    void processElements(SQLStatementQueue sql, int elementType, boolean namedEntitiesOnly) {
        String elementTableName = null;
        switch(elementType) {
            case NODE:
                elementTableName = InterDB.NODETABLE;
                break;
            case WAY:
                elementTableName = InterDB.WAYTABLE;
                break;
            case RELATION:
                elementTableName = InterDB.RELATIONTABLE;
                break;
        }
        
        String maxIDString = this.calculateInitialIDs(sql, elementTableName);
        if(this.initialMaxID == null) {
            return;
        }
        
        switch(elementType) {
            case NODE:
                this.nodesTableEntries = maxIDString;
                break;
            case WAY:
                this.waysTableEntries = maxIDString;
                break;
            case RELATION:
                this.relationsTableEntries = maxIDString;
                break;
        }
        
        // first: figure out min and max osm_id in nodes table
        BigDecimal lowerID = this.initialLowerID;
        BigDecimal upperID = this.initialUpperID;
        BigDecimal maxID = this.initialMaxID;
            
        this.upperIDString = Util.setDotsInStringValue(upperID.toPlainString());
        this.lowerIDString = Util.setDotsInStringValue(lowerID.toPlainString());
        
        System.out.println("Start processing entites");
        System.out.println(this.getStatistics());
        
        try {
            this.printStarted(elementTableName);
            this.era = 0; // start new element type - reset for statistics
            do {
                long before = System.currentTimeMillis();
                sql.append("SELECT * FROM ");
                sql.append(DB.getFullTableName(this.schema, elementTableName));
                sql.append(" where id <= "); // including upper
                sql.append(upperID.toString());
                sql.append(" AND id > "); // excluding lower 
                sql.append(lowerID.toString());
                sql.append(" AND classcode != -1 "); // excluding untyped entities 
                if(namedEntitiesOnly) {
                    sql.append(" AND serializedtags like '%004name%'"); // entities with a name
                }
                sql.append(";");
                ResultSet qResult = sql.executeWithResult();
                long after = System.currentTimeMillis();
                this.noteTime(after-before, TIME_SELECT_ELEMENTS);
                
                while(qResult.next()) {
                    this.number++;
                    this.printStatistics();
                    before = System.currentTimeMillis();
                    this.processElement(qResult, sql, elementType, namedEntitiesOnly);
                    after = System.currentTimeMillis();
                    this.noteTime(after-before, TIME_PROCESS_ELEMENTS);
                }
                
                // next bulk of data
                lowerID = upperID;
                upperID = upperID.add(steps);
                
                if(upperID.compareTo(initialMaxID) == 1 && lowerID.compareTo(initialMaxID) == -1) {
                    upperID = initialMaxID; // last round
                }
                
                this.upperIDString = Util.setDotsInStringValue(upperID.toPlainString());
                this.lowerIDString = Util.setDotsInStringValue(lowerID.toPlainString());
                
            } while(!(upperID.compareTo(initialMaxID) == 1));
        } 
        catch (SQLException ex) {
            // fatal exception.. do not continue
            Util.printExceptionMessage(ex, sql, "when selecting nodes/ways/relation", false);
        }
        this.printFinished(elementTableName);
    }
        
    private void printExceptionMessage(Exception ex, SQLStatementQueue sql, OSMElement element) {
        if(element != null) {
            System.err.print("inter2ohdm: exception when processing ");
            if(element instanceof OSMNode) {
                System.err.print("node ");
            }
            else if(element instanceof OSMWay) {
                System.err.print("way ");
            }
            else {
                System.err.print("relation ");
            }
            System.err.print("with osm_id = ");
            System.err.println(element.getOSMIDString());
        }
        System.err.println("inter2ohdm: sql request: " + sql.toString());
        System.err.println(ex.getLocalizedMessage());
        ex.printStackTrace(System.err);
    }
    
    @Override
    OSMWay addNodes2OHDMWay(OSMWay way) throws SQLException {
        // find all associated nodes and add to that way
        /* SQL Query is like this
            select * from nodes_table where osm_id IN 
            (SELECT node_id FROM waynodes_table where way_id = ID_of_way);            
        */ 

        long before = System.currentTimeMillis();
        
//        Iterator<String> nodeIDIter = way.getNodeIDs();
//        if(nodeIDIter != null) {
//            // add actual nodes to that way
//            SQLStatementQueue sql = new SQLStatementQueue(this.sourceConnection);
//            
//            sql.append("select * from ");
//            sql.append(DB.getFullTableName(this.schema, NODETABLE));
//            sql.append(" where osm_id in (");
//            
//            boolean first = true;
//            while(nodeIDIter.hasNext()) {
//                String nodeID = nodeIDIter.next();
//                
//                if(first) {
//                    first = false;
//                } else {
//                    sql.append(", ");
//                }
//                
//                sql.append(nodeID);
//            }
//            sql.append(");");  
//        
//            ResultSet qResultNode = sql.executeWithResult();
//
//            while(qResultNode.next()) {
//                OSMNode node = this.createOHDMNode(qResultNode);
//                way.addNode(node);
//            }
//            
//            qResultNode.close();
//        }
        
        // believe it or not but that's faster with index on waynodemember
        // I don't believe it's true for huge data
        SQLStatementQueue sql = new SQLStatementQueue(this.sourceConnection);

        sql.append("select * from ");
        sql.append(DB.getFullTableName(this.schema, NODETABLE));
        sql.append(" where osm_id IN (SELECT node_id FROM ");            
        sql.append(DB.getFullTableName(this.schema, WAYMEMBER));
        sql.append(" where way_id = ");            
        sql.append(way.getOSMIDString());
        sql.append(");");  

        ResultSet qResultNode = sql.executeWithResult();

        while(qResultNode.next()) {
            long beforeNode = System.currentTimeMillis();
            OSMNode node = this.createOHDMNode(qResultNode);
            long afterNode = System.currentTimeMillis();
            this.noteTime(afterNode-beforeNode, TIME_CREATE_NODE);
            way.addNode(node);
        }

        qResultNode.close();
        long after = System.currentTimeMillis();
        this.noteTime(after-before, TIME_ADD_NODES);
                
        return way;
    }
    
    private final String progressSign = "*";
    private int progresslineCount = 0;
    private long era = 0;

    private void printStarted(String what) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.print("Start importing ");
        System.out.println(what);
        System.out.println(this.getStatistics());
        System.out.println("--------------------------------------------------------------------------------");
    }
    
    private void printFinished(String what) {
        System.out.println("\n--------------------------------------------------------------------------------");
        System.out.print("Finished importing ");
        System.out.println(what);
        System.out.println(this.getStatistics());
        System.out.println("--------------------------------------------------------------------------------");
    }
    
    private long lastCheckedEntities = 0;
    private long lastCheckTime;
    
    public String getStatistics() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ex) {
            Logger.getLogger(OSMExtractor.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("max ids: ");
        sb.append("n:");
        sb.append(Util.setDotsInStringValue(this.nodesTableEntries));
        sb.append(",w:");
        sb.append(Util.setDotsInStringValue(this.waysTableEntries));
        sb.append(",r:");
        sb.append(Util.setDotsInStringValue(this.relationsTableEntries));
        sb.append("\n");
        
        sb.append(this.lowerIDString);
        sb.append(" < current range <= ");
        sb.append(this.upperIDString);
        sb.append(") | read steps: " + Util.getValueWithDots(this.steplen));
        sb.append("\n");
        
        long newCheckedEntities =  this.numberCheckedNodes + this.numberCheckedWays + this.numberCheckedRelations;
        long diffCheckedEntities = newCheckedEntities - this.lastCheckedEntities;
        this.lastCheckedEntities = newCheckedEntities;
        
        //////////////////////////////////////////////////////////////////////
        //                             checked                              //
        //////////////////////////////////////////////////////////////////////
        sb.append("checked : ");
        sb.append(Util.getValueWithDots(this.lastCheckedEntities));
        sb.append(" (n:");
        sb.append(Util.getValueWithDots(this.numberCheckedNodes));
        sb.append(",w:");
        sb.append(Util.getValueWithDots(this.numberCheckedWays));
        sb.append(",r:");
        sb.append(Util.getValueWithDots(this.numberCheckedRelations));
        sb.append(")\n");

        //////////////////////////////////////////////////////////////////////
        //                             imported                             //
        //////////////////////////////////////////////////////////////////////
        sb.append("imported: ");
        sb.append(Util.getValueWithDots(this.numberImportedNodes + this.numberImportedWays + this.numberImportedRelations));
        sb.append(" (n:");
        sb.append(Util.getValueWithDots(this.numberImportedNodes));
        sb.append(",w:");
        sb.append(Util.getValueWithDots(this.numberImportedWays));
        sb.append(",r:");
        sb.append(Util.getValueWithDots(this.numberImportedRelations));
        sb.append(") ");
        
        sb.append("historic: ");
        sb.append(Util.getValueWithDots(this.historicInfos));
        sb.append(" | elapsed: ");
        sb.append(Util.getElapsedTime(this.startTime));
        sb.append("\n");
        
        
        //////////////////////////////////////////////////////////////////////
        //                          new / speed                             //
        //////////////////////////////////////////////////////////////////////
        
        this.lastCheckTime = this.lastCheckTime > 0 ? this.lastCheckTime : this.startTime;
        long now = System.currentTimeMillis();
        long diffTime = now - this.lastCheckTime;
        this.lastCheckTime = now;
        
        long diffInSeconds = diffTime / 1000;
        long speed = 0;
        if(diffInSeconds > 0) {
            speed = diffCheckedEntities / diffInSeconds;
        }
        if(speed > 0) {
            sb.append("new     : ");
            sb.append(Util.getValueWithDots(diffCheckedEntities));
            sb.append(" | ");
            sb.append(speed);
            sb.append(" per sec ");
            
            if(speed > 0 && !this.nodesTableEntries.equalsIgnoreCase("?")) {
                String currentEntriesMaxString;
                long readEntities = 0;
                
                if(this.waysTableEntries.equalsIgnoreCase("?")) {
                    currentEntriesMaxString = this.nodesTableEntries;
                    readEntities = this.numberCheckedNodes;
                } else if(this.relationsTableEntries.equalsIgnoreCase("?")) {
                    currentEntriesMaxString = this.waysTableEntries;
                    readEntities = this.numberCheckedWays;
                } else {
                    currentEntriesMaxString = this.relationsTableEntries;
                    readEntities = this.numberCheckedRelations;
                }

                long maxID = Long.parseLong(currentEntriesMaxString);
                
                long remains = maxID - readEntities;
                
                long eta = (remains / speed);
                
                sb.append(" | eta: ");
                sb.append(Util.getTimeString(eta));
            }
            sb.append("\n");

            long avgTime = getAndResetAvgTime(TIME_SELECT_ELEMENTS);
            sb.append("avg time: ");
            sb.append("select (" + Util.getValueWithDots(this.steplen) + "): ");
            sb.append(Util.getValueWithDots(avgTime));
            sb.append(" ms ");
            
            avgTime = getAndResetAvgTime(TIME_PROCESS_ELEMENTS);
            if(avgTime > -1) { 
                sb.append(" | ");
                sb.append("process(" + this.avgTimeScale + "): ");
                sb.append(Util.getValueWithDots(avgTime));
                sb.append(" ms ");
            }
            
            sb.append("\n");
            sb.append("          ");
            avgTime = getAndResetAvgTime(TIME_ADD_NODES);
            if(avgTime > -1) { 
                sb.append("addNodes(" + this.avgTimeScale + "): ");
                sb.append(Util.getValueWithDots(avgTime));
                sb.append(" ms ");
            }
            
            avgTime = getAndResetAvgTime(TIME_CREATE_NODE);
            if(avgTime > -1) { 
                sb.append(" | ");
                sb.append("createNode(" + this.avgTimeScale + "): ");
                sb.append(Util.getValueWithDots(avgTime));
                sb.append(" ms ");
            }
            
            sb.append("\n");
        }
        
        return sb.toString();
    }
    
    private long p = 0;
    private static final int P_MAX = 100;
    
    private void printStatistics() {
        // show little progress...
//        if(++p % P_MAX == 0) {
//            System.out.print(".");
//        }
//        
//        // line break
//        if(p % (P_MAX * 50) == 0) { // 50 signs each line
//            System.out.println(".");
//            // stats
//            if(p % (P_MAX * 500) == 0) { // after ten lines
//                System.out.println(Util.getValueWithDots(p * this.steplen * P_MAX * 500) + " lines read");
//            }
//        }
        
        // show big steps
        if(++this.printEra > PRINT_ERA_LENGTH) {
            this.printEra = 0;
            System.out.println("\n" + this.getStatistics());
        }
    }
    
    OSMElement currentElement = null;

    void processElement(ResultSet qResult, SQLStatementQueue sql, int elementType, boolean importUnnamedEntities) {
        this.currentElement = null;
        try {
            switch(elementType) {
                case NODE:
                    this.processNode(qResult, sql, importUnnamedEntities);
                    break;
                case WAY:
                    this.processWay(qResult, sql, importUnnamedEntities);
                    break;
                case RELATION:
                    this.processRelation(qResult, sql, importUnnamedEntities);
                    break;
            }
        }
        catch(Throwable t) {
            System.err.println("---------------------------------------------------------------------------");
            System.err.print("was handling a ");
            switch(elementType) {
                case NODE:
                    System.err.println("NODE ");
                    break;
                case WAY:
                    System.err.println("WAY ");
                    break;
                case RELATION:
                    System.err.println("RELATION ");
                    break;
            }
            if(currentElement != null) {
                System.err.println("current element osm id: " + this.currentElement.getOSMIDString());
            } else {
                System.err.println("current element is null");
            }
            Util.printExceptionMessage(t, sql, "uncatched throwable when processing element from intermediate db", true);
            System.err.println("---------------------------------------------------------------------------");
        }
    }

    @Override
    public void trigger() {
        System.out.println("\n" + this.getStatistics());
    }

    private static final int TIME_SELECT_ELEMENTS = 0;
    private static final int TIME_PROCESS_ELEMENTS = 1;
    private static final int TIME_ADD_NODES = 2;
    private static final int TIME_CREATE_NODE = 3;
    
    private int selectCount = 0;
    private long selectTime = 0;
    
    private int processCount = 0;
    private long processTime = 0;
    
    private int addNodesCount = 0;
    private long addNodesTime = 0;
    
    private int createNodesCount = 0;
    private long createNodesTime = 0;
    
    private void noteTime(long time, int what) {
        switch(what) {
            case TIME_SELECT_ELEMENTS: 
                this.selectCount++;
                this.selectTime += time;
                break;
                
            case TIME_PROCESS_ELEMENTS: 
                this.processCount++;
                this.processTime += time;
                break;
                
            case TIME_ADD_NODES: 
                this.addNodesCount++;
                this.addNodesTime += time;
                break;
                
            case TIME_CREATE_NODE: 
                this.createNodesCount++;
                this.createNodesTime += time;
                break;
        }
    }
    
    private int avgTimeScale = 1000;

    private long getAndResetAvgTime(int what) {
        long avg = 0;
        
        switch(what) {
            case TIME_SELECT_ELEMENTS: 
                if(this.selectCount == 0) return -1;
                
                avg = this.selectTime / this.selectCount;
                this.selectCount = 0;
                this.selectTime = 0;
                break;
                
            case TIME_PROCESS_ELEMENTS: 
                this.processCount /= avgTimeScale;
                if(this.processCount == 0) return -1;

                avg = this.processTime / this.processCount;
                this.processCount = 0;
                this.processTime = 0;
                break;
                
            case TIME_ADD_NODES: 
                this.addNodesCount /= avgTimeScale;
                if(this.addNodesCount == 0) return -1;
                
                avg = this.addNodesTime / this.addNodesCount;
                this.addNodesCount = 0;
                this.addNodesTime = 0;
                break;
                
            case TIME_CREATE_NODE: 
                this.createNodesCount /= avgTimeScale;
                if(this.createNodesCount == 0) return -1;
                
                avg = this.createNodesTime / this.createNodesCount;
                this.createNodesCount = 0;
                this.createNodesTime = 0;
                break;
        }
        
        return avg;
    }
}
