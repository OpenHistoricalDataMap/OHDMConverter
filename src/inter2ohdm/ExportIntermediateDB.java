package inter2ohdm;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import util.InterDB;
import static util.InterDB.NODETABLE;
import static util.InterDB.RELATIONMEMBER;
import static util.InterDB.RELATIONTABLE;
import static util.InterDB.WAYMEMBER;
import static util.InterDB.WAYTABLE;
import util.DB;
import util.SQLStatementQueue;
import util.Util;

/**
 *
 * @author thsc
 */
public class ExportIntermediateDB extends IntermediateDB {
    private final Importer importer;
    
    private final String schema;
    
    private int printEra = 0;
    private final static int PRINT_ERA_LENGTH = 10000;
    private static final int DEFAULT_STEP_LEN = 1000;

    // for statistics
    private long number;
    private long numberNodes = 0;
    private long numberWays = 0;
    private long numberRelations = 0;
    private long historicInfos = 0;
    private final long startTime;
    
    static final int NODE = 0;
    static final int WAY = 1;
    static final int RELATION = 2;
    
    ExportIntermediateDB(Connection sourceConnection, String schema, Importer importer, int steplen) {
        super(sourceConnection, schema);
        
        this.startTime = System.currentTimeMillis();
        this.number = 0;
        
        this.schema = schema;
        this.importer = importer;
        
        if(steplen < 1) {
            steplen = DEFAULT_STEP_LEN;
        }
        
        this.steps = new BigDecimal(steplen);
    }

    private BigDecimal initialLowerID;
    private BigDecimal initialUpperID;
    private BigDecimal initialMaxID;
    private final BigDecimal steps;
    
    private void calculateInitialIDs(SQLStatementQueue sql, String tableName) {
        // first: figure out min and max osm_id in nodes table
        try {
            sql.append("SELECT min(id) FROM ");
            sql.append(DB.getFullTableName(this.schema, tableName));
            sql.append(";");

            ResultSet result = sql.executeWithResult();
            result.next();
            BigDecimal minID = result.getBigDecimal(1);

            sql.append("SELECT max(id) FROM ");
            sql.append(DB.getFullTableName(this.schema, tableName));
            sql.append(";");

            result = sql.executeWithResult();
            result.next();
            this.initialMaxID = result.getBigDecimal(1);

            this.initialLowerID = minID.subtract(new BigDecimal(1));
            this.initialUpperID = minID.add(this.steps);
        }
        catch(SQLException se) {
            Util.printExceptionMessage(se, sql, "when calculating initial min max ids for select of nodes, ways or relations", false);
        }
    }
    
    void processNode(ResultSet qResult, SQLStatementQueue sql, boolean importUnnamedEntities) {
        OSMNode node = null;
        try {
            node = this.createOHDMNode(qResult);
            this.currentElement = node;

            if(node.isConsistent()) {
                // now process that stuff
                if(this.importer.importNode(node, importUnnamedEntities)) {
                    this.numberNodes++;
                }

                if(this.importer.importPostProcessing(node, importUnnamedEntities)) {
                    this.historicInfos++;
                }
            } else {
                this.printError(System.err, "node not consistent:\n" + node);
            }
        }
        catch(SQLException se) {
            System.err.println("node osm_id: " + node.getOSMIDString());
            Util.printExceptionMessage(se, sql, "failure when processing node.. non fatal", true);
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

//            if(!way.isPart() && way.getName() == null) notPartNumber++;

            this.addNodes2OHDMWay(way);

            if(way.isConsistent()) {
                // process that stuff
                if(this.importer.importWay(way, importUnnamedEntities)) {
                    this.numberWays++;
                }

                if(this.importer.importPostProcessing(way, importUnnamedEntities)) {
                    this.historicInfos++;
                }
            } else {
                this.printError(System.err, "way not consistent:\n" + way);
            }
        }
        catch(SQLException se) {
            System.err.println("way: " + way);
            Util.printExceptionMessage(se, sql, "failure when processing way.. non fatal", true);
        }
    }
    
    void processRelation(ResultSet qResult, SQLStatementQueue sql, boolean importUnnamedEntities) {
        OSMRelation relation = null;
        
        try {
            relation = this.createOHDMRelation(qResult);
            
            String r_id = relation.getOSMIDString();
            if(r_id.equalsIgnoreCase("4451529")) {
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
                OSMElement.GeometryType type = null;

                sql.append("SELECT * FROM ");

                id = qResultRelation.getBigDecimal("node_id");
                if(id != null) {
                    sql.append(DB.getFullTableName(this.schema, NODETABLE));
                    type = OSMElement.GeometryType.POINT;
                } else {
                    id = qResultRelation.getBigDecimal("way_id");
                    if(id != null) {
                        sql.append(DB.getFullTableName(this.schema, WAYTABLE));
                        type = OSMElement.GeometryType.LINESTRING;
                    } else {
                        id = qResultRelation.getBigDecimal("member_rel_id");
                        if(id != null) {
                            sql.append(DB.getFullTableName(this.schema, RELATIONTABLE));
                            type = OSMElement.GeometryType.RELATION;
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
                        case POINT: 
                            memberElement = this.createOHDMNode(memberResult);
                            break;
                        case LINESTRING:
                            memberElement = this.createOHDMWay(memberResult);
                            break;
                        case RELATION:
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
            
            if(relation.isConsistent()) {
                // process that stuff
                if(relationMemberComplete && this.importer.importRelation(relation, importUnnamedEntities)) {
                    this.numberRelations++;

                    if(this.importer.importPostProcessing(relation, importUnnamedEntities)) {
                        this.historicInfos++;
                    }

                } 
            } else {
                this.printError(System.err, "inconsistent relation\n" + relation);
            }
        }
        catch(SQLException se) {
            System.err.println("relation osm_id: " + relation.getOSMIDString());
            Util.printExceptionMessage(se, sql, "failure when processing relation.. non fatal", true);
        }
    }
    
    void processNodes(SQLStatementQueue sql, boolean importUnnamedEntities) {
        this.processElements(sql, NODE, importUnnamedEntities);
    }
    
    void processWays(SQLStatementQueue sql, boolean importUnnamedEntities) {
        this.processElements(sql, WAY, importUnnamedEntities);
    }
    
    void processRelations(SQLStatementQueue sql, boolean importUnnamedEntities) {
        this.processElements(sql, RELATION, importUnnamedEntities);
    }
    
    void processElements(SQLStatementQueue sql, int elementType, boolean importUnnamedEntities) {
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
        
        this.calculateInitialIDs(sql, elementTableName);
        // first: figure out min and max osm_id in nodes table
        BigDecimal lowerID = this.initialLowerID;
        BigDecimal upperID = this.initialUpperID;
        BigDecimal maxID = this.initialMaxID;
            
        try {
            do {
                this.printSelectBetween(elementTableName, lowerID.toString(), upperID.toString());
        
                sql.append("SELECT * FROM ");
                sql.append(DB.getFullTableName(this.schema, elementTableName));
                sql.append(" where id <= "); // including upper
                sql.append(upperID.toString());
                sql.append(" AND id > "); // excluding lower 
                sql.append(lowerID.toString());
                sql.append(" AND classcode != -1 "); // excluding lower 
                if(importUnnamedEntities) {
                    sql.append(" AND serializedtags like '%004name%'"); // entities with a name
                }
                sql.append(";");
                ResultSet qResult = sql.executeWithResult();
                
                while(qResult.next()) {
                    this.number++;
                    this.printStatistics();
                    this.processElement(qResult, sql, elementType, importUnnamedEntities);
                }
                
                // next bulk of data
                lowerID = upperID;
                upperID = upperID.add(steps);
                
                if(upperID.compareTo(initialMaxID) == 1 && lowerID.compareTo(initialMaxID) == -1) {
                    upperID = initialMaxID; // last round
                }
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
        SQLStatementQueue sql = new SQLStatementQueue(this.sourceConnection);

        // TODO: replace join with enumeration from member id in table ways itself..
        sql.append("select * from ");
        sql.append(DB.getFullTableName(this.schema, NODETABLE));
        sql.append(" where osm_id IN (SELECT node_id FROM ");            
        sql.append(DB.getFullTableName(this.schema, WAYMEMBER));
        sql.append(" where way_id = ");            
        sql.append(way.getOSMIDString());
        sql.append(");");  

        ResultSet qResultNode = sql.executeWithResult();

        while(qResultNode.next()) {
            OSMNode node = this.createOHDMNode(qResultNode);
            way.addNode(node);
        }
        
        qResultNode.close();
        
        return way;
    }
    
    private void printSelectBetween(String what, String from, String to) {
        System.out.println("---------------------------------------");
        System.out.print("select ");
        System.out.print(Util.setDotsInStringValue(from));
        System.out.print(" < ");
        System.out.print(what);
        System.out.print(".id =< ");
        System.out.println(Util.setDotsInStringValue(to));
        System.out.println("---------------------------------------");
    }
    
    private void printFinished(String what) {
        System.out.println("********************************************************************************");
        System.out.print("Finished importing ");
        System.out.println(what);
        System.out.println(this.getStatistics());
        System.out.println("********************************************************************************");
    }
    
    public String getStatistics() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("checked: ");
        sb.append(Util.getValueWithDots(this.number));
        sb.append(" | imported: ");
        sb.append(Util.getValueWithDots(this.numberNodes + this.numberWays + this.numberRelations));
        sb.append(" (n:");
        sb.append(Util.getValueWithDots(this.numberNodes));
        sb.append(", w:");
        sb.append(Util.getValueWithDots(this.numberWays));
        sb.append(", r:");
        sb.append(Util.getValueWithDots(this.numberRelations));
        sb.append(") | h:");
        sb.append(Util.getValueWithDots(this.historicInfos));
        sb.append(" | elapsed: ");
        sb.append(Util.getElapsedTime(this.startTime));
        
        return sb.toString();
    }
    
    private void printStatistics() {
        if(++this.printEra >= PRINT_ERA_LENGTH) {
            this.printEra = 0;
            System.out.println(this.getStatistics());
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
}
