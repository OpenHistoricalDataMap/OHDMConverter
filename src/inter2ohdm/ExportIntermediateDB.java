package inter2ohdm;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import static osm2inter_v2.InterDB.NODETABLE;
import static osm2inter_v2.InterDB.RELATIONMEMBER;
import static osm2inter_v2.InterDB.RELATIONTABLE;
import static osm2inter_v2.InterDB.WAYMEMBER;
import static osm2inter_v2.InterDB.WAYTABLE;
import util.DB;
import util.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public class ExportIntermediateDB extends IntermediateDB {
    private final Importer importer;
    
    private static final int DEFAULT_STEP_LEN = 1000;
    private int numberNodes = 0;
    private int numberWays = 0;
    private int numberRelations = 0;
    private final String schema;
    
    private int historicInfos = 0;
    
    private final SQLStatementQueue sourceQueue;
    
    ExportIntermediateDB(Connection sourceConnection, String schema, Importer importer, int steplen) {
        super(sourceConnection, schema);
        
        this.schema = schema;
        this.importer = importer;
        
        this.sourceQueue = new SQLStatementQueue(sourceConnection);
        
        if(steplen < 1) {
            steplen = DEFAULT_STEP_LEN;
        }
        
        this.steps = new BigDecimal(steplen);
    }

    void processNodes() {
        this.processNodes(this.sourceQueue);
    }
    
    private BigDecimal initialLowerID;
    private BigDecimal initialUpperID;
    private BigDecimal initialMaxID;
    private BigDecimal steps;
    
    private void calculateInitialIDs(SQLStatementQueue sql, String tableName) throws SQLException {
        // first: figure out min and max osm_id in nodes table
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
    
    void processNodes(SQLStatementQueue sql) {
        // go through node table and do what has to be done.

        int number = 0;
        int notPartNumber = 0;
            
        try {
            this.calculateInitialIDs(sql, NODETABLE);
            // first: figure out min and max osm_id in nodes table
            BigDecimal lowerID = this.initialLowerID;
            BigDecimal upperID = this.initialUpperID;
            BigDecimal maxID = this.initialMaxID;
            
            System.out.println("Nodes... print a star after 100 nodes");
            do {
                System.out.print("select nodes between ");
                System.out.print(lowerID.toString());
                System.out.print(" and ");
                System.out.println(upperID.toString());
        
                sql.append("SELECT * FROM ");
                sql.append(DB.getFullTableName(this.schema, NODETABLE));
                sql.append(" where id <= "); // including upper
                sql.append(upperID.toString());
                sql.append(" AND id > "); // excluding lower 
                sql.append(lowerID.toString());
                sql.append(";");
                ResultSet qResultNode = sql.executeWithResult();
                
                while(qResultNode.next()) {
                    number++;
                    if(number % 100 == 0) {System.out.print("*");}
                    if(number % 1000 == 0) {
                        System.out.print("\n");
                    }

                    OHDMNode node = this.createOHDMNode(qResultNode);

                    if(!node.isPart() && node.getName() == null) notPartNumber++;

                    // now process that stuff
                    if(this.importer.importNode(node)) {
                        this.numberNodes++;
                    }

                    if(this.importer.importPostProcessing(node)) {
                        this.historicInfos++;
                    }

                }
                
                // next bulk of data
                lowerID = upperID;
                upperID = upperID.add(steps);
                
                if(upperID.compareTo(initialMaxID) == 1 && lowerID.compareTo(initialMaxID) == -1) {
                    upperID = initialMaxID; // last round
                }
                
            } while(!(upperID.compareTo(initialMaxID) == 1));
        } catch (SQLException ex) {
            System.err.println("inter2ohdm: exception when processing sql request: " + sql.toString());
            System.err.println(ex.getLocalizedMessage());
        }
        System.out.println("\nChecked / imported nodes / not part and no identity: " + number + " / " + this.numberNodes + " / " + notPartNumber);
    }
    
    void processWays() {
        this.processWays(this.sourceQueue);
    }
    
    void processWays(SQLStatementQueue sql) {
        int number = 0;
        int notPartNumber = 0;
        
        try {
            this.calculateInitialIDs(sql, WAYTABLE);
            
            // first: figure out min and max osm_id in nodes table
            BigDecimal lowerID = this.initialLowerID;
            BigDecimal upperID = this.initialUpperID;
            BigDecimal maxID = this.initialMaxID;
            
            System.out.println("Ways... print a star after 100 ways");

            do {
                System.out.print("select ways between ");
                System.out.print(lowerID.toString());
                System.out.print(" and ");
                System.out.println(upperID.toString());
                
                sql.append("SELECT * FROM ");
                sql.append(DB.getFullTableName(this.schema, WAYTABLE));
                sql.append(" where id <= "); // including upper
                sql.append(upperID.toString());
                sql.append(" AND id > "); // excluding lower 
                sql.append(lowerID.toString());
                sql.append(";");

                //PreparedStatement stmt = this.sourceConnection.prepareStatement(sql.toString());
                ResultSet qResultWay = sql.executeWithResult();

                while(qResultWay.next()) {
                    number++;
                    if(number % 100 == 0) {System.out.print("*");}
                    if(number % 1000 == 0) {
                        System.out.print("\n");
                    }
                    OHDMWay way = this.createOHDMWay(qResultWay);

                    if(!way.isPart() && way.getName() == null) notPartNumber++;

                    this.addNodes2OHDMWay(way);

                    // process that stuff
                    if(this.importer.importWay(way)) {
                        this.numberWays++;
                    }

                    if(this.importer.importPostProcessing(way)) {
                        this.historicInfos++;
                    }
                }
                
                // next bulk of data
                lowerID = upperID;
                upperID = upperID.add(steps);
                
                if(upperID.compareTo(initialMaxID) == 1 && lowerID.compareTo(initialMaxID) == -1) {
                    upperID = initialMaxID; // last round
                }
                
            } while(!(upperID.compareTo(initialMaxID) == 1));
            
        } catch (SQLException ex) {
            System.err.println("inter2ohdm: exception when processing sql request: " + sql.toString());
            System.err.println(ex.getLocalizedMessage());
        }
        
        System.out.println("\nChecked / imported ways / not part and identity:  " + number + " / " + this.numberWays + " / " + notPartNumber);
    }
    
    @Override
    OHDMWay addNodes2OHDMWay(OHDMWay way) throws SQLException {
        // find all associated nodes and add to that way
        /* SQL Query is like this
            select * from nodes_table where osm_id IN 
            (SELECT node_id FROM waynodes_table where way_id = ID_of_way);            
        */ 
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
            OHDMNode node = this.createOHDMNode(qResultNode);
            way.addNode(node);
        }
        
        qResultNode.close();
        
        return way;
    }
    
    void processRelations() {
        this.processRelations(this.sourceQueue);
    }
    
    void processRelations(SQLStatementQueue sql) {
        int number = 0;
        boolean debug_alreadyPrinted = false;
        
        try {
            this.calculateInitialIDs(sql, RELATIONTABLE);
            // first: figure out min and max osm_id in nodes table
            BigDecimal lowerID = this.initialLowerID;
            BigDecimal upperID = this.initialUpperID;
            BigDecimal maxID = this.initialMaxID;
            
            System.out.println("Relations... print a star after 100 relations");

            do {
                System.out.print("select relations between ");
                System.out.print(lowerID.toString());
                System.out.print(" and ");
                System.out.println(upperID.toString());

                sql.append("SELECT * FROM ");
                sql.append(DB.getFullTableName(this.schema, RELATIONTABLE));
                sql.append(" where id <= "); // including upper
                sql.append(upperID.toString());
                sql.append(" AND id > "); // excluding lower 
                sql.append(lowerID.toString());
                sql.append(";");

                ResultSet qResultRelations = sql.executeWithResult();

                while(qResultRelations.next()) {
                    debug_alreadyPrinted = false;
                    number++;
                    if(number % 100 == 0) {System.out.print("*");}
                    if(number % 1000 == 0) {
                        System.out.print("\n");
                    }

                    OHDMRelation relation = this.createOHDMRelation(qResultRelations);

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
                        OHDMElement.GeometryType type = null;

                        sql.append("SELECT * FROM ");

                        id = qResultRelation.getBigDecimal("node_id");
                        if(id != null) {
                            sql.append(DB.getFullTableName(this.schema, NODETABLE));
                            type = OHDMElement.GeometryType.POINT;
                        } else {
                            id = qResultRelation.getBigDecimal("way_id");
                            if(id != null) {
                                sql.append(DB.getFullTableName(this.schema, WAYTABLE));
                                type = OHDMElement.GeometryType.LINESTRING;
                            } else {
                                id = qResultRelation.getBigDecimal("member_rel_id");
                                if(id != null) {
                                    sql.append(DB.getFullTableName(this.schema, RELATIONTABLE));
                                    type = OHDMElement.GeometryType.RELATION;
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
                            OHDMElement memberElement = null;
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
                            System.out.println("would removed relation: " + relation.getOSMIDString());
                            debug_alreadyPrinted = true;
                            //relation.remove();
                            relationMemberComplete = false; 
                        }
                        memberResult.close();

                        if(!relationMemberComplete) break;
                    }

                    // process that stuff
                    if(relationMemberComplete && this.importer.importRelation(relation)) {
                        this.numberRelations++;

                        if(this.importer.importPostProcessing(relation)) {
                            this.historicInfos++;
                        }

                    } else {
                        if(!debug_alreadyPrinted) {
                            String type = relation.getClassName();
                            if(type == null) type ="not set";
                            System.out.println("not imported relation: " + relation.getOSMIDString() + " / classname: " + type);
                        }
                    }
                }
                
                // next bulk of data
                lowerID = upperID;
                upperID = upperID.add(steps);
                
                if(upperID.compareTo(initialMaxID) == 1 && lowerID.compareTo(initialMaxID) == -1) {
                    upperID = initialMaxID; // last round
                }
                
            } while(!(upperID.compareTo(initialMaxID) == 1));
                
        } catch (SQLException ex) {
            System.err.println("inter2ohdm: exception when processing sql request: " + sql.toString());
            System.err.println(ex.getLocalizedMessage());
        }
        
        System.out.println("\nChecked / imported relations: " + number + " / " + this.numberRelations);
    }
    
    public String getStatistics() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("imported: ");
        sb.append(this.numberNodes);
        sb.append(" nodes | ");
        sb.append(this.numberWays);
        sb.append(" ways | ");
        sb.append(this.numberRelations);
        sb.append(" relations | ");
        sb.append(this.historicInfos);
        sb.append(" historical information");
        
        return sb.toString();
    }
}
