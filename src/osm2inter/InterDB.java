package osm2inter;

import java.sql.SQLException;
import util.DB;
import util.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public class InterDB {
    public static final String NODETABLE = "nodes";
    public static final String RELATIONMEMBER = "relationmember";
    public static final String RELATIONTABLE = "relations";
    public static final String WAYMEMBER = "waynodes";
    public static final String WAYTABLE = "ways";
    
    static void dropTables(SQLStatementQueue sql, String targetSchema) throws SQLException {
        // drop
        DB.drop(sql, targetSchema, NODETABLE);
        DB.drop(sql, targetSchema, RELATIONMEMBER);
        DB.drop(sql, targetSchema, RELATIONTABLE);
        DB.drop(sql, targetSchema, WAYMEMBER);
        DB.drop(sql, targetSchema, WAYTABLE);
    }
    
    static void createTables(SQLStatementQueue sql, String schema) throws SQLException {
        System.out.println("--- setting up intermediate db ---");
        try {
            System.out.println("drop tables");
            InterDB.dropTables(sql, schema);
        } catch (SQLException e) {
            System.err.println("error while dropping tables: " + e.getLocalizedMessage());
        }

        try {
            // setup classification
//            OSMClassification.getOSMClassification().setupClassificationTable(sql, schema);
            
            // NODETABLE
            // sequence
            DB.createSequence(sql, schema, NODETABLE);
            // table
            sql.append(DB.getCreateTableBegin(schema, NODETABLE));
            // add table specifics
            sql.append(",");
            sql.append("osm_id bigint,");
            sql.append("classcode bigint,");
            sql.append("serializedTags character varying,");
            sql.append("longitude character varying,");
            sql.append("latitude character varying,");
            sql.append("ohdm_geom_id bigint,");
            sql.append("ohdm_object_id bigint,");
            sql.append("is_part boolean DEFAULT false,");
            sql.append("new boolean,");
            sql.append("changed boolean,");
            sql.append("valid boolean);");
            sql.forceExecute();

            // WAYTABLE
            // sequence
            DB.createSequence(sql, schema, WAYTABLE);
            // table
            sql.append(DB.getCreateTableBegin(schema, WAYTABLE));
            // add table specifics
            sql.append(",");
            sql.append("osm_id bigint,");
            sql.append("classcode bigint,");
            sql.append("serializedTags character varying,");
            sql.append("ohdm_geom_id bigint,");
            sql.append("ohdm_object_id bigint,");
            sql.append("node_ids character varying,");
            sql.append("is_part boolean DEFAULT false,");
            sql.append("valid boolean);");
            sql.forceExecute();

            // RELATIONTABLE
            // sequence
            DB.createSequence(sql, schema, RELATIONTABLE);
            // table
            sql.append(DB.getCreateTableBegin(schema, RELATIONTABLE));
            // add table specifics
            sql.append(",");
            sql.append("osm_id bigint,");
            sql.append("classcode bigint,");
            sql.append("serializedTags character varying,");
            sql.append("ohdm_geom_id bigint,");
            sql.append("ohdm_object_id bigint,");
            sql.append("member_ids character varying,");
            sql.append("valid boolean);");
            sql.forceExecute();

            // WAYMEMBER
            // sequence
            DB.createSequence(sql, schema, WAYMEMBER);
            // table
            sql.append(DB.getCreateTableBegin(schema, WAYMEMBER));
            // add table specifics
            sql.append(",");
            sql.append("way_id bigint, ");
            sql.append("node_id bigint");
            sql.append(");");
            sql.forceExecute();

            // RELATIONMEMBER
            // sequence
            DB.createSequence(sql, schema, RELATIONMEMBER);
            // table
            sql.append(DB.getCreateTableBegin(schema, RELATIONMEMBER));
            // add table specifics
            sql.append(",");
            sql.append("relation_id bigint NOT NULL, ");
            sql.append("node_id bigint,");
            sql.append("way_id bigint,");
            sql.append("member_rel_id bigint,");
            sql.append("role character varying");
            sql.append(");");
            sql.forceExecute();
      
        } catch (SQLException e) {
            System.err.println("error while setting up tables: " + e.getLocalizedMessage());
        }
    }
}
