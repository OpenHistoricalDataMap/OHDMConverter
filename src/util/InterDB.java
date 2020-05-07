package util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
    public static final String STRING_DELIMITER = ",";
    
    public static void dropTables(SQLStatementQueue sql, String targetSchema) throws SQLException {
        // drop
        DB.drop(sql, targetSchema, NODETABLE);
        DB.drop(sql, targetSchema, RELATIONMEMBER);
        DB.drop(sql, targetSchema, RELATIONTABLE);
        DB.drop(sql, targetSchema, WAYMEMBER);
        DB.drop(sql, targetSchema, WAYTABLE);
    }
    
    public static void createTables(SQLStatementQueue sql, String schema) throws SQLException {
        try {
            InterDB.dropTables(sql, schema);
        } catch (SQLException e) {
//            System.err.println("error while dropping tables: " + e.getLocalizedMessage());
        }

        try {
            // write timestamp first
            DB.writeTimeStamp(sql, schema);

            // setup classification
//            OSMClassification.getOSMClassification().setupClassificationTable(sql, schema);
            
//            System.out.println("start tables creation for intermediate database");
            // NODETABLE
            // sequence
            DB.createSequence(sql, schema, NODETABLE);
            // table
            sql.append(DB.getCreateTableBegin(schema, NODETABLE));
            // add table specifics
            sql.append(",");
            sql.append("osm_id bigint,");
            sql.append("tstamp date,");
            sql.append("classcode bigint,");
            sql.append("otherclasscodes character varying,");
            sql.append("serializedTags character varying,");
            sql.append("longitude character varying,");
            sql.append("latitude character varying,");
            sql.append("ohdm_geom_id bigint,");
            sql.append("ohdm_geom_type int,");
            sql.append("ohdm_object_id bigint,");

            sql.append("geom_changed boolean DEFAULT false,");
            sql.append("object_changed boolean DEFAULT false,");
            sql.append("deleted boolean DEFAULT false,");
            sql.append("object_new boolean DEFAULT false,");
            sql.append("has_name boolean DEFAULT false,");
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
            sql.append("tstamp date,");
            sql.append("classcode bigint,");
            sql.append("otherclasscodes character varying,");
            sql.append("serializedTags character varying,");
            sql.append("ohdm_geom_id bigint,");
            sql.append("ohdm_geom_type int,");
            sql.append("ohdm_object_id bigint,");
            sql.append("node_ids character varying,");
//            sql.append("is_part boolean DEFAULT false,");
            /*
            sql.append("new boolean DEFAULT false,");
            sql.append("changed boolean DEFAULT false,");
            sql.append("deleted boolean DEFAULT false,");
             */
            sql.append("geom_changed boolean DEFAULT false,");
            sql.append("object_changed boolean DEFAULT false,");
            sql.append("deleted boolean DEFAULT false,");
            sql.append("object_new boolean DEFAULT false,");

            sql.append("has_name boolean DEFAULT false,");
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
            sql.append("tstamp date,");
            sql.append("classcode bigint,");
            sql.append("otherclasscodes character varying,");
            sql.append("serializedTags character varying,");
            sql.append("ohdm_geom_id bigint,");
            sql.append("ohdm_geom_type int,");
            sql.append("ohdm_object_id bigint,");
            sql.append("member_ids character varying,");
            /*
            sql.append("new boolean DEFAULT false,");
            sql.append("changed boolean DEFAULT false,");
            sql.append("deleted boolean DEFAULT false,");
             */
            sql.append("geom_changed boolean DEFAULT false,");
            sql.append("object_changed boolean DEFAULT false,");
            sql.append("deleted boolean DEFAULT false,");
            sql.append("object_new boolean DEFAULT false,");

            sql.append("has_name boolean DEFAULT false,");
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
//            System.out.println("intermediate database is ready for import");

        } catch (SQLException e) {
            Util.printExceptionMessage(e, sql, "when creating database", false);
        }
    }

    public static List<String> getIDList(String commaSeparatedStrings) {
        List<String> l = new ArrayList<>();
        if (commaSeparatedStrings == null) {
            return l;
        }
        StringTokenizer st = new StringTokenizer(commaSeparatedStrings, InterDB.STRING_DELIMITER);
        while (st.hasMoreTokens()) {
            String token = st.nextToken().trim();
         
            try {
                // an integer?
                Integer.parseInt(token);
                // yes
                l.add(token);
            }
            catch(NumberFormatException e) {
                // no number, go ahead - should happen. It's only my program.
            }
        }
        return l;
    }

    public static String getString(List<Integer> elements) {
        if (elements == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Integer eString : elements) {
            if (first) {
                first = false;
            } else {
                sb.append(InterDB.STRING_DELIMITER);
            }
            sb.append(eString);
        }
        return sb.toString();
    }
}
