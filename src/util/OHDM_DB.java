package util;

import java.sql.Connection;
import java.sql.SQLException;
import osm.OSMClassification;

/**
 *
 * @author thsc
 */
public class OHDM_DB {
    // Table names
    public static final String POLYGONS = "polygons";
    public static final String CONTENT = "content";
    public static final String GEOOBJECT = "geoobject";
    public static final String POINTS = "points";
    public static final String GEOOBJECT_GEOMETRY = "geoobject_geometry";
    public static final String GEOOBJECT_URL = "geoobject_url";
    public static final String LINES = "lines";
    public static final String EXTERNAL_USERS = "external_users";
    public static final String CLASSIFICATION = "classification";
    public static final String SUBSEQUENT_GEOM_USER = "subsequent_geom_user";
    public static final String EXTERNAL_SYSTEMS = "external_systems";
    public static final String GEOOBJECT_CONTENT = "geoobject_content";
    
    public static final int UNKNOWN_USER_ID = -1;
    public static final String URL = "url";
    
    // Geometry Types
    public static final int OHDM_GEOOBJECT_GEOMTYPE = 0;
    public static final int OHDM_POINT_GEOMTYPE = 1;
    public static final int OHDM_LINESTRING_GEOMTYPE = 2;
    public static final int OHDM_POLYGON_GEOMTYPE = 3;
    
    // same ? TODO
    public static final int POINT = 1;
    public static final int LINESTRING = 2;
    public static final int POLYGON = 3;
    public static final int RELATION = 0;
    
    public static String getGeometryName(int type) {
        switch(type) {
            case OHDM_DB.OHDM_POINT_GEOMTYPE: return "point";
            case OHDM_DB.OHDM_LINESTRING_GEOMTYPE: return "line";
            case OHDM_DB.OHDM_POLYGON_GEOMTYPE: return "polygon";
        }
        
        return null;
    }

    public static void dropOHDMTables(Connection targetConnection, String targetSchema) throws SQLException {
        DB.drop(targetConnection, targetSchema, EXTERNAL_SYSTEMS);
        DB.drop(targetConnection, targetSchema, EXTERNAL_USERS);
        DB.drop(targetConnection, targetSchema, CLASSIFICATION);
        DB.drop(targetConnection, targetSchema, CONTENT);
        DB.drop(targetConnection, targetSchema, GEOOBJECT);
        DB.drop(targetConnection, targetSchema, GEOOBJECT_CONTENT);
        DB.drop(targetConnection, targetSchema, GEOOBJECT_GEOMETRY);
        DB.drop(targetConnection, targetSchema, GEOOBJECT_URL);
        DB.drop(targetConnection, targetSchema, LINES);
        DB.drop(targetConnection, targetSchema, POINTS);
        DB.drop(targetConnection, targetSchema, POLYGONS);
        DB.drop(targetConnection, targetSchema, URL);
        DB.drop(targetConnection, targetSchema, SUBSEQUENT_GEOM_USER);
    }


    public static void dropNodeTables(Connection targetConnection, String targetSchema) throws SQLException {
        DB.drop(targetConnection, targetSchema, LINES);
    }

    public static void dropWayTables(Connection targetConnection, String targetSchema) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public static void dropRelationTables(Connection targetConnection, String targetSchema) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public static void createOHDMTables(Connection targetConnection, String schema) throws SQLException {
        SQLStatementQueue sq = new SQLStatementQueue(targetConnection);

        // EXTERNAL_SYSTEMS
        DB.createSequence(targetConnection, schema, EXTERNAL_SYSTEMS);
        sq.append(DB.getCreateTableBegin(schema, EXTERNAL_SYSTEMS));
        sq.append(",");
        sq.append("name character varying,");
        sq.append("description character varying");
        sq.append(");");
        sq.forceExecute();
        sq.append("INSERT INTO ");
        sq.append(DB.getFullTableName(schema, EXTERNAL_SYSTEMS));
        sq.append(" (name, description) VALUES ('osm', 'Open Street Map');");
        sq.forceExecute();
        
        // EXTERNAL_USERS
        DB.createSequence(targetConnection, schema, EXTERNAL_USERS);
        sq.append(DB.getCreateTableBegin(schema, EXTERNAL_USERS));
        sq.append(",");
        sq.append("userid bigint,");
        sq.append("username character varying,");
        sq.append("external_system_id bigint NOT NULL");
        sq.append(");");
        sq.forceExecute();
        
        // CLASSIFICATION
        DB.createSequence(targetConnection, schema, CLASSIFICATION);
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, CLASSIFICATION));
        sq.append(",");
        sq.append("class character varying,");
        sq.append("subclassname character varying");
        sq.append(");");
        sq.forceExecute();
        
        // fill classification table
        OSMClassification.getOSMClassification().write2Table(targetConnection, DB.getFullTableName(schema, CLASSIFICATION));
        
        // CONTENT
        DB.createSequence(targetConnection, schema, CONTENT);
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, CONTENT));
        sq.append(",");
        sq.append("name character varying,");
        sq.append("value bytea NOT NULL,");
        sq.append("mimetype character varying,");
        sq.append("source_user_id bigint NOT NULL");
        sq.append(");");
        sq.forceExecute();
        
        // GEOOBJECT
        DB.createSequence(targetConnection, schema, GEOOBJECT);
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, GEOOBJECT));
        sq.append(",");
        sq.append("name character varying,");
        sq.append("source_user_id bigint NOT NULL");
        sq.append(");");
        sq.forceExecute();
        sq.append("INSERT INTO ");
        sq.append(DB.getFullTableName(schema, GEOOBJECT));
        sq.append("(id, source_user_id) VALUES (0, 1);");
        sq.forceExecute();
        
        // GEOOBJECT_CONTENT
        DB.createSequence(targetConnection, schema, GEOOBJECT_CONTENT);
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, GEOOBJECT_CONTENT));
        sq.append(",");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("valid_since_offset bigint DEFAULT 0,");
        sq.append("valid_until_offset bigint DEFAULT 0,");
        sq.append("geoobject_id bigint NOT NULL,");
        sq.append("content_id bigint NOT NULL");
        sq.append(");");
        sq.forceExecute();
        
        // GEOOBJECT_GEOMETRY
        DB.createSequence(targetConnection, schema, GEOOBJECT_GEOMETRY);
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, GEOOBJECT_GEOMETRY));
        sq.append(",");
        sq.append("id_target bigint,");
        sq.append("type_target bigint,");
        sq.append("id_geoobject_source bigint NOT NULL,");
        sq.append("role character varying,");
        sq.append("classification_id bigint NOT NULL,");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("valid_since_offset bigint DEFAULT 0,");
        sq.append("valid_until_offset bigint DEFAULT 0,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.forceExecute();
        
        // GEOOBJECT_URL
        DB.createSequence(targetConnection, schema, GEOOBJECT_URL);
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, GEOOBJECT_URL));
        sq.append(",");
        sq.append("geoobject_id bigint NOT NULL,");
        sq.append("url_id bigint NOT NULL,");
        sq.append("valid_since date NOT NULL,");
        sq.append("valid_until date NOT NULL,");
        sq.append("valid_since_offset bigint DEFAULT 0,");
        sq.append("valid_until_offset bigint DEFAULT 0");
        sq.append(");");
        sq.forceExecute();
        
        // LINES
        DB.createSequence(targetConnection, schema, LINES);
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, LINES));
        sq.append(",");
        sq.append("line geometry,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.forceExecute();
        
        // POINTS
        DB.createSequence(targetConnection, schema, POINTS);
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, POINTS));
        sq.append(",");
        sq.append("point geometry,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.forceExecute();
        
        // POLYGONS
        DB.createSequence(targetConnection, schema, POLYGONS);
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, POLYGONS));
        sq.append(",");
        sq.append("polygon geometry,");
        sq.append("source_user_id bigint");
        sq.append(");");
        sq.forceExecute();
        
        // URL
        DB.createSequence(targetConnection, schema, URL);
        sq = new SQLStatementQueue(targetConnection);
        sq.append(DB.getCreateTableBegin(schema, URL));
        sq.append(",");
        sq.append("url character varying,");
        sq.append("source_user_id bigint");
        sq.append(");");
        
        // SUBSEQUENT_GEOM_USER
        DB.createSequence(targetConnection, schema, SUBSEQUENT_GEOM_USER);
        // table
        sq.append(DB.getCreateTableBegin(schema, SUBSEQUENT_GEOM_USER));
        // add table specifics
        sq.append(",");
        sq.append("target_id bigint NOT NULL, ");
        sq.append("point_id bigint, ");
        sq.append("line_id bigint,");
        sq.append("polygon_id bigint");
        sq.append(");");
        sq.forceExecute();
    }
}
