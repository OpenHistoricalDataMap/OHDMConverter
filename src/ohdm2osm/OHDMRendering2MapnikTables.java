package ohdm2osm;

import osm.OSMClassification;
import util.DB;
import util.OHDM_DB;
import util.Parameter;
import util.SQLStatementQueue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class OHDMRendering2MapnikTables {

    private final Connection connection;
    private final Parameter sourceParameter;
    private final Parameter targetParameter;
    private final SQLStatementQueue sql;

    public OHDMRendering2MapnikTables(Parameter sourceParameter, Parameter targetParameter) throws SQLException {
        this.sourceParameter = sourceParameter;
        this.targetParameter = targetParameter;

        if(!sourceParameter.getdbName().equalsIgnoreCase(targetParameter.getdbName())) {
            throw new SQLException("source and target must be in same database - fatal, stop process");
        }

        this.connection = DB.createConnection(sourceParameter);
        this.sql = new SQLStatementQueue(this.connection);
    }

    /**
     * Convert renderiing tables into mapnik tables:
     planet_osm_point
     planet_osm_line
     planet_osm_polygon
     planet_osm_roads
     * @param nodeTables
     * @param linesTables
     * @param polygonTables
     */
    void convert(List<String> nodeTables, List<String> linesTables, List<String> polygonTables) throws SQLException {
        // create tables
        this.setupPointTable();
        this.convertPoints(nodeTables);

        this.setupLinesTable(); // TODO
        this.convertLines(linesTables);
    }

    private void convertPoints(List<String> pointTableNames) throws SQLException {
        SQLStatementQueue insertSQL = new SQLStatementQueue(DB.createConnection(this.targetParameter));
        OSMClassification osmC = OSMClassification.getOSMClassification();

        if(pointTableNames != null) {
            for(String tableName : pointTableNames) {
                System.out.println("converting " + tableName);
                sql.append("SELECT st_astext(point), geom_id, name, valid_since, valid_until, subclassname, classid  FROM ");
                sql.append(util.DB.getFullTableName(this.sourceParameter.getSchema(), tableName));

                try {
                    ResultSet resultSet = sql.executeWithResult();

                    while(resultSet.next()) {
                        String className = osmC.getClassNameByFullName(
                                osmC.getFullClassName(resultSet.getInt("classid")));
                        String subClassName = resultSet.getString("subclassname");

                        className = this.ohdmClass2mapnikColumn(className);
                        subClassName = this.ohdmSubClassName2mapnikColumnValue(className, subClassName);

                        insertSQL.append("INSERT into ");
                        insertSQL.append(util.DB.getFullTableName(this.targetParameter.getSchema(), POINT_TABLE_NAME));
                        insertSQL.append("(way, osm_id, name, valid_since, valid_until, ");
                        insertSQL.append(className);
                        insertSQL.append(") VALUES (ST_TRANSFORM(ST_GeomFromEwkt('SRID=3857;");
                        insertSQL.append(resultSet.getString(1));
                        insertSQL.append("'), 900913), ");
                        insertSQL.append(resultSet.getString(2));
                        insertSQL.append(", '");
                        insertSQL.append(resultSet.getString(3));
                        insertSQL.append("', '");
                        insertSQL.append(resultSet.getString(4));
                        insertSQL.append("', '");
                        insertSQL.append(resultSet.getString(5));
                        insertSQL.append("', '");
                        insertSQL.append(subClassName);
                        insertSQL.append(" ');");
                        insertSQL.forceExecute(); // DEBUG TODO
                    }
                }
                catch(SQLException sqle) {
                    System.err.println("error while select: " + sqle.toString());
                    System.err.println(sql.toString());
                }

                // force execute after each table
                /* // DEBUG TODO
                try {
                    insertSQL.forceExecute();
                }
                catch(Exception e) {
                    System.err.println("error while converting points");
                    System.err.println(insertSQL.toString().substring(0, 50));
                }
                 */
            }
        }
    }

    // FYI: https://www.postgresql.org/docs/9.1/hstore.html
    private void convertLines(List<String> lineTableNames) throws SQLException {
        SQLStatementQueue insertSQL = new SQLStatementQueue(DB.createConnection(this.targetParameter));
        OSMClassification osmC = OSMClassification.getOSMClassification();

        if(lineTableNames != null) {
            for(String tableName : lineTableNames) {

                // TODO
                System.out.println("converting " + tableName);
                sql.append("SELECT st_astext(point), geom_id, name, valid_since, valid_until, subclassname, classid  FROM ");
                sql.append(util.DB.getFullTableName(this.sourceParameter.getSchema(), tableName));

                try {
                    ResultSet resultSet = sql.executeWithResult();

                    while(resultSet.next()) {
                        String className = osmC.getClassNameByFullName(
                                osmC.getFullClassName(resultSet.getInt("classid")));
                        String subClassName = resultSet.getString("subclassname");

                        className = this.ohdmClass2mapnikColumn(className);
                        subClassName = this.ohdmSubClassName2mapnikColumnValue(className, subClassName);

                        insertSQL.append("INSERT into ");
                        insertSQL.append(util.DB.getFullTableName(this.targetParameter.getSchema(), POINT_TABLE_NAME));
                        insertSQL.append("(way, osm_id, name, valid_since, valid_until, ");
                        insertSQL.append(className);
                        insertSQL.append(") VALUES (ST_TRANSFORM(ST_GeomFromEwkt('SRID=3857;");
                        insertSQL.append(resultSet.getString(1));
                        insertSQL.append("'), 900913), ");
                        insertSQL.append(resultSet.getString(2));
                        insertSQL.append(", '");
                        insertSQL.append(resultSet.getString(3));
                        insertSQL.append("', '");
                        insertSQL.append(resultSet.getString(4));
                        insertSQL.append("', '");
                        insertSQL.append(resultSet.getString(5));
                        insertSQL.append("', '");
                        insertSQL.append(subClassName);
                        insertSQL.append(" ');");
                        insertSQL.forceExecute(); // DEBUG TODO
                    }
                }
                catch(SQLException sqle) {
                    System.err.println("error while select: " + sqle.toString());
                    System.err.println(sql.toString());
                }

                // force execute after each table
                /* // DEBUG TODO
                try {
                    insertSQL.forceExecute();
                }
                catch(Exception e) {
                    System.err.println("error while converting points");
                    System.err.println(insertSQL.toString().substring(0, 50));
                }
                 */
            }
        }
    }

    public static final String POINT_TABLE_NAME = "planet_osm_point";
    public static final String LINE_TABLE_NAME = "planet_osm_line";

    void setupLinesTable() throws SQLException {
        sql.append("DROP TABLE ");
        sql.append(DB.getFullTableName(this.targetParameter.getSchema() , LINE_TABLE_NAME));
        sql.append(" CASCADE; ");

        try {
            sql.forceExecute();
        } catch (SQLException e) {
            System.out.println("cannot drop line table - ok");
        }

        sql.append("CREATE TABLE ");
        sql.append(DB.getFullTableName(this.targetParameter.getSchema() ,LINE_TABLE_NAME));
        sql.append("(");
        sql.append("osm_id bigint,");
        sql.append("access text COLLATE pg_catalog.\"default\",");
        sql.append("\"addr:housename\" text COLLATE pg_catalog.\"default\",");
        sql.append("\"addr:housenumber\" text COLLATE pg_catalog.\"default\",");
        sql.append("\"addr:interpolation\" text COLLATE pg_catalog.\"default\",");
        sql.append("admin_level text COLLATE pg_catalog.\"default\",");
        sql.append("aerialway text COLLATE pg_catalog.\"default\",");
        sql.append("aeroway text COLLATE pg_catalog.\"default\",");
        sql.append("amenity text COLLATE pg_catalog.\"default\",");
        sql.append("barrier text COLLATE pg_catalog.\"default\",");
        sql.append("bicycle text COLLATE pg_catalog.\"default\",");
        sql.append("bridge text COLLATE pg_catalog.\"default\",");
        sql.append("boundary text COLLATE pg_catalog.\"default\",");
        sql.append("building text COLLATE pg_catalog.\"default\",");
        sql.append("construction text COLLATE pg_catalog.\"default\",");
        sql.append("covered text COLLATE pg_catalog.\"default\",");
        sql.append("foot text COLLATE pg_catalog.\"default\",");
        sql.append("highway text COLLATE pg_catalog.\"default\",");
        sql.append("historic text COLLATE pg_catalog.\"default\",");
        sql.append("horse text COLLATE pg_catalog.\"default\",");
        sql.append("junction text COLLATE pg_catalog.\"default\",");
        sql.append("landuse text COLLATE pg_catalog.\"default\",");
        sql.append("layer text COLLATE pg_catalog.\"default\",");
        sql.append("leisure text COLLATE pg_catalog.\"default\",");
        sql.append("lock text COLLATE pg_catalog.\"default\",");
        sql.append("man_made text COLLATE pg_catalog.\"default\",");
        sql.append("military text COLLATE pg_catalog.\"default\",");
        sql.append("name text COLLATE pg_catalog.\"default\",");
        sql.append("\"natural\" text COLLATE pg_catalog.\"default\",");
        sql.append("oneway text COLLATE pg_catalog.\"default\",");
        sql.append("place text COLLATE pg_catalog.\"default\",");
        sql.append("power text COLLATE pg_catalog.\"default\",");
        sql.append("railway text COLLATE pg_catalog.\"default\",");
        sql.append("ref text COLLATE pg_catalog.\"default\",");
        sql.append("religion text COLLATE pg_catalog.\"default\",");
        sql.append("route text COLLATE pg_catalog.\"default\",");
        sql.append("service text COLLATE pg_catalog.\"default\",");
        sql.append("shop text COLLATE pg_catalog.\"default\",");
        sql.append("surface text COLLATE pg_catalog.\"default\",");
        sql.append("tourism text COLLATE pg_catalog.\"default\",");
        sql.append("tracktype text COLLATE pg_catalog.\"default\",");
        sql.append("tunnel text COLLATE pg_catalog.\"default\",");
        sql.append("water text COLLATE pg_catalog.\"default\",");
        sql.append("waterway text COLLATE pg_catalog.\"default\",");
        sql.append("z_order integer,");
        sql.append("tags hstore,");
        sql.append("way geometry(LineString,3857),");
        sql.append("valid_since date NOT NULL,");
        sql.append("valid_until date NOT NULL)");

        sql.forceExecute();
    }

    void setupPointTable() throws SQLException {
        sql.append("DROP TABLE ");
        sql.append(DB.getFullTableName(this.targetParameter.getSchema() , POINT_TABLE_NAME));
        sql.append(" CASCADE; ");

        try {
            sql.forceExecute();
        } catch (SQLException e) {
            System.out.println("cannot drop points table - ok");
        }

        sql.append("CREATE TABLE ");
        sql.append(DB.getFullTableName(this.targetParameter.getSchema() ,POINT_TABLE_NAME));
        sql.append("(");
        sql.append("osm_id bigint,");
        sql.append("access text COLLATE pg_catalog.\"default\",");
        sql.append("\"addr:housename\" text COLLATE pg_catalog.\"default\",");
        sql.append("\"addr:housenumber\" text COLLATE pg_catalog.\"default\",");
        sql.append("\"addr:interpolation\" text COLLATE pg_catalog.\"default\",");
        sql.append("admin_level text COLLATE pg_catalog.\"default\",");
        sql.append("aerialway text COLLATE pg_catalog.\"default\",");
        sql.append("aeroway text COLLATE pg_catalog.\"default\",");
        sql.append("amenity text COLLATE pg_catalog.\"default\",");
        sql.append("area text COLLATE pg_catalog.\"default\",");
        sql.append("barrier text COLLATE pg_catalog.\"default\",");
        sql.append("bicycle text COLLATE pg_catalog.\"default\",");
        sql.append("brand text COLLATE pg_catalog.\"default\",");
        sql.append("bridge text COLLATE pg_catalog.\"default\",");
        sql.append("boundary text COLLATE pg_catalog.\"default\",");
        sql.append("building text COLLATE pg_catalog.\"default\",");
        sql.append("capital text COLLATE pg_catalog.\"default\",");
        sql.append("construction text COLLATE pg_catalog.\"default\",");
        sql.append("covered text COLLATE pg_catalog.\"default\",");
        sql.append("culvert text COLLATE pg_catalog.\"default\",");
        sql.append("cutting text COLLATE pg_catalog.\"default\",");
        sql.append("denomination text COLLATE pg_catalog.\"default\",");
        sql.append("disused text COLLATE pg_catalog.\"default\",");
        sql.append("ele text COLLATE pg_catalog.\"default\",");
        sql.append("embankment text COLLATE pg_catalog.\"default\",");
        sql.append("foot text COLLATE pg_catalog.\"default\",");
        sql.append("\"generator:source\" text COLLATE pg_catalog.\"default\",");
        sql.append("harbour text COLLATE pg_catalog.\"default\",");
        sql.append("highway text COLLATE pg_catalog.\"default\",");
        sql.append("historic text COLLATE pg_catalog.\"default\",");
        sql.append("horse text COLLATE pg_catalog.\"default\",");
        sql.append("intermittent text COLLATE pg_catalog.\"default\",");
        sql.append("junction text COLLATE pg_catalog.\"default\",");
        sql.append("landuse text COLLATE pg_catalog.\"default\",");
        sql.append("layer text COLLATE pg_catalog.\"default\",");
        sql.append("leisure text COLLATE pg_catalog.\"default\",");
        sql.append("lock text COLLATE pg_catalog.\"default\",");
        sql.append("man_made text COLLATE pg_catalog.\"default\",");
        sql.append("military text COLLATE pg_catalog.\"default\",");
        sql.append("motorcar text COLLATE pg_catalog.\"default\",");
        sql.append("name text COLLATE pg_catalog.\"default\",");
        sql.append("\"natural\" text COLLATE pg_catalog.\"default\",");
        sql.append("office text COLLATE pg_catalog.\"default\",");
        sql.append("oneway text COLLATE pg_catalog.\"default\",");
        sql.append("operator text COLLATE pg_catalog.\"default\",");
        sql.append("place text COLLATE pg_catalog.\"default\",");
        sql.append("poi text COLLATE pg_catalog.\"default\",");
        sql.append("population text COLLATE pg_catalog.\"default\",");
        sql.append("power text COLLATE pg_catalog.\"default\",");
        sql.append("power_source text COLLATE pg_catalog.\"default\",");
        sql.append("public_transport text COLLATE pg_catalog.\"default\",");
        sql.append("railway text COLLATE pg_catalog.\"default\",");
        sql.append("ref text COLLATE pg_catalog.\"default\",");
        sql.append("religion text COLLATE pg_catalog.\"default\",");
        sql.append("route text COLLATE pg_catalog.\"default\",");
        sql.append("service text COLLATE pg_catalog.\"default\",");
        sql.append("shop text COLLATE pg_catalog.\"default\",");
        sql.append("sport text COLLATE pg_catalog.\"default\",");
        sql.append("surface text COLLATE pg_catalog.\"default\",");
        sql.append("toll text COLLATE pg_catalog.\"default\",");
        sql.append("tourism text COLLATE pg_catalog.\"default\",");
        sql.append("\"tower:type\" text COLLATE pg_catalog.\"default\",");
        sql.append("tunnel text COLLATE pg_catalog.\"default\",");
        sql.append("water text COLLATE pg_catalog.\"default\",");
        sql.append("waterway text COLLATE pg_catalog.\"default\",");
        sql.append("wetland text COLLATE pg_catalog.\"default\",");
        sql.append("width text COLLATE pg_catalog.\"default\",");
        sql.append("wood text COLLATE pg_catalog.\"default\",");
        sql.append("z_order integer,");
        sql.append("way geometry(Point,900913),");
        sql.append("valid_since date NOT NULL,");
        sql.append("valid_until date NOT NULL)");

        sql.forceExecute();
    }

    String ohdmClass2mapnikColumn(String className) {
        className = className.trim();
        if(className.equalsIgnoreCase("ohdm_boundary")) {
            return "admin_level";
        } else if(className.equalsIgnoreCase("man")) {
            return "man_made";
        } else if(className.equalsIgnoreCase("natural")) {
            return "\"natural\"";
        }

        // default - nothing to convert
        else {
            return className;
        }
    }

    String ohdmSubClassName2mapnikColumnValue(String className, String subClassName) {
        className = className.trim();
        subClassName = subClassName.trim();
        if(className.equalsIgnoreCase("ohdm_boundary")) {
            int index = subClassName.indexOf("_");
            return subClassName.substring(index+1);
        } else {
            return subClassName;
        }
    }

    public static void main(String[] args) throws IOException, SQLException {
        String DEFAULT_OHDM_PARAMETER_FILE = "db_convert_source.txt";
        String DEFAULT_OSM_PARAMETER_FILE = "db_convert_target.txt";

        String ohdmParamterFileName = DEFAULT_OHDM_PARAMETER_FILE;
        String mapnikParamterFileName = DEFAULT_OSM_PARAMETER_FILE;

        if(args.length > 0) {
            ohdmParamterFileName = args[0];
        }

        if(args.length > 1) {
            mapnikParamterFileName = args[1];
        }

        Parameter source = new Parameter(ohdmParamterFileName);
        Parameter target = new Parameter(mapnikParamterFileName);

        System.out.println("converting OHDM rendering tables to OSM rendering tables");
        System.out.println("use ohdm parameters from: " + ohdmParamterFileName);
        System.out.println("use osm parameters from: " + mapnikParamterFileName);

        OSMClassification osmC = OSMClassification.getOSMClassification();
        List<String> nodeTables = osmC.getGenericTableNames(OHDM_DB.OHDM_POINT_GEOMTYPE);
        List<String> linesTables = osmC.getGenericTableNames(OHDM_DB.OHDM_LINESTRING_GEOMTYPE);
        List<String> polygonTables = osmC.getGenericTableNames(OHDM_DB.OHDM_POLYGON_GEOMTYPE);


        OHDMRendering2MapnikTables converter = new OHDMRendering2MapnikTables(source, target);

        converter.convert(nodeTables, linesTables, polygonTables);
    }
}
