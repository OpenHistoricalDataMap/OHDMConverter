package ohdm2osm;

import osm.OSMClassification;
import util.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

        // debugging
        System.out.println("***************************************************************************************");
        System.out.println("*                                         POINTS DONE                                **");
        System.out.println("***************************************************************************************");

        this.setupLinesTable();
        this.convertLines(linesTables);

        // debugging
        System.out.println("***************************************************************************************");
        System.out.println("*                                    LINES DONE                                      **");
        System.out.println("***************************************************************************************");

        this.setupPolygonsTable();
        this.convertPolygons(polygonTables);

        // debugging
        System.out.println("***************************************************************************************");
        System.out.println("*                                 POLYGONS DONE                                      **");
        System.out.println("***************************************************************************************");
    }

    private void convertPoints(List<String> pointTableNames) {
        if(pointTableNames != null) {
            for(String tableName : pointTableNames) {
                System.out.println("converting " + tableName);

/*
INSERT INTO mapniktest.planet_osm_point (way, osm_id, name, amenity, valid_since, valid_until)
(SELECT point, geom_id, name, subclassname, valid_since, valid_until FROM public.amenity_points)
 */

                sql.append("INSERT INTO ");
                sql.append(util.DB.getFullTableName(this.targetParameter.getSchema(), POINT_TABLE_NAME));
                sql.append(" (way, osm_id, name, ");
                sql.append(this.ohdmRenderingTableName2mapnikColumnName(tableName));
                sql.append(", valid_since, valid_until, tags)");
                sql.append(" (SELECT point, geom_id, name, subclassname, valid_since, valid_until, tags FROM ");
                sql.append(util.DB.getFullTableName(this.sourceParameter.getSchema(), tableName));
                sql.append(");");

                try {
                    sql.forceExecute();
                } catch (SQLException e) {
                    Util.printExceptionMessage(e, sql, "error during mapnik conversion", true);
                }
            }
        }
    }

    // FYI: https://www.postgresql.org/docs/9.1/hstore.html
    private void convertLines(List<String> lineTableNames) throws SQLException {
        if(lineTableNames != null) {
            for (String tableName : lineTableNames) {
                System.out.println("converting " + tableName);
/*
INSERT INTO mapniktest.planet_osm_line (way, osm_id, name, amenity, valid_since, valid_until)
(SELECT line, geom_id, name, subclassname, valid_since, valid_until FROM public.amenity_lines)
*/
                sql.append("INSERT INTO ");
                sql.append(util.DB.getFullTableName(this.targetParameter.getSchema(), LINE_TABLE_NAME));
                sql.append(" (way, osm_id, name, ");
                sql.append(this.ohdmRenderingTableName2mapnikColumnName(tableName));
                sql.append(", valid_since, valid_until, tags)");
                sql.append(" (SELECT line, geom_id, name, subclassname, valid_since, valid_until, tags FROM ");
                sql.append(util.DB.getFullTableName(this.sourceParameter.getSchema(), tableName));
                sql.append(");");

                try {
                    sql.forceExecute();
                } catch (SQLException e) {
                    Util.printExceptionMessage(e, sql, "error during mapnik conversion", true);
                }
            }
        }
    }

    private void convertPolygons(List<String> polygonTableNames) throws SQLException {
        if(polygonTableNames != null) {
            for(String tableName : polygonTableNames) {
                System.out.println("converting " + tableName);

/*
INSERT INTO mapniktest.planet_osm_polygon (way, osm_id, name, amenity, valid_since, valid_until)
(SELECT polygon, geom_id, name, subclassname, valid_since, valid_until FROM public.amenity_polygons)
 */
                sql.append("INSERT INTO ");
                sql.append(util.DB.getFullTableName(this.targetParameter.getSchema(), POLYGON_TABLE_NAME));
                sql.append(" (way, osm_id, name, ");
                sql.append(this.ohdmRenderingTableName2mapnikColumnName(tableName));
                sql.append(", valid_since, valid_until, tags)");
                sql.append(" (SELECT polygon, geom_id, name, subclassname, valid_since, valid_until, tags FROM ");
                sql.append(util.DB.getFullTableName(this.sourceParameter.getSchema(), tableName));
                sql.append(");");

                try {
                    sql.forceExecute();
                } catch (SQLException e) {
                    Util.printExceptionMessage(e, sql, "error during mapnik conversion", true);
                }
            }
        }
    }

    public static final String POINT_TABLE_NAME = "planet_osm_point";
    public static final String LINE_TABLE_NAME = "planet_osm_line";
    public static final String POLYGON_TABLE_NAME = "planet_osm_polygon";

    void setupPolygonsTable() throws SQLException {
        sql.append("DROP TABLE ");
        sql.append(DB.getFullTableName(this.targetParameter.getSchema() , POLYGON_TABLE_NAME));
        sql.append(" CASCADE; ");

        try {
            sql.forceExecute();
        } catch (SQLException e) {
            System.out.println("cannot drop line table - ok");
        }

        sql.append("CREATE TABLE ");
        sql.append(DB.getFullTableName(this.targetParameter.getSchema() ,POLYGON_TABLE_NAME));
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
        sql.append("layer integer,");
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
        sql.append("way_area real,");
        sql.append("z_order integer,");
        sql.append("tags hstore,");
        sql.append("way geometry(Geometry,3857),");
        sql.append("valid_since date NOT NULL,");
        sql.append("valid_until date NOT NULL)");

        sql.forceExecute();
    }

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
        sql.append("layer integer,");
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
        sql.append("way_area real,");
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
        sql.append("admin_level text COLLATE pg_catalog.\"default\",");
        sql.append("aerialway text COLLATE pg_catalog.\"default\",");
        sql.append("aeroway text COLLATE pg_catalog.\"default\",");
        sql.append("amenity text COLLATE pg_catalog.\"default\",");
        sql.append("barrier text COLLATE pg_catalog.\"default\",");
        sql.append("boundary text COLLATE pg_catalog.\"default\",");
        sql.append("building text COLLATE pg_catalog.\"default\",");
        sql.append("highway text COLLATE pg_catalog.\"default\",");
        sql.append("historic text COLLATE pg_catalog.\"default\",");
        sql.append("junction text COLLATE pg_catalog.\"default\",");
        sql.append("landuse text COLLATE pg_catalog.\"default\",");
        sql.append("layer integer,");
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
        sql.append("shop text COLLATE pg_catalog.\"default\",");
        sql.append("tourism text COLLATE pg_catalog.\"default\",");
        sql.append("water text COLLATE pg_catalog.\"default\",");
        sql.append("waterway text COLLATE pg_catalog.\"default\",");
        sql.append("tags hstore,");
        sql.append("way geometry(Point,3857),");
        sql.append("valid_since date NOT NULL,");
        sql.append("valid_until date NOT NULL)");

        sql.forceExecute();
    }

    String ohdmRenderingTableName2mapnikColumnName(String tableName) {
        String className = OSMClassification.getOSMClassification().getClassNameByFullName(tableName);
        if(
                className.equalsIgnoreCase("ohdm_boundary")
                || className.equalsIgnoreCase("boundary")
        ) {
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

    private static final String DEFAULT_RENDERING_PARAMETER_FILE = "db_rendering.txt";
    private static final String DEFAULT_MAPNIK_PARAMETER_FILE = "db_mapnik.txt";

    public static void main(String[] args) throws IOException, SQLException {

        String renderingParamterFileName = DEFAULT_RENDERING_PARAMETER_FILE;
        String mapnikParamterFileName = DEFAULT_MAPNIK_PARAMETER_FILE;

        if(args.length > 0) {
            renderingParamterFileName = args[0];
        }

        if(args.length > 1) {
            mapnikParamterFileName = args[1];
        }

        Parameter source = new Parameter(renderingParamterFileName);
        Parameter target = new Parameter(mapnikParamterFileName);

        System.out.println("converting OHDM rendering tables to Mapnik rendering tables");
        System.out.println("use OHDM rendering parameters from: " + renderingParamterFileName);
        System.out.println("use mapnik parameters from: " + mapnikParamterFileName);

        OSMClassification osmC = OSMClassification.getOSMClassification();
        List<String> nodeTables = osmC.getGenericTableNames(OHDM_DB.OHDM_POINT_GEOMTYPE);
        List<String> linesTables = osmC.getGenericTableNames(OHDM_DB.OHDM_LINESTRING_GEOMTYPE);
        List<String> polygonTables = osmC.getGenericTableNames(OHDM_DB.OHDM_POLYGON_GEOMTYPE);


        OHDMRendering2MapnikTables converter = new OHDMRendering2MapnikTables(source, target);

        System.err.println("TODO: that converter does not understand all OHDM rendering tables!!!");
        converter.convert(
                cleanList(nodeTables),
                cleanList(linesTables),
                cleanList(polygonTables)
        );
        System.err.println("TODO: that converter does not understand all OHDM rendering tables!!!");
    }

    /**
     * remove all tables which cause problems. - just for a pre-release TODO
     * @param tableNames
     * @return
     */
    private static List<String> cleanList(List<String> tableNames) {
        List<String> cleaned = new ArrayList<>();

        for(String tableName : tableNames) {
            if(tableName.startsWith("ohdm")
                    || tableName.startsWith("emergency")
                    || tableName.startsWith("public")
                    || tableName.startsWith("sport")
                    || tableName.startsWith("office")
            ) continue;

            cleaned.add(tableName);
        }

        return cleaned;
    }
}
