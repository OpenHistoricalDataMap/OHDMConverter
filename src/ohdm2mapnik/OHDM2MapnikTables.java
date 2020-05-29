package ohdm2mapnik;

import ohdm2mapnik.GeoObject.Line;
import ohdm2mapnik.GeoObject.Point;
import ohdm2mapnik.GeoObject.Polygon;
import util.DB;
import util.Parameter;
import util.SQLStatementQueue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OHDM2MapnikTables {

    // chunk size to import at once
    private static int chunkSize = 10000;

    public static void main(String[] args) throws SQLException, IOException {
        String sourceParameterFileName = "db_ohdm.txt";
        String targetParameterFileName = "db_mapnik.txt";

        if (args.length > 0) {
            sourceParameterFileName = args[0];
        }

        if (args.length > 1) {
            targetParameterFileName = args[1];
        }

        Parameter sourceParameter = new Parameter(sourceParameterFileName);
        Parameter targetParameter = new Parameter(targetParameterFileName);

        Connection connection = DB.createConnection(targetParameter);

        String targetSchema = targetParameter.getSchema();

        String sourceSchema = sourceParameter.getSchema();

        SQLStatementQueue sql = new SQLStatementQueue(connection);
        OHDM2MapnikTables mapnikTables = new OHDM2MapnikTables();

        mapnikTables.setupMapnikDB(sql, targetSchema);

        mapnikTables.convertDatabase(sql, targetSchema, sourceSchema);

        System.out.println("Mapnik tables creation finished");
    }

    /**
     * fill cache tables for the mapnik db with ohdm data
     *
     * @param sql
     * @param sourceSchema
     */
    void fillCacheMapnikTables(SQLStatementQueue sql, String targetSchema, String sourceSchema) {
        System.out.println("Start to fill the mapnik cache tables, this could take some hours...");

        try {
            sql.forceExecute();
        } catch (SQLException e) {
            System.err.println("exception ignored: " + e);
        }

        // points
        sql.append("INSERT INTO " + targetSchema + ".ohdm_points (geoobject_id, name, classification_class, classification_subclassname, tags, valid_since, valid_until, way) " +
                "            SELECT " +
                "                geoobject_geometry.id_geoobject_source as geoobject_id, " +
                "                geoobject.name, " +
                "                classification.class as classification_class, " +
                "                classification.subclassname as classification_subclassname, " +
                "                geoobject_geometry.tags, " +
                "                geoobject_geometry.valid_since, " +
                "                geoobject_geometry.valid_until, " +
                "                ST_Transform(points.point, 3857) as way " +
                "            FROM " + sourceSchema + ".geoobject_geometry " +
                "            JOIN  " + sourceSchema + ".points ON geoobject_geometry.id_target=points.id " +
                "            JOIN  " + sourceSchema + ".geoobject ON geoobject_geometry.id_geoobject_source=geoobject.id " +
                "            JOIN  " + sourceSchema + ".classification ON geoobject_geometry.classification_id=classification.id " +
                "            WHERE geoobject_geometry.type_target = 0 or geoobject_geometry.type_target = 1;");

        System.out.println("start filling points ...");
        try {
            sql.forceExecute();
        } catch (SQLException e) {
            System.err.println("exception ignored: " + e);
        }

        // lines
        sql.append("INSERT INTO " + targetSchema + ".ohdm_lines (geoobject_id, name, classification_class, classification_subclassname, tags, valid_since, valid_until, way) " +
                "            SELECT " +
                "                geoobject_geometry.id_geoobject_source as geoobject_id, " +
                "                geoobject.name, " +
                "                classification.class as classification_class, " +
                "                classification.subclassname as classification_subclassname, " +
                "                geoobject_geometry.tags, " +
                "                geoobject_geometry.valid_since, " +
                "                geoobject_geometry.valid_until, " +
                "                ST_Transform(lines.line, 3857) as way " +
                "            FROM  " + sourceSchema + ".geoobject_geometry " +
                "            JOIN  " + sourceSchema + ".lines ON geoobject_geometry.id_target=lines.id " +
                "            JOIN  " + sourceSchema + ".geoobject ON geoobject_geometry.id_geoobject_source=geoobject.id " +
                "            JOIN  " + sourceSchema + ".classification ON geoobject_geometry.classification_id=classification.id " +
                "            WHERE geoobject_geometry.type_target = 2;");

        System.out.println("start filling lines ...");
        try {
            sql.forceExecute();
        } catch (SQLException e) {
            System.err.println("exception ignored: " + e);
        }

        // polygons
        sql.append("INSERT INTO " + targetSchema + ".ohdm_polygons (geoobject_id, name, classification_class, classification_subclassname, tags, valid_since, valid_until, way, way_area) " +
                "            SELECT " +
                "                geoobject_geometry.id_geoobject_source as geoobject_id, " +
                "                geoobject.name, " +
                "                classification.class as classification_class, " +
                "                classification.subclassname as classification_subclassname, " +
                "                geoobject_geometry.tags, " +
                "                geoobject_geometry.valid_since, " +
                "                geoobject_geometry.valid_until, " +
                "                ST_MakeValid(ST_Transform(polygons.polygon, 3857)) as way, " +
                "                ST_Area(ST_MakeValid(ST_Transform(polygons.polygon, 3857))) as way_area " +
                "            FROM  " + sourceSchema + ".geoobject_geometry " +
                "            JOIN  " + sourceSchema + ".polygons ON geoobject_geometry.id_target=polygons.id " +
                "            JOIN  " + sourceSchema + ".geoobject ON geoobject_geometry.id_geoobject_source=geoobject.id " +
                "            JOIN  " + sourceSchema + ".classification ON geoobject_geometry.classification_id=classification.id " +
                "            WHERE geoobject_geometry.type_target = 3;");

        System.out.println("start filling polygons ...");
        try {
            sql.forceExecute();
        } catch (SQLException e) {
            System.err.println("exception ignored: " + e);
        }

        System.out.println("Mapnik cache tables filles!");
    }

    /**
     * delete cache mapnik tables
     *
     * @param sql
     * @param targetSchema
     */
    void clearCacheMapnikTables(SQLStatementQueue sql, String targetSchema) {
        // cache points
        sql.append("DELETE FROM " + targetSchema + ".ohdm_points; ");

        // cache lines
        sql.append("DELETE FROM " + targetSchema + ".ohdm_lines; ");

        // cache polygons
        sql.append("DELETE FROM " + targetSchema + ".ohdm_polygons; ");

        try {
            sql.forceExecute();
        } catch (SQLException e) {
            System.err.println("exception: " + e);
            System.err.println("Planet osm tables are missing, please create the table with MaptikTileServer first!");
            System.exit(1);
        }

        System.out.println("clear up cache mapnik tables");
    }


    /**
     * check if the mapnik tables exist on the database & clear up old data
     *
     * @param sql
     * @param targetSchema
     */
    void setupMapnikDB(SQLStatementQueue sql, String targetSchema) {
        // points
        sql.append("DELETE FROM " + targetSchema + ".planet_osm_point; ");

        // lines
        sql.append("DELETE FROM " + targetSchema + ".planet_osm_line; ");

        // roads
        sql.append("DELETE FROM " + targetSchema + ".planet_osm_roads; ");

        // polygons
        sql.append("DELETE FROM " + targetSchema + ".planet_osm_polygon; ");

        try {
            sql.forceExecute();
        } catch (SQLException e) {
            System.err.println("exception: " + e);
            System.err.println("Planet osm tables are missing, please create the table with MaptikTileServer first!");
            System.exit(1);
        }

        System.out.println("Mapnik tables exists, continue ...");

        this.clearCacheMapnikTables(sql, targetSchema);
    }

    /**
     * Count rows to convert
     *
     * @param sql
     * @param sourceSchema
     * @param table        which should be counted
     * @return
     * @throws SQLException
     */
    private int countRows(SQLStatementQueue sql, String sourceSchema, String table) throws SQLException {
        sql.append("SELECT COUNT(*) FROM " + sourceSchema + "." + table + ";");

        ResultSet result = null;
        try {
            result = sql.executeWithResult();
        } catch (SQLException e) {
            System.err.println("exception ignored: " + e);
        }

        while (result.next()) {
            return result.getInt(1); // counted rows
        }

        return 0;
    }

    /**
     * Show the current import status
     *
     * @param currentRow
     * @param maxRow
     * @param geometry
     */
    void showImportStatus(int currentRow, int maxRow, String geometry) {
        if (currentRow % 10000 == 0) {
            System.out.println(geometry + ": " + currentRow + " of " + maxRow);
        }
    }

    void convertDatabase(SQLStatementQueue sql, String targetSchema, String sourceSchema) throws SQLException {
        // fill cache tables with ohdm data
        this.fillCacheMapnikTables(sql, targetSchema, sourceSchema);

        // count rows
        System.out.println("Count points rows ...");
        int pointsRows = this.countRows(sql, targetSchema, "ohdm_points");
        System.out.println("Count lines rows ...");
        int linesRows = this.countRows(sql, targetSchema, "ohdm_lines");
        System.out.println("Count polygons rows ...");
        int polygonsRows = this.countRows(sql, targetSchema, "ohdm_polygons");

        // start import
        this.convertPoints(sql, targetSchema, pointsRows);
        this.convertLines(sql, targetSchema, linesRows);
        this.convertPolygons(sql, targetSchema, polygonsRows);

        // delete cache tables
        this.clearCacheMapnikTables(sql, targetSchema);
    }


    /**
     * Convert point cache mapnik tables to real mapnik tables
     *
     * @param sql
     * @param targetSchema
     * @param maxRows
     * @throws SQLException
     */
    void convertPoints(SQLStatementQueue sql, String targetSchema, int maxRows) throws SQLException {
        System.out.println("Start import points!");

        for (int i = 0; i <= maxRows; i += chunkSize) {
            sql.append("SELECT id, geoobject_id, name, classification_class, classification_subclassname, tags, valid_since, valid_until, ST_TRANSFORM(way, 3857) as way " +
                    "FROM " + targetSchema + ".ohdm_points " +
                    "ORDER BY id LIMIT " + chunkSize + " OFFSET " + i + ";");
            ResultSet result = null;
            try {
                result = sql.executeWithResult();
            } catch (SQLException e) {
                System.err.println("exception ignored: " + e);
            }

            Point point;
            int currentRow = i;

            while (result.next()) {
                point = new Point(
                        result.getLong(1), // id
                        result.getLong(2), // geoobjectId
                        result.getString(3), // name
                        result.getString(4), // classificationClass
                        result.getString(5), // classificationSubclassname
                        result.getString(6), // tags
                        result.getDate(7), // validSince
                        result.getDate(8), // validUntil
                        result.getString(9) // way
                );

                sql.append(point.getMapnikQuery(targetSchema));

                currentRow++;
                this.showImportStatus(currentRow, maxRows, "point");
            }

            System.out.println("Upload to database...");
            try {
                sql.forceExecute();
            } catch (SQLException e) {
                System.err.println("exception ignored: " + e);
            }
        }
        System.out.println("Points import are completed!");
    }

    /**
     * Convert line cache mapnik tables to real mapnik tables
     *
     * @param sql
     * @param targetSchema
     * @param maxRows
     * @throws SQLException
     */
    void convertLines(SQLStatementQueue sql, String targetSchema, int maxRows) throws SQLException {
        System.out.println("Start import lines!");

        for (int i = 0; i <= maxRows; i += chunkSize) {
            sql.append("SELECT id, geoobject_id, name, classification_class, classification_subclassname, tags, valid_since, valid_until, ST_TRANSFORM(way, 3857) as way " +
                    "FROM " + targetSchema + ".ohdm_lines " +
                    "ORDER BY id LIMIT " + chunkSize + " OFFSET " + i + ";");

            ResultSet result = null;
            try {
                result = sql.executeWithResult();
            } catch (SQLException e) {
                System.err.println("exception ignored: " + e);
            }

            Line line;
            int currentRow = i;

            while (result.next()) {
                line = new Line(
                        result.getLong(1), // id
                        result.getLong(2), // geoobjectId
                        result.getString(3), // name
                        result.getString(4), // classificationClass
                        result.getString(5), // classificationSubclassname
                        result.getString(6), // tags
                        result.getDate(7), // validSince
                        result.getDate(8), // validUntil
                        result.getString(9) // way
                );

                sql.append(line.getMapnikQuery(targetSchema));

                currentRow++;
                this.showImportStatus(currentRow, maxRows, "line");
            }

            System.out.println("Upload to database...");
            try {
                sql.forceExecute();
            } catch (SQLException e) {
                System.err.println("exception ignored: " + e);
            }
        }

        System.out.println("Lines import are completed!");
    }

    /**
     * Convert point cache mapnik tables to real mapnik tables
     *
     * @param sql
     * @param targetSchema
     * @param maxRows
     * @throws SQLException
     */
    void convertPolygons(SQLStatementQueue sql, String targetSchema, int maxRows) throws SQLException {
        System.out.println("Start import polygons!");

        for (int i = 0; i <= maxRows; i += chunkSize) {
            sql.append("SELECT id, geoobject_id, name, classification_class, classification_subclassname, tags, valid_since, valid_until, way_area, ST_TRANSFORM(way, 3857) as way " +
                    "FROM " + targetSchema + ".ohdm_polygons " +
                    "ORDER BY id LIMIT " + chunkSize + " OFFSET " + i + ";");

            ResultSet result = null;
            try {
                result = sql.executeWithResult();
            } catch (SQLException e) {
                System.err.println("exception ignored: " + e);
            }

            Polygon polygon;
            int currentRow = i;

            while (result.next()) {
                polygon = new Polygon(
                        result.getLong(1), // id
                        result.getLong(2), // geoobjectId
                        result.getString(3), // name
                        result.getString(4), // classificationClass
                        result.getString(5), // classificationSubclassname
                        result.getString(6), // tags
                        result.getDate(7), // validSince
                        result.getDate(8), // validUntil
                        result.getDouble(9), // way_area
                        result.getString(10) // way
                );

                sql.append(polygon.getMapnikQuery(targetSchema));

                currentRow++;
                this.showImportStatus(currentRow, maxRows, "polygon");
            }

            System.out.println("Upload to database...");
            try {
                sql.forceExecute();
            } catch (SQLException e) {
                System.err.println("exception ignored: " + e);
            }
        }

        System.out.println("Polygons import are completed!");
    }

}
