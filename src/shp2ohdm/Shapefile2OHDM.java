package shp2ohdm;

import util.DB;
import util.OHDM_DB;
import util.Parameter;
import util.SQLStatementQueue;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Shapefile2OHDM {
    private final Parameter targetParameter;
    private final Parameter importParameter;
    private int lfd = 0;
    private BufferedWriter out;

    public static void main(String[] args) throws IOException, SQLException {
        String targetParameterFileName = "db_ohdm_historic_local.txt";
        String importParameterFileName = "db_shape_import.txt";

        if(args.length > 0) {
            importParameterFileName = args[0];
        } else if(args.length > 1) {
            targetParameterFileName = args[1];
        }  if(args.length > 2) {
            System.err.println("at most two parameter required: shape-import description and ohdm target description");
            System.exit(1);
        }

        System.out.println("use " + importParameterFileName + " as shapefile description");
        System.out.println("use " + targetParameterFileName + " as ohdm db description");

        Shapefile2OHDM shape2OHDM = new Shapefile2OHDM(
                new Parameter(importParameterFileName),
                new Parameter(targetParameterFileName)
        );

        shape2OHDM.importShapefile();

    }

    public Shapefile2OHDM(Parameter importParameter, Parameter targetParameter)
            throws IOException, SQLException {
        this.targetParameter = targetParameter;
        this.importParameter = importParameter;
    }

    // TODO

    /**
     * Let's assume a shapefile contains a description of one or more object with probably sub-objects, like a
     * country with provinces.
     *
     * Each row would describe a sub-object its geometry. (We can ignore the fact that a row could describe more
     * geometries. We simply repeat the following algorithm.) What we need column names for
     * <ul><li>geometry</li><li>object name</li></ul>
     *
     * We would also take optional additional column names that describe the object more specific.
     *
     * We need column names for valid-since / until or a general parameter if one or both values are not present in this
     * table. Further optional parameters:
     * <ul><li>importer user name (who hold rights)</li><li>object name</li></ul>
     *
     * @throws IOException
     * @throws SQLException
     */
    void importShapefile() throws IOException, SQLException {
        // just to be sure - create ohdm db if not yet done
        Connection connection = DB.createConnection(targetParameter);

        try {
            util.OHDM_DB.createOHDMTables(connection, targetParameter.getSchema());
        }
        catch(SQLException e) {
            System.out.println("catch and ignore sql exception - db most probably already exists: "
                    + e.getLocalizedMessage());
        }
        connection.close();

        SQLStatementQueue sourceQueue = new SQLStatementQueue(this.importParameter);

        // find primary key
        String pkColumnName = null;

        /*
SELECT
     tc.TABLE_SCHEMA
    ,tc.TABLE_NAME
    ,tc.CONSTRAINT_NAME
    ,kcu.COLUMN_NAME
    ,kcu.ORDINAL_POSITION
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc

INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
    ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME

WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
AND tc.TABLE_NAME = 'preussen_1830'
         */

        sourceQueue.append("SELECT kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc " +
                "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu\n" +
                "    ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME\n" +
                "WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'\n" +
                "AND tc.TABLE_NAME = '");

        sourceQueue.append(importParameter.getTableName());
        sourceQueue.append("'");

        ResultSet resultSet = sourceQueue.executeWithResult();
        if(resultSet.next()) {
            // got primary key column name
            pkColumnName = resultSet.getString(1);
        } else {
            // no pk - fatal
            System.err.println("import table has no primary key - fatal in this version");
            throw new SQLException("no primary key found");
        }

        List<ImportRow> importRows = new ArrayList<>();

        String columnNameObjectName = this.importParameter.getColumnNameObjectName();
        String columnNameGeometry = this.importParameter.getColumnNameGeometry();

        // SELECT prov_name, st_asewkt(geom), st_geometrytype(geom) FROM test.preussen_1830;
        sourceQueue.append("select pg_typeof(");
        sourceQueue.append(pkColumnName);
        sourceQueue.append("), ");
        sourceQueue.append(pkColumnName);
        sourceQueue.append(", ");
        sourceQueue.append(columnNameObjectName);
        sourceQueue.append(", st_geometrytype(" + columnNameGeometry + ")");
        sourceQueue.append(" FROM ");
        sourceQueue.append(DB.getFullTableName(importParameter.getSchema(), importParameter.getTableName()));
        resultSet = sourceQueue.executeWithResult();

        int pk;
        String objectName = null;
        String geometryType = null;

        while(resultSet.next()) {
            String pkType = resultSet.getString(1);
            switch(pkType) {
                case "integer": pk = resultSet.getInt(2); break;
                default: throw new SQLException("only integer primary key are supported yet - see shape2ohdm code");
            }
            objectName = resultSet.getString(3);
            geometryType = resultSet.getString(4);

            importRows.add(new ImportRow(pk, objectName, geometryType));
        }

        /*
        select st_asewkt(b.geom) from (SELECT (ST_Dump(geom)).geom AS geom FROM test.preussen_1830 as a) as b
        select * into test.temp from (select st_asewkt(b.geom) from (SELECT (ST_Dump(geom)).geom AS geom FROM test.preussen_1830 as a) as b) as c
         */

        SQLStatementQueue targetQueue = new SQLStatementQueue(this.targetParameter);
    }

    private class ImportRow {
        public final Object pk;
        public final String objectName;
        public final String geomType;

        ImportRow(Object pk, String objectName, String geomType) {
            this.pk = pk;
            this.objectName = objectName;
            this.geomType = geomType;
        }
    }
}
