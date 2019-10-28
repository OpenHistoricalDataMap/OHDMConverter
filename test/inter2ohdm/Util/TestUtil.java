package inter2ohdm.Util;

import util.*;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestUtil {

    public static String geoobject_geometry = OHDM_DB.TABLE_GEOOBJECT_GEOMETRY;
    public static String geoobject = OHDM_DB.TABLE_GEOOBJECT;
    public static String classification = OHDM_DB.TABLE_CLASSIFICATION;

    public static Parameter parameter = null;

    private static SQLStatementQueue insertSql = null;


    public static Date getDate(String name) {
        Date date = null;
        try {
            insertSql.append("select valid_until from ");
            insertSql.append(parameter.getSchema());
            insertSql.append(".");
            insertSql.append(geoobject_geometry);
            insertSql.append(",");
            insertSql.append(",");
            insertSql.append(parameter.getSchema());
            insertSql.append(".");
            insertSql.append(geoobject);
            insertSql.append(" where id_geoobject_source=");
            insertSql.append(parameter.getSchema());
            insertSql.append(".");
            insertSql.append(geoobject);
            insertSql.append(".id and name = '" + name + "';");
            ResultSet resultSet = insertSql.executeWithResult();
            resultSet.next();
            date = resultSet.getDate("valid_until");
            insertSql.flushThreads();
            System.out.println("Date: " + date);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return date;
    }

    public static ResultSet getResultSetByGeoObjectName(String geoobjectName) throws SQLException {
        SQLStatementQueue statementQueue = TestUtil.getInsertSql();
        statementQueue.append("select valid_until");
        statementQueue.append(",");
        statementQueue.append(parameter.getSchema());
        statementQueue.append(".");
        statementQueue.append(geoobject_geometry);
        statementQueue.append(".");
        statementQueue.append("id as geoobj_geom_id, ");
        statementQueue.append(parameter.getSchema());
        statementQueue.append(".");
        statementQueue.append(classification);
        statementQueue.append(".");
        statementQueue.append("id from ");
        statementQueue.append(parameter.getSchema());
        statementQueue.append(".");
        statementQueue.append(geoobject_geometry);
        statementQueue.append(",");
        statementQueue.append(parameter.getSchema());
        statementQueue.append(".");
        statementQueue.append(geoobject);
        statementQueue.append(",");
        statementQueue.append(parameter.getSchema());
        statementQueue.append(".");
        statementQueue.append(classification);
        statementQueue.append(" where id_geoobject_source=");
        statementQueue.append(geoobject);
        statementQueue.append(".id and classification_id =");
        statementQueue.append(classification);
        statementQueue.append(".id and name = '");
        statementQueue.append(geoobjectName);
        statementQueue.append("'");
        return statementQueue.executeWithResult();
    }

    public static Date getDateById(long id) {
        Date date = null;
        try {
            insertSql.append("select valid_until from ");
            insertSql.append(parameter.getSchema());
            insertSql.append(".");
            insertSql.append(geoobject_geometry);
            insertSql.append(" where id = " + id + ";");
            System.out.println(insertSql.toString());
            ResultSet resultSet = insertSql.executeWithResult();
            resultSet.next();
            date = resultSet.getDate("valid_until");
            insertSql.flushThreads();
            System.out.println("Date: " + date);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return date;
    }

    public static void importOsmTestFile(String parameterProperties, String[] args) throws SQLException, IOException {
        parameter = new Parameter(parameterProperties);
        insertSql = new SQLStatementQueue(DB.createConnection(parameter));
        OHDMConverter.main(args);
    }

    public static SQLStatementQueue getInsertSql() {
        return insertSql;
    }

    public static long getIdByName(String name) throws SQLException {
        insertSql.append("select ");
        insertSql.append(parameter.getSchema());
        insertSql.append(".");
        insertSql.append(geoobject_geometry);
        insertSql.append(".");
        insertSql.append("id as geoobj_geom_id from ");
        insertSql.append(parameter.getSchema());
        insertSql.append(".");
        insertSql.append(geoobject_geometry);
        insertSql.append(",");
        insertSql.append(parameter.getSchema());
        insertSql.append(".");
        insertSql.append(geoobject);
        insertSql.append(" where id_geoobject_source=");
        insertSql.append(parameter.getSchema());
        insertSql.append(".");
        insertSql.append(geoobject);
        insertSql.append(".id and name = '");
        insertSql.append(name);
        insertSql.append("';");
        ResultSet resultSet = insertSql.executeWithResult();
        resultSet.next();
        long id = resultSet.getLong("geoobj_geom_id");
        insertSql.flushThreads();
        return id;
    }

    public static boolean setBufferUtilResultSetsForTestMapV1() throws SQLException {
        UpdateBufferUtil.tc19V1 = TestUtil.getResultSetByGeoObjectName("19-TC");
        UpdateBufferUtil.tc20V1 = TestUtil.getResultSetByGeoObjectName("20-TC");
        UpdateBufferUtil.tc21V1 = TestUtil.getResultSetByGeoObjectName("21-TC");
        UpdateBufferUtil.tc22V1 = TestUtil.getResultSetByGeoObjectName("22-TC");
        UpdateBufferUtil.tc23V1 = TestUtil.getResultSetByGeoObjectName("23-TC");
        UpdateBufferUtil.tc24V1 = TestUtil.getResultSetByGeoObjectName("24-TC");
        UpdateBufferUtil.tc25V1 = TestUtil.getResultSetByGeoObjectName("25-TC");
        UpdateBufferUtil.tc26V1 = TestUtil.getResultSetByGeoObjectName("26-TC");
        UpdateBufferUtil.tc27V1 = TestUtil.getResultSetByGeoObjectName("27-TC");
        return true;
    }
    public static boolean setBufferUtilResultSetsForTestMapV2() throws SQLException {
        UpdateBufferUtil.tc19V2 = TestUtil.getResultSetByGeoObjectName("19-TC");
        UpdateBufferUtil.tc20V2 = TestUtil.getResultSetByGeoObjectName("20-TC");
        UpdateBufferUtil.tc21V2 = TestUtil.getResultSetByGeoObjectName("21-TC");
        UpdateBufferUtil.tc22V2 = TestUtil.getResultSetByGeoObjectName("22-TC");
        UpdateBufferUtil.tc23V2 = TestUtil.getResultSetByGeoObjectName("23-TC");
        UpdateBufferUtil.tc24V2 = TestUtil.getResultSetByGeoObjectName("24-TC");
        UpdateBufferUtil.tc25V2 = TestUtil.getResultSetByGeoObjectName("25-TC");
        UpdateBufferUtil.tc26V2 = TestUtil.getResultSetByGeoObjectName("26-TC");
        UpdateBufferUtil.tc27V2 = TestUtil.getResultSetByGeoObjectName("27-TC");
        return true;
    }
}
