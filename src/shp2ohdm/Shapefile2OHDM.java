package shp2ohdm;

import util.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Shapefile2OHDM {
    private final Parameter targetParameter;
    private final Parameter importParameter;
    private SQLStatementQueue sqlQueue;
    private String pkColumnName = null;
    private BigDecimal importerUserID;
    private boolean importerUserIDSet;

    public static void main(String[] args) {
        String importParameterFileName ="db_ohdm_historic_local.txt";  // "C:\\Program Files\\Apache Software Foundation\\Tomcat 9.0\\bin\\configFiles\\db_ohdm_historic_local"; //
        String targetParameterFileName =  "db_shape_import.txt"; // "C:\\Program Files\\Apache Software Foundation\\Tomcat 9.0\\bin\\configFiles\\db_shape_import";//

      if(args.length > 0) {
            importParameterFileName = args[0];
        }
        if(args.length > 1) {
            targetParameterFileName = args[1];
        }
         else{ //(args.length > 2)
            System.err.println("at most two parameter required: shape-import description and ohdm target description");
            System.exit(1);
        }

        System.out.println("use " + importParameterFileName + " as shapefile description");
        System.out.println("use " + targetParameterFileName + " as ohdm db description");

        try {
            Shapefile2OHDM shape2OHDM = new Shapefile2OHDM(
                    new Parameter(importParameterFileName),
                    new Parameter(targetParameterFileName)
            );

            shape2OHDM.importShapefile();
        }
        catch(Exception e) {
            System.err.println(e.getLocalizedMessage());
        }
    }

    public Shapefile2OHDM(Parameter importParameter, Parameter targetParameter) throws SQLException {
        this.targetParameter = targetParameter;
        this.importParameter = importParameter;

        if(!importParameter.getdbName().equalsIgnoreCase(targetParameter.getdbName())) {
            throw new SQLException("both schemas must be in the same database - fatal error");
        }
    }

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
    void importShapefile() throws IOException {
        try {
            // just to be sure - create ohdm db if not yet done
            Connection connection = DB.createConnection(targetParameter);

            try {
                util.OHDM_DB.createOHDMTables(connection, targetParameter.getSchema());
            } catch (SQLException e) {
                System.out.println("catch and ignore sql exception - db most probably already exists: "
                        + e.getLocalizedMessage());
            }
            connection.close();

            String importerUserName = this.importParameter.getUserName();
            System.err.println("deal with user id!");
            this.importerUserIDSet = true;
            this.importerUserID = new BigDecimal(-1);

            String validSinceString = importParameter.getValidSince();
            String columnValidSinceYear = importParameter.getColumnValidSinceYear();
            String validUntilString = importParameter.getValidUntil();
            String columnValidUntilYear = importParameter.getColumnValidUntilYear();

            if ( (validSinceString == null && columnValidSinceYear == null)
                || (validUntilString == null && columnValidUntilYear == null)
                || this.importParameter.getClassificationID() == -1) {

                validSinceString = java.time.LocalDate.now().toString() ;
                validUntilString = java.time.LocalDate.now().toString() ;


               // throw new SQLException("validSince, validUntil (at least year) and classification must be set - fatal, give up");
            }

            // both schemas are in the same db - ensured, see constructor
            this.sqlQueue = new SQLStatementQueue(this.importParameter);

           // find primary key column
            sqlQueue.append("SELECT kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc " +
                    "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu\n" +
                    "    ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME\n" +
                    "WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'\n" +
                    "AND tc.TABLE_NAME = '");

            sqlQueue.append(importParameter.getTableName());
            sqlQueue.append("'");

            ResultSet resultSet = sqlQueue.executeWithResult();

            if (resultSet.next()) {
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

            // set srid
            System.out.println("4326 is assumed as import format - going to set explicitly.");
            sqlQueue.append("UPDATE ");
            sqlQueue.append(DB.getFullTableName(importParameter.getSchema(), importParameter.getTableName()));
            sqlQueue.append(" SET ");
            sqlQueue.append(columnNameGeometry);
            sqlQueue.append(" = st_setsrid(");
            sqlQueue.append(columnNameGeometry);
            sqlQueue.append(", 4326)");
            sqlQueue.forceExecute();

            // SELECT prov_name, st_asewkt(geom), st_geometrytype(geom) FROM test.preussen_1830;
            sqlQueue.append("select pg_typeof(");
            sqlQueue.append(pkColumnName);
            sqlQueue.append("), ");
            sqlQueue.append(pkColumnName);
            sqlQueue.append(", ");
            sqlQueue.append(columnNameObjectName);
            sqlQueue.append(", st_geometrytype(" + columnNameGeometry + ")");
            sqlQueue.append(" FROM ");
            sqlQueue.append(DB.getFullTableName(importParameter.getSchema(), importParameter.getTableName()));
            resultSet = sqlQueue.executeWithResult();

            int pk;
            String objectName = null;
            String geometryType = null;

            while (resultSet.next()) {
                String pkType = resultSet.getString(1);
                switch (pkType) {
                    case "integer":
                        pk = resultSet.getInt(2);
                        break;
                    default:
                        throw new SQLException("only integer primary key are supported yet - see shape2ohdm code");
                }
                objectName = resultSet.getString(3);
                geometryType = resultSet.getString(4);

                importRows.add(new ImportRow(pk, objectName, geometryType));
            }

            // for the final import logging - keep track of any imported row
            List<BigDecimal> importedObjectGeometryIDs = new ArrayList<>();
            List<BigDecimal> importedObjectIDs = new ArrayList<>();

            List<BigDecimal> importedPointGeomIDs = new ArrayList<>();
            List<BigDecimal> importedLineGeomIDs = new ArrayList<>();
            List<BigDecimal> importedPolygonGeomIDs = new ArrayList<>();

            // we made a survey - let's copy those data into ohdm
            for (ImportRow currentRow : importRows) {
                try {
                    System.out.println("import row: " + currentRow);

                    /////////////////// geometries /////////////////////////////////////////////////
                    if (currentRow.geomType.contains("ST_Multi")) {
                        this.insertMultiGeometry(currentRow);
                    } else {
                        // single geometry
                        this.insertSingleGeometry(currentRow);
                    }

                    /////////////////// objects ////////////////////////////////////////////////////
                    String fullObjectTableName = DB.getFullTableName(this.targetParameter.getSchema(), OHDM_DB.TABLE_GEOOBJECT);
                    this.sqlQueue.append("insert into ");
                    this.sqlQueue.append(fullObjectTableName);
                    this.sqlQueue.append(" (name");
                    if (this.importerUserIDSet) {
                        this.sqlQueue.append(", source_user_id");
                    }
                    this.sqlQueue.append(") VALUES ('");
                    this.sqlQueue.append(Util.escapeSpecialChar4SQL(currentRow.objectName));
                    this.sqlQueue.append("'");
                    if (this.importerUserIDSet) {
                        this.sqlQueue.append(", ");
                        this.sqlQueue.append(this.importerUserID.longValue());
                    }
                    this.sqlQueue.append(") returning id");
                    resultSet = this.sqlQueue.executeWithResult();
                    resultSet.next(); // just a single line
                    BigDecimal objectOHDMID = resultSet.getBigDecimal(1);

                    importedObjectIDs.add(objectOHDMID);

                    /////////////////// objects - geometry ////////////////////////////////////////////////

                    String fullOGTableName = DB.getFullTableName(this.targetParameter.getSchema(), OHDM_DB.TABLE_GEOOBJECT_GEOMETRY);
                    StringBuilder sb = new StringBuilder();
                    sb.append("insert into ");
                    sb.append(fullOGTableName);
                    sb.append(" (");
                    if (this.importerUserIDSet) {
                        sb.append("source_user_id, ");
                    }
                    sb.append("id_geoobject_source, classification_id, valid_since, valid_until, type_target, id_target) SELECT ");
                    if (this.importerUserIDSet) {
                        sb.append(this.importerUserID.longValue());
                        sb.append(", ");
                    }
                    sb.append(objectOHDMID.longValue()); // object id
                    sb.append(", ");
                    sb.append(this.importParameter.getClassificationID()); // classification id
                    sb.append(", ");

    /*
    insert into ohdmhistoric.geoobject_geometry
    (id_geoobject_source, classification_id, source_user_id, valid_since)
    select 123456, 56, 42, to_timestamp(
        concat(
            cast(gwsyear as varchar(4)), '-',
            cast(gwsmonth as varchar(2)), '-',
             cast(gwsday as varchar(2))
        ), 'YYYY-MM-DD') FROM test.cshapes returning id
     */

                    String validSinceValue = null;
                    // since
                    if (columnValidSinceYear == null) {
                        validSinceValue = "'" + validSinceString + "'";
                    }
                    else { // at least year is in the table
                        validSinceValue = "to_timestamp(concat(cast(";
                        validSinceValue += columnValidSinceYear;
                        validSinceValue += " as varchar(4)), '-',";
                        String columnValidSinceMonth = this.importParameter.getColumnValidSinceMonth();
                        if (columnValidSinceMonth == null) {
                            validSinceValue += "'01-',";
                        } else {
                            validSinceValue += "cast(";
                            validSinceValue += columnValidSinceMonth;
                            validSinceValue += " as varchar(2)), '-',";
                        }
                        String columnValidSinceDay = this.importParameter.getColumnValidSinceDay();
                        if (columnValidSinceDay == null) {
                            validSinceValue += "'01',";
                        } else {
                            validSinceValue += "cast(";
                            validSinceValue += columnValidSinceDay;
                            validSinceValue += " as varchar(2))";
                        }
                        validSinceValue += "), 'YYYY-MM-DD')";
                    }

                    sb.append(validSinceValue);
                    sb.append(", ");

                    String validUntilValue = null;
                    // until
                    if (columnValidUntilYear == null) {
                        validUntilValue = "'" + validUntilString + "'";
                    }
                    else { // at least year is in the table
                        validUntilValue = "to_timestamp(concat(cast(";
                        validUntilValue += columnValidUntilYear;
                        validUntilValue += " as varchar(4)), '-',";
                        String columnValidUntilMonth = this.importParameter.getColumnValidUntilMonth();
                        if (columnValidUntilMonth == null) {
                            validUntilValue += "'01-',";
                        } else {
                            validUntilValue += "cast(";
                            validUntilValue += columnValidUntilMonth;
                            validUntilValue += " as varchar(2)), '-',";
                        }
                        String columnValidUntilDay = this.importParameter.getColumnValidUntilDay();
                        if (columnValidUntilDay == null) {
                            validUntilValue += "'01',";
                        } else {
                            validUntilValue += "cast(";
                            validUntilValue += columnValidUntilDay;
                            validUntilValue += " as varchar(2))";
                        }
                        validUntilValue += "), 'YYYY-MM-DD')";
                    }

                    sb.append(validUntilValue);
                    sb.append(", ");

                    // add geometries
                    importedObjectGeometryIDs = this.addGeomsAndInsert(sb.toString(), currentRow.pointIDs, OHDM_DB.POINT, importedObjectGeometryIDs, currentRow);
                    importedObjectGeometryIDs = this.addGeomsAndInsert(sb.toString(), currentRow.lineIDs, OHDM_DB.LINESTRING, importedObjectGeometryIDs, currentRow);
                    importedObjectGeometryIDs = this.addGeomsAndInsert(sb.toString(), currentRow.polygonIDs, OHDM_DB.POLYGON, importedObjectGeometryIDs, currentRow);
                }
                catch(SQLException e) {
                    System.err.println("Exception when processing row " + currentRow.pk);
                    System.err.println(e.getLocalizedMessage());
                    System.err.println(sqlQueue);
                    System.err.println("proceed with next row");
                }
            } // done with a row

            String nowString = Util.getNowTimeString();

            // TODO: create table with geoobject-geometry ids to allow removing inserted rows
            // insert: importedObjectGeometryIDs
            String insertedIDsTableName = this.importParameter.getTableName() + Util.fillWhiteSpacesWithUnderline(nowString);
            insertedIDsTableName = Util.makeValidTableName(insertedIDsTableName);
            insertedIDsTableName = DB.getFullTableName(this.importParameter.getSchema(), insertedIDsTableName);

            sqlQueue.append("CREATE TABLE IF NOT EXISTS ");
            sqlQueue.append(insertedIDsTableName);
            sqlQueue.append(" (id_geoobject_geometry bigint);");
            sqlQueue.forceExecute();

            for(BigDecimal goGeomID : importedObjectGeometryIDs) {
                sqlQueue.append("INSERT INTO ");
                sqlQueue.append(insertedIDsTableName);
                sqlQueue.append(" VALUES ( ");
                sqlQueue.append(goGeomID.longValue());
                sqlQueue.append("); ");
                sqlQueue.forceExecute();
            }

            // write a log entry about this import

            /*
        sq.append("source_user_id bigint, ");
        sq.append("importDate timestamp,");
        sq.append("source_url character varying,");
        sq.append("source_license character varying,");
        sq.append("source_comments character varying");
             */
            String fullExternalImportTableName = DB.getFullTableName(this.targetParameter.getSchema(), OHDM_DB.TABLE_EXTERNAL_IMPORTS);
            this.sqlQueue.append("insert into ");
            this.sqlQueue.append(fullExternalImportTableName);
            this.sqlQueue.append(" (");
            if (this.importerUserIDSet) {
                this.sqlQueue.append("source_user_id, ");
            }

            this.sqlQueue.append("importDate, source_url, source_license, source_citation, source_comments, ");
            this.sqlQueue.append("importParameterFileName,importTableName,importedGObject_Geometry_IDs_Table) VALUES (");
            if (this.importerUserIDSet) {
                this.sqlQueue.append(this.importerUserID.longValue());
                this.sqlQueue.append(", ");
            }
            this.sqlQueue.append("'");
            this.sqlQueue.append(nowString);
            this.sqlQueue.append("', '");
            this.sqlQueue.append(Util.escapeSpecialChar4SQL(this.importParameter.getSourceURL()));
            this.sqlQueue.append("', '");
            this.sqlQueue.append(Util.escapeSpecialChar4SQL(this.importParameter.geSourceLicense()));
            this.sqlQueue.append("', '");
            this.sqlQueue.append(Util.escapeSpecialChar4SQL(this.importParameter.geSourceCitation()));
            this.sqlQueue.append("', '");
            this.sqlQueue.append(Util.escapeSpecialChar4SQL(this.importParameter.geSourceComments()));
            this.sqlQueue.append("', '");
            this.sqlQueue.append(Util.escapeSpecialChar4SQL(this.importParameter.getParameterFilename()));
            this.sqlQueue.append("', '");
            this.sqlQueue.append(Util.escapeSpecialChar4SQL(
                    DB.getFullTableName(importParameter.getSchema(), importParameter.getTableName())));
            this.sqlQueue.append("', '");
            this.sqlQueue.append(Util.escapeSpecialChar4SQL(insertedIDsTableName));
            this.sqlQueue.append("');");
            this.sqlQueue.forceExecute();
        }
        catch(Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.err.println(sqlQueue.toString());
        }
    }

    private List<BigDecimal> addGeomsAndInsert(String insertPreamble, List<BigDecimal> ohdmIDs, int geomType,
                                               List<BigDecimal> goIDs, ImportRow currentRow) throws SQLException {

        if(ohdmIDs.isEmpty()) return goIDs; // don't do anything

        for(BigDecimal id : ohdmIDs) {
            sqlQueue.append(insertPreamble);
            sqlQueue.append(geomType);
            sqlQueue.append(", ");
            sqlQueue.append(id.longValue());
            sqlQueue.append(" FROM ");
            sqlQueue.append(DB.getFullTableName(importParameter.getSchema(), importParameter.getTableName()));
            sqlQueue.append(" WHERE ");
            sqlQueue.append(this.pkColumnName);
            sqlQueue.append(" = ");
            sqlQueue.append(currentRow.pk.toString());
            sqlQueue.append(" returning id ");

            ResultSet resultSet = sqlQueue.executeWithResult();
            resultSet.next();
            goIDs.add(resultSet.getBigDecimal(1));
        }

        return goIDs;
    }

    private void insertSingleGeometry(ImportRow currentRow) throws SQLException {
        System.err.println("insertSingleGeometry not yet implemented");
        /*
        this.sqlQueue.append("SELECT id, st_geometrytype(geom) FROM ");
        this.sqlQueue.append(fullTmpTableName);

        ResultSet resultSet = this.sqlQueue.executeWithResult();
        // there is just a single line
        resultSet.next();
        int id = resultSet.getInt(1);
        String geomType = resultSet.getString(2);

         */
    }

    private void insertMultiGeometry(ImportRow currentRow) throws SQLException {
        // split multigeometry
        String fullTmpTableName = this.createTempGeomTableWithID(this.sqlQueue, importParameter.getSchema());

        /*
        select st_asewkt(b.geom) from (SELECT (ST_Dump(geom)).geom AS geom FROM test.preussen_1830 as a) as b
        select * into test.temp from
        (select singleGeom.geom from
        (SELECT (ST_Dump(geom)).geom AS geom FROM
        (select * from test.preussen_1830 where gid = 1)
            as dump)
            as singleGeom) as c
         */

        this.sqlQueue.append("insert into ");
        this.sqlQueue.append(fullTmpTableName);
        this.sqlQueue.append(" (geom) select singleGeom.geom from (SELECT (ST_Dump(geom)).geom AS geom FROM (select * FROM ");
        this.sqlQueue.append(DB.getFullTableName(importParameter.getSchema(), importParameter.getTableName()));
        this.sqlQueue.append(" where ");
        this.sqlQueue.append(this.pkColumnName);
        this.sqlQueue.append(" = ");
        this.sqlQueue.append(currentRow.pk.toString());
        this.sqlQueue.append(") as dump) as singleGeom");

        this.sqlQueue.forceExecute();

        // now iterate new table and import each geometry to ohdm geometry tables
        currentRow.addPointIDs(this.insertGeometry(fullTmpTableName, OHDM_DB.POINT));
        currentRow.addLineIDs(this.insertGeometry(fullTmpTableName, OHDM_DB.LINESTRING));
        currentRow.addPolygonIDs(this.insertGeometry(fullTmpTableName, OHDM_DB.POLYGON));

        DB.drop(this.sqlQueue, fullTmpTableName);
    }

    private List<BigDecimal> insertGeometry(String fullTmpTableName, int geometryType) throws SQLException {
        String geometryTypeString = null;

        switch(geometryType) {
            case OHDM_DB.POINT: geometryTypeString = "ST_Point"; break;
            case OHDM_DB.LINESTRING: geometryTypeString = "ST_Linestring"; break;
            case OHDM_DB.POLYGON: geometryTypeString = "ST_Polygon"; break;
            default: throw new SQLException("unknown geometry type: " + geometryType);
        }

        this.sqlQueue.append("SELECT id FROM ");
        this.sqlQueue.append(fullTmpTableName);
        this.sqlQueue.append(" where st_geometrytype(geom) = '");
        this.sqlQueue.append(geometryTypeString);
        this.sqlQueue.append("'");

        ResultSet resultSet = this.sqlQueue.executeWithResult();

        List<Integer> idList = new ArrayList<>();
        while(resultSet.next()) {
            idList.add(resultSet.getInt(1));
        }

/*
        System.out.println(geometryTypeString);
        System.out.println("+++++++++++++++++");
        for(int id : idList) {
            System.out.print("id == " + id + " | ");
        }
        System.out.println("\n+++++++++++++++++");
*/

        String fullTargetTableName = null;
        String geomColumnName = null;

        switch(geometryType) {
            case OHDM_DB.POINT:
                fullTargetTableName = DB.getFullTableName(targetParameter.getSchema(), OHDM_DB.TABLE_POINTS);
                geomColumnName = OHDM_DB.POINT_GEOMETRY_COLUMN_NAME;
                break;
            case OHDM_DB.LINESTRING:
                fullTargetTableName = DB.getFullTableName(targetParameter.getSchema(), OHDM_DB.TABLE_LINES);
                geomColumnName = OHDM_DB.LINE_GEOMETRY_COLUMN_NAME;
                break;
            case OHDM_DB.POLYGON:
                fullTargetTableName = DB.getFullTableName(targetParameter.getSchema(), OHDM_DB.TABLE_POLYGONS);
                geomColumnName = OHDM_DB.POLYGON_GEOMETRY_COLUMN_NAME;
                break;
        }

        // insert into ohdm - piece by piece
        List<BigDecimal> ohdmGeomIDs = new ArrayList<>();
        for(int id : idList) {
            this.sqlQueue.append("insert into ");
            this.sqlQueue.append(fullTargetTableName);
            this.sqlQueue.append(" (");
            this.sqlQueue.append(geomColumnName);
            this.sqlQueue.append(") select geom from ");
            this.sqlQueue.append(fullTmpTableName);
            this.sqlQueue.append(" where id = ");
            this.sqlQueue.append(id);
            this.sqlQueue.append(" returning id");

            resultSet = this.sqlQueue.executeWithResult();
            resultSet.next();
            BigDecimal bigDecimal = resultSet.getBigDecimal(1);
            ohdmGeomIDs.add(bigDecimal);
        }

        // set user name - if any
        if(this.importerUserIDSet && !ohdmGeomIDs.isEmpty()) {
            this.sqlQueue.append("update ");
            this.sqlQueue.append(fullTargetTableName);
            this.sqlQueue.append(" SET source_user_id = ");
            this.sqlQueue.append(this.importerUserID.longValue());
            this.sqlQueue.append(" where ");
            this.sqlQueue.append(this.produceAlternativeIDsClause(ohdmGeomIDs));
            this.sqlQueue.append("; ");
            this.sqlQueue.forceExecute();
        }

        return ohdmGeomIDs;
    }

    private String produceAlternativeIDsClause(List<BigDecimal> ohdmGeomIDs) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;

        for(BigDecimal id : ohdmGeomIDs) {
            if (!first) {
                sb.append(" OR ");
            } else {
                first = false;
            }

            sb.append("id = ");
            sb.append(id);
        }

        return sb.toString();
    }

    private String createTempGeomTableWithID(SQLStatementQueue sq, String schema) throws SQLException {
        String tmpTableName = "temp" + System.currentTimeMillis();
        String fullTmpTableName = DB.getFullTableName(schema, tmpTableName);
        DB.createSequence(sq, schema, tmpTableName);

        sq.append(DB.getCreateTableBegin(schema, tmpTableName));
        sq.append(",");
        sq.append("geom geometry");
        sq.append(");");
        sq.forceExecute();

        return fullTmpTableName;
    }


    private class ImportRow {
        public final Object pk;
        public final String objectName;
        public final String geomType;
        public List<BigDecimal> pointIDs;
        public List<BigDecimal> lineIDs;
        public List<BigDecimal> polygonIDs;

        ImportRow(Object pk, String objectName, String geomType) {
            this.pk = pk;
            this.objectName = objectName;
            this.geomType = geomType;
        }

        public String toString() {
            return "pk: " + pk + " | name: " + objectName + " | geomType: " + geomType;
        }

        public void addPointIDs(List<BigDecimal> geomIDList) {
            this.pointIDs = geomIDList;
        }

        public void addLineIDs(List<BigDecimal> geomIDList) {
            this.lineIDs = geomIDList;
        }

        public void addPolygonIDs(List<BigDecimal> geomIDList) {
            this.polygonIDs = geomIDList;
        }
    }
}
