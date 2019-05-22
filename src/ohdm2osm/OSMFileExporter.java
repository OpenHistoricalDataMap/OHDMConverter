package ohdm2osm;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import osm.OSMClassification;
import util.DB;
import util.OHDM_DB;
import util.Parameter;
import util.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public class OSMFileExporter {

    private static final String DEFAULT_USERNAME = "username";
    private static final String PADDING = "  ";
    private final Connection sourceConnection;
    private final PrintStream nodeStream;
    private final PrintStream wayStream;
    private final SimpleDateFormat dateFormat;
    private final PrintStream relationStream;
    private final SQLStatementQueue sql;
    private int ldfID;
    private final List<String> pointTableNames;
    private final List<String> linesTableNames;
    private final List<String> polygonTableNames;
    private final Parameter sourceParameter;
    private final String bboxWKT;
    private final String dateString;
    private String minLatString;
    private String minLongString;
    private String maxLatString;
    private String maxLongString;

    public OSMFileExporter(Parameter sourceParameter, OutputStream nodeOSStream,
                           OutputStream wayOSStream,
                           OutputStream relationOSStream,
                           List<String> pointTableNames, List<String> linesTableNames,
                           List<String> polygonTableNames,
                           String minLatString, String minLongString, String maxLatString,
                           String maxLongString, String dateString) throws SQLException {
        
        this.sourceParameter = sourceParameter;
        this.sourceConnection = DB.createConnection(sourceParameter);
        this.sql = new SQLStatementQueue(this.sourceConnection);
        this.nodeStream = new PrintStream(nodeOSStream);
        this.wayStream = new PrintStream(wayOSStream);
        this.relationStream = new PrintStream(relationOSStream);
        this.pointTableNames = pointTableNames;
        this.linesTableNames = linesTableNames;
        this.polygonTableNames = polygonTableNames;
        this.minLatString = minLatString;
        this.minLongString = minLongString;
        this.maxLatString = maxLatString;
        this.maxLongString = maxLongString;

        StringBuilder sb = new StringBuilder();
        sb.append("POLYGON((");
        sb.append(this.minLongString); // down left
        sb.append(" ");
        sb.append(this.minLatString);
        sb.append(", ");
        sb.append(this.minLongString); // upper left
        sb.append(" ");
        sb.append(this.maxLatString);
        sb.append(", ");
        sb.append(this.maxLongString); // upper right
        sb.append(" ");
        sb.append(this.maxLatString);
        sb.append(", ");
        sb.append(this.maxLongString); // down right
        sb.append(" ");
        sb.append(this.minLatString);
        sb.append(", ");
        sb.append(this.minLongString); // back to down left
        sb.append(" ");
        sb.append(this.minLatString);
        sb.append("))");

        this.bboxWKT = sb.toString();
        this.dateString = dateString;

        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        this.ldfID = 1;
    }

    private void exportPoints() throws SQLException {
        // points
        /*
        SELECT ST_X(ST_TRANSFORM(point, 4326)), ST_Y(ST_TRANSFORM(point, 4326)), classid, name, valid_since
        FROM public.shop_iconbakery
        WHERE
        valid_since <= '2018_01-01' AND valid_until >= '2018_01-01'
        AND
        ST_WITHIN(point, ST_TRANSFORM(ST_GeomFromEWKT('SRID=4326;POLYGON((2 45, 2 55, 14 55, 14 45, 2 45))'), 3857))
         */
        if(this.pointTableNames != null) {
            for(String tableName : this.pointTableNames) {
                sql.append("SELECT ST_X(ST_TRANSFORM(point, 4326)), ST_Y(ST_TRANSFORM(point, 4326)), ");
                sql.append("classid, name, valid_since FROM ");
                sql.append(util.DB.getFullTableName(this.sourceParameter.getSchema(), tableName));
                sql.append(" WHERE valid_since <= '");
                sql.append(this.dateString);
                sql.append("' AND valid_until >= '");
                sql.append(this.dateString);
                sql.append("' AND ST_WITHIN(point, ST_TRANSFORM(ST_GeomFromEWKT('SRID=4326;");
                sql.append(this.bboxWKT);
                sql.append("'), 3857))");

                ResultSet resultSet = sql.executeWithResult();
                while(resultSet.next()) {
                    this.printNode(
                            resultSet.getDate("valid_since"),
                            resultSet.getDouble("st_y"),
                            resultSet.getDouble("st_x"),
                            resultSet.getBigDecimal("classid"),
                            resultSet.getString("name")
                    );
                }
            }
        }
    }

    private void exportLines() throws SQLException {
        /*
        WAY
SELECT st_astext(st_transform(line, 4326)), name, classid, valid_since
	FROM public.highway_primary_lines
	WHERE
        valid_since <= '2018_01-01' AND valid_until >= '2018_01-01'
        AND
        ST_WITHIN(line, ST_TRANSFORM(ST_GeomFromEWKT('SRID=4326;POLYGON((2 45, 2 55, 14 55, 14 45, 2 45))'), 3857))
         */

        if(this.linesTableNames != null) {
            for(String tableName : this.linesTableNames) {
                sql.append("SELECT ST_AsText(st_transform(line, 4326)), name, classid, valid_since FROM ");
                sql.append(util.DB.getFullTableName(this.sourceParameter.getSchema(), tableName));
                sql.append(" WHERE valid_since <= '");
                sql.append(this.dateString);
                sql.append("' AND valid_until >= '");
                sql.append(this.dateString);
                sql.append("' AND ST_WITHIN(line, ST_TRANSFORM(ST_GeomFromEWKT('SRID=4326;");
                sql.append(this.bboxWKT);
                sql.append("'), 3857))");

                ResultSet resultSet = sql.executeWithResult();
                while(resultSet.next()) {
                    this.printWay(
                            resultSet.getDate("valid_since"),
                            resultSet.getString(1),
                            resultSet.getBigDecimal("classid"),
                            resultSet.getString("name"),
                            false
                    );
                }
            }
        }
    }

    private void exportPolygons() throws SQLException {

        /*
POLYGON
SELECT st_astext(st_transform((ST_ExteriorRing(polygon)),4326)), ST_NumInteriorRings(polygon), classid, name, valid_since
  FROM public.building_apartments limit 10;

  if num interior rings > 1 query interior rings...

SELECT geom_id, st_astext(st_transform((ST_ExteriorRing(polygon)),4326)), ST_NumInteriorRings(polygon),
st_astext(ST_InteriorRingN(polygon, 1)), subclassname, name, valid_since
  FROM public.building_apartments where ST_NumInteriorRings(polygon) > 0 limit 10;

  where geom_id IN (siehe oben)
         */

        if(this.polygonTableNames != null) {
            for(String tableName : this.polygonTableNames) {
                sql.append("SELECT st_astext(st_transform((ST_ExteriorRing(polygon)),4326)), " +
                        "ST_NumInteriorRings(polygon), classid, name, valid_since, geom_id FROM ");
                sql.append(util.DB.getFullTableName(this.sourceParameter.getSchema(), tableName));
                sql.append(" WHERE valid_since <= '");
                sql.append(this.dateString);
                sql.append("' AND valid_until >= '");
                sql.append(this.dateString);
                sql.append("' AND ST_WITHIN(polygon, ST_TRANSFORM(ST_GeomFromEWKT('SRID=4326;");
                sql.append(this.bboxWKT);
                sql.append("'), 3857))");

                ResultSet resultSet = sql.executeWithResult();
                while(resultSet.next()) {
                    java.sql.Date valid_since = resultSet.getDate("valid_since");
                    BigDecimal classid = resultSet.getBigDecimal("classid");
                    String name = resultSet.getString("name");
                    int numberInteriorRings = resultSet.getInt(2);

                    if(numberInteriorRings == 0) {
                        // just a closed way
                        this.printWay(valid_since, resultSet.getString(1),
                                classid, name, true);
                    } else {
                        // becomes a multipolygon
                        this.printWay(valid_since, resultSet.getString(1),
                                null, null, true);
                        // TODO: classid and name null or to be set?!

                        // remember Id out ring
                        int idOuter = this.ldfID-1;

                        List<Integer> innerIDs = new ArrayList<>();

                        // remember geom_id
                        BigDecimal geom_id_polygon = resultSet.getBigDecimal("geom_id");

                        for(int indexInterior = 1; indexInterior <= numberInteriorRings; indexInterior++) {
                        /*
SELECT st_astext(ST_TRANSFORM(ST_InteriorRingN(polygon, 1), 4326))
 FROM public.building_apartments where geom_id = ;
                         */
                            sql.append("SELECT st_astext(ST_TRANSFORM(ST_InteriorRingN(polygon, ");
                            sql.append(indexInterior);
                            sql.append("), 4326)) FROM ");
                            sql.append(util.DB.getFullTableName(this.sourceParameter.getSchema(), tableName));
                            sql.append(" WHERE geom_id = ");
                            sql.append(geom_id_polygon.toString());

                            resultSet = sql.executeWithResult();
                            if(resultSet.next()) {
                                this.printWay(valid_since, resultSet.getString(1),
                                        null, null,true);

                                innerIDs.add(this.ldfID-1);
                            }
                        }

                        // wrote inner ways - create relation
                        /*
                        <relation id="1">
                          <tag k="type" v="multipolygon" />
                          <member type="way" id="1" role="outer" />
                          <member type="way" id="2" role="inner" />
                        </relation>
                         */

                        this.relationStream.print(PADDING);
                        this.relationStream.print("<relation id='");
                        this.relationStream.print(this.ldfID++);
                        this.relationStream.print("' timestamp='");
                        this.relationStream.print(this.dateFormat.format(valid_since));

                        this.relationStream.print("' uid='1' user='");
                        this.relationStream.print(DEFAULT_USERNAME);
                        this.relationStream.print("'");

                        this.relationStream.println(" visible='true' version='1' changeset='1'>");

                        this.printTag("type", "multipolygon", this.relationStream);

                        this.relationStream.print(PADDING);
                        this.relationStream.print(PADDING);
                        this.relationStream.print("<member type='way' ref='");
                        this.relationStream.print(idOuter);
                        this.relationStream.println("' role='outer' />");

                        for(int innerID : innerIDs) {
                            this.relationStream.print(PADDING);
                            this.relationStream.print(PADDING);
                            this.relationStream.print("<member type='way' ref='");
                            this.relationStream.print(innerID);
                            this.relationStream.println("' role='inner' />");
                        }

                        this.printClassificationTag(classid, this.relationStream);
                        if(name != null) {
                            this.printTag("name", name, this.relationStream);
                        }

                        this.relationStream.print(PADDING);
                        this.relationStream.println("</relation>");
                    }
                }
            }
        }
    }

    private void printNode(java.sql.Date valid_since, double latitude, double longitude,
                           BigDecimal classid, String name) {
        // cut length latitude
        String latString = Double.toString(latitude);
        if(latString.length() > 10) {
            latString = latString.substring(0, 11);
        }

        // cut length longitude
        String longString = Double.toString(longitude);
        if(longString.length() > 10) {
            longString = longString.substring(0, 11);
        }

        this.printNode(valid_since, latString, longString, classid, name);
    }

    private void printNode(java.sql.Date valid_since, String latString, String longString,
                           BigDecimal classid, String name) {
        this.nodeStream.print(PADDING);
        this.nodeStream.print("<node id='");
        this.nodeStream.print(this.ldfID++);

        this.nodeStream.print("' timestamp='");
        this.nodeStream.print(this.dateFormat.format(valid_since));

        this.nodeStream.print("' uid='1' user='");
        this.nodeStream.print(DEFAULT_USERNAME);
        this.nodeStream.print("'");

        this.nodeStream.print(" visible='true' version='1' changeset='1' ");

        this.nodeStream.print("lat='");
        this.nodeStream.print(latString);

        this.nodeStream.print("' lon='");
        this.nodeStream.print(longString);
        this.nodeStream.print("'");


        if(classid == null && name == null) {
            // anonymous node
            this.nodeStream.println(" />");
        } else {
            this.nodeStream.println(" >");

            this.printClassificationTag(classid, this.nodeStream);

            if(name != null) {
                this.printTag("name", name, this.nodeStream);
            }


            this.nodeStream.print(PADDING);
            this.nodeStream.println("</node>");
        }
    }

    private void printClassificationTag(BigDecimal classid, PrintStream ps) {
        if(classid != null) {
            this.printClassificationTag(classid.intValue(), ps);
        }
    }

    private void printClassificationTag(int classid, PrintStream ps) {
        OSMClassification osmC = OSMClassification.getOSMClassification();
        String fullClassName =
                osmC.getFullClassName(classid);

        this.printTag(
                osmC.getClassNameByFullName(fullClassName),
                osmC.getSubClassNameByFullName(fullClassName),
                ps);
    }

    private String doEscape(String text) {
        // escape & -> &amp;
        int i = text.indexOf("&");

        if(i == -1) return text;
        StringBuilder sb = new StringBuilder(text.substring(0, i+1));
        sb.append("amp;");
        sb.append(text.substring(i+1));

        return sb.toString();

    }

    private void printTag(String key, String value, PrintStream ps) {
        ps.print(PADDING);
        ps.print("  <tag k='");
        ps.print(this.doEscape(key));
        ps.print("' v='");
        ps.print(this.doEscape(value));
        ps.println("' />");
    }

    List<String> getLatLongFromLineString(String linestring) {
        List<String> latLongList = new ArrayList<>();
        if(linestring == null) {
            return latLongList;
        }

        // extract linestring
        int open = linestring.indexOf("(");
        int close = linestring.indexOf(")");

        String coordStrings = linestring.substring(open+1, close);
        StringTokenizer st = new StringTokenizer(coordStrings, ",");

        // walk through coordinates
        while(st.hasMoreTokens()) {
            String coordString = st.nextToken();
            // split
            int i = coordString.indexOf(" ");
            latLongList.add(coordString.substring(i+1)); // latitude
            latLongList.add(coordString.substring(0, i)); // longitude
        }

        return latLongList;
    }

    private void printWay(java.sql.Date valid_since, String linestring, BigDecimal classid, String name,
                          boolean isPolygon) {
        int firstNodeID = this.ldfID;
        // extract nodes from linestring
        List<String> latLongList = this.getLatLongFromLineString(linestring);
        Iterator<String> iterator = latLongList.iterator();

        // look ahead
        String latString = iterator.next();
        String longString = iterator.next();

        do {
            this.printNode(valid_since, latString, longString, null, null);
            latString = iterator.next();
            longString = iterator.next();
        } while(iterator.hasNext());

        if(!isPolygon) {
            // it is a way - save final node - in a polygon: it is the same point
            this.printNode(valid_since, latString, longString, null, null);
        }

        int lastNodeID = this.ldfID-1;

        /*
        for each way:

        for way
          <way id='lfdNummer' timestamp='since' uid='1' user='fake' visible='true' version='1'>
            <nd ref='p1' />
            <nd ref='p2' />
            <tag k='highway' v='subclassname' />
          </way>

         */

        this.wayStream.print(PADDING);
        this.wayStream.print("<way id='");
        this.wayStream.print(this.ldfID++);

        this.wayStream.print("' timestamp='");
        this.wayStream.print(this.dateFormat.format(valid_since));

        this.wayStream.print("' uid='1' user='");
        this.wayStream.print(DEFAULT_USERNAME);
        this.wayStream.print("'");

        this.wayStream.println(" visible='true' version='1' changeset='1' >");
        for(int nodeID = firstNodeID; nodeID <= lastNodeID; nodeID++) {
            this.wayStream.print(PADDING);
            this.wayStream.print(PADDING);
            this.wayStream.print("<nd ref='");
            this.wayStream.print(nodeID);
            this.wayStream.println("' />");
        }

        if(isPolygon) {
            // first node must be repeated at the end
            this.wayStream.print(PADDING);
            this.wayStream.print(PADDING);
            this.wayStream.print("<nd ref='");
            this.wayStream.print(firstNodeID);
            this.wayStream.println("' />");
        }

        this.printClassificationTag(classid, this.wayStream);

        this.wayStream.print(PADDING);
        this.wayStream.println("</way>");
    }

    void export() {
        // write XML preamble
        this.nodeStream.println("<?xml version='1.0' encoding='UTF-8'?>");
        this.nodeStream.println("<osm version='0.6' generator='OHDM_Extractor'>");
        this.nodeStream.print("<bounds minlat='");
        this.nodeStream.print(this.minLatString);
        this.nodeStream.print("' minlon='");
        this.nodeStream.print(this.minLongString);
        this.nodeStream.print("' maxlat='");
        this.nodeStream.print(this.maxLatString);
        this.nodeStream.print("' maxlon='");
        this.nodeStream.print(this.maxLongString);
        this.nodeStream.println("' origin='OHDM 1.0 (ohdm.net)' />");

        try {
            this.exportPoints();
            this.exportLines();
            this.exportPolygons();
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println(this.sql);
        }
    }

    public static void main(String[] args) throws IOException, SQLException, ParseException {
        String DEFAULT_RENDERING_PARAMETER_FILE = "db_rendering_local.txt";
        String OUTPUTFILENAME = "ohdm_extracted.osm";
        String minLatString = "45";
        String minLongString = "10";
        String maxLatString = "55";
        String maxLongString = "15";

        String dateString = "2016-12-31";
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = df.parse(dateString);

        System.out.println("extracting OSM data file from OHDM rendering tables");

        File nodeFile = new File(OUTPUTFILENAME);
        File wayFile = File.createTempFile("way", "_osm");
        File relationFile = File.createTempFile("relation", "_osm");

        OutputStream nodeStream = new FileOutputStream(nodeFile);
        OutputStream wayStream = new FileOutputStream(wayFile);
        OutputStream relationStream = new FileOutputStream(relationFile);

        Parameter renderingParameter = new Parameter(DEFAULT_RENDERING_PARAMETER_FILE);
/*
        List<String> nodeTables = new ArrayList<>();
        nodeTables.add("shop_iconbakery");
        List<String> linesTables = new ArrayList<>();
        linesTables.add("highway_primary_lines");
        List<String> polygonTables = new ArrayList<>();
        polygonTables.add("building_apartments");
        polygonTables.add("building_polygons");
*/

        OSMClassification osmC = OSMClassification.getOSMClassification();
        List<String> nodeTables = osmC.getGenericTableNames(OHDM_DB.OHDM_POINT_GEOMTYPE);
        List<String> linesTables = osmC.getGenericTableNames(OHDM_DB.OHDM_LINESTRING_GEOMTYPE);
        List<String> polygonTables = osmC.getGenericTableNames(OHDM_DB.OHDM_POLYGON_GEOMTYPE);

        OSMFileExporter exporter = new OSMFileExporter(renderingParameter,
        nodeStream, wayStream, relationStream,
        nodeTables, linesTables, polygonTables,
        minLatString, minLongString, maxLatString, maxLongString,
        dateString);

        exporter.export();

        // close way stream
        wayStream.close();

        // re-open
        InputStream wayIS = new FileInputStream(wayFile);

        // add wayIS to nodes
        int value = wayIS.read();
        while(value != -1) {
            nodeStream.write(value);
            value = wayIS.read();
        }
        wayIS.close();
        wayFile.deleteOnExit();

        // re-open
        InputStream relationIS = new FileInputStream(relationFile);

        // add wayIS to nodes
        value = relationIS.read();
        while(value != -1) {
            nodeStream.write(value);
            value = relationIS.read();
        }
        relationIS.close();
        relationFile.deleteOnExit();

        // end osm document
        new PrintStream(nodeStream).println("</osm>");

        // close file
        nodeStream.close();
    }
}
