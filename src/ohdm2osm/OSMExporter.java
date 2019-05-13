package ohdm2osm;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import util.DB;
import util.Parameter;
import util.SQLStatementQueue;

/**
 *
 * @author thsc
 */
public class OSMExporter {
    
    private final Connection sourceConnection;
    private final PrintStream nodeStream;
    private final PrintStream wayStream;
    private final int ldfID;
    private final List<String> pointTableNames;
    private final List<String> linesTableNames;
    private final List<String> polygonTableNames;
    private final Parameter sourceParameter;
    private final String bboxWKT;

    public OSMExporter(Parameter sourceParameter, OutputStream nodeOSStream,
                       OutputStream wayOSStream,
                       List<String> pointTableNames, List<String> linesTableNames,
                       List<String> polygonTableNames, String bboxWKT) throws SQLException {
        
        this.sourceParameter = sourceParameter;
        this.sourceConnection = DB.createConnection(sourceParameter);
        this.nodeStream = new PrintStream(nodeOSStream);
        this.wayStream = new PrintStream(wayOSStream);
        this.pointTableNames = pointTableNames;
        this.linesTableNames = linesTableNames;
        this.polygonTableNames = polygonTableNames;
        this.bboxWKT = bboxWKT;

        this.ldfID = 1;
    }

    private void exportNodes() {


    }

    private void exportLines() {

    }

    private void exportPolygons() {

    }

    void export() {
        SQLStatementQueue sql = new SQLStatementQueue();
        // points
        if(pointTableNames != null) {
            for(String tableName : pointTableNames) {
                sql.append("SELECT ST_X(ST_TRANSFORM(point, 4326)), ST_Y(ST_TRANSFORM(point, 4326)), subclassname, ");
                sql.append("name, valid_since FROM ");
                sql.append(util.DB.getFullTableName(this.sourceParameter.getSchema(), tableName));
                sql.append(" WHERE...");

                // HIER WEITERMACHEN: ST_WITHIN(bbox);
            }
        }

        /*
        IF POINT:
SELECT ST_X(ST_TRANSFORM(point, 4326)), ST_Y(ST_TRANSFORM(point, 4326)), subclassname, name, valid_since
FROM public.highway_points;

  -->

<node id='lfdNummer' timestamp='valid_since' uid='1' user='fake' visible='true' lat='point.' lon='13.5951339'>
<tag k='highway' v='subclassname' /></node>
         */

        /*
        WAY
SELECT st_astext(st_transform(line, 4326)), name, subclassname, valid_since
  FROM public.highway_lines;

 -> parse linestring, create for each point - remember pointsid p1, p2 etc;

<node id='lfdNummer' timestamp='valid_since' uid='1' user='fake' visible='true' lat='xxx.' lon='xxx'></node>

for way
  <way id='lfdNummer' timestamp='since' uid='1' user='fake' visible='true' version='1'>
    <nd ref='p1' />
    <nd ref='p2' />
    <tag k='highway' v='subclassname' />
  </way>
         */

        /*
POLYGON
SELECT geom_id, st_astext(st_transform((ST_ExteriorRing(polygon)),4326)), ST_NumInteriorRings(polygon), subclassname, name, valid_since
  FROM public.building_apartments limit 10;

  if num interior rings > 1 query interior rings...

SELECT geom_id, st_astext(st_transform((ST_ExteriorRing(polygon)),4326)), ST_NumInteriorRings(polygon),
st_astext(ST_InteriorRingN(polygon, 1)), subclassname, name, valid_since
  FROM public.building_apartments where ST_NumInteriorRings(polygon) > 0 limit 10;

  where geom_id IN (siehe oben)
         */
    }
    
    public static void main(String[] args) throws IOException, SQLException {

        String DEFAULT_RENDERING_PARAMETER_FILE = "db_rendering.txt";
        String OUTPUTFILENAME = "ohdm.osm";
        String bbox = "POLYGON((10 50, 10 54, 14 54, 15 50, 10 50))";

        System.out.println("this becomes the export tool of OHDM to OSM data");

        File nodeFile = new File(OUTPUTFILENAME);
        File wayFile = File.createTempFile("way", "_osm");

        OutputStream nodeStream = new FileOutputStream(nodeFile);
        OutputStream wayStream = new FileOutputStream(wayFile);

        Parameter renderingParameter = new Parameter(DEFAULT_RENDERING_PARAMETER_FILE);

        List<String> nodeTables = new ArrayList<>();
        nodeTables.add("highway_point");
        List<String> linesTables = new ArrayList<>();
        nodeTables.add("highway_lines");
        List<String> polygonTables = new ArrayList<>();
        nodeTables.add("highway_polygons");

        OSMExporter exporter = new OSMExporter(renderingParameter, nodeStream, wayStream,
                nodeTables, linesTables, polygonTables, bbox);

        exporter.export();

        // close way stream
        wayStream.close();

        // re-open
        InputStream ways = new FileInputStream(wayFile);

        // add ways to nodes
        int value = ways.read();
        while(value != -1) {
            nodeStream.write(value);
            value = ways.read();
        }

        nodeStream.close();
        ways.close();
    }
}
