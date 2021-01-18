package rendering2rdf;

import util.DB;
import util.OHDM_DB;
import util.Parameter;
import util.SQLStatementQueue;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Rendering2rdf {
    private final Parameter sourceParameter;
    private final SQLStatementQueue sqlQueue;
    private int lfd = 0;
    private BufferedWriter out;
    private Set<String> subClasses;


    public Rendering2rdf(Parameter sourceParameter, String filename)
            throws IOException, SQLException {
        this.sourceParameter = sourceParameter;
        this.subClasses = new HashSet<String>();
        this.sqlQueue = new SQLStatementQueue(this.sourceParameter);

    }

    public static void main(String[] args) throws IOException, SQLException {
        String sourceParameterFileName = "db_rendering.txt";
        String filename = "RDF.ttl";
        String polygonString = null;

        if (args.length > 0) {
            sourceParameterFileName = args[0];
        }

        if (args.length > 1) {
            filename = args[1];
        }

        if (args.length > 2) {
            polygonString = args[2];
            System.err.println("POLYGON not yet supported (working on it)");
        }

        Rendering2rdf rendering2rdf = new Rendering2rdf(new Parameter(sourceParameterFileName), filename);


        rendering2rdf.produceTurtle(filename);

    }

    void produceTurtle(String filename) throws IOException, SQLException {
        this.out = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(filename), StandardCharsets.UTF_8));

        // write preamble
        this.out.write("@prefix ohdm: <http://www.ohdm.net/ohdm/schema/1.0> . \n");
        this.out.write("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . \n");
        this.out.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . \n");
        this.out.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . \n");
        this.out.write("@prefix geo: <http://www.opengis.net/ont/geosparql#> . \n");
        this.out.write("@prefix geom:  <http://example.org/Geometry#> . \n");
        this.out.write("@prefix sf:    <http://www.opengis.net/ont/sf#> . \n");

        ResultSet tables = getAllTableNames();
        int i = 1;
        while (tables.next()) {
            String str = tables.getString(i);
            if (str.contains("lines"))
                this.produceTurtle(str, OHDM_DB.OHDM_LINESTRING_GEOMTYPE);
            else if (str.contains("points"))
                this.produceTurtle(str, OHDM_DB.OHDM_POINT_GEOMTYPE);
            else if (str.contains("polygons"))
                this.produceTurtle(str, OHDM_DB.OHDM_POLYGON_GEOMTYPE);
            else {
                //faulty tables that shouldn't be there, tables got created by conversion from ohdm to render db and weren't dropped properly later
                System.out.println("faulty table");
            }
        }

        Iterator<String> it = subClasses.iterator();

        while(it.hasNext()){
            StringBuilder b = new StringBuilder();
            b.append("ohdm:").append(it.next());
            b.append(" a rdfs:class ; \n");
            b.append(" rdfs:subclassOf geo:Feautre . \n");
            this.out.write(b.toString());
        }

        // close file
        this.out.close();
    }

    private ResultSet getAllTableNames() throws SQLException {
        /*
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'rendering'
        */

        this.sqlQueue.append("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + sourceParameter.getSchema() + "'");
        return this.sqlQueue.executeWithResult();
    }


    void produceTurtle(String tableName, int geomType) throws SQLException, IOException {
        /*
        select object_id, geom_id,
        classid, name, valid_since, valid_until,
        from 'param';
        */
        String geotype = "";

        this.sqlQueue.append("SELECT object_id, geom_id, classid, name, subclassname, valid_since, valid_until, st_astext(st_transform(");
        switch (geomType) {
            case OHDM_DB.OHDM_POINT_GEOMTYPE:
                this.sqlQueue.append("point");
                geotype = "Point";
                break;
            case OHDM_DB.OHDM_LINESTRING_GEOMTYPE:
                this.sqlQueue.append("line");
                geotype = "LineString";
                break;
            case OHDM_DB.OHDM_POLYGON_GEOMTYPE:
                this.sqlQueue.append("polygon");
                geotype = "Polygon";
                break;
        }

        this.sqlQueue.append(", 4326)) FROM ");
        this.sqlQueue.append(DB.getFullTableName(this.sourceParameter.getSchema(), tableName));
        this.sqlQueue.append(";");

        ResultSet qResult = this.sqlQueue.executeWithResult();
        int count = 1;
        while (qResult.next()) {
            this.writeTurtleEntry(
                    geotype,
                    count,
                    qResult.getBigDecimal(1),
                    qResult.getBigDecimal(2),
                    qResult.getBigDecimal(3),
                    qResult.getString(4),
                    qResult.getString(5),
                    qResult.getDate(6),
                    qResult.getDate(7),
                    qResult.getString(8));
            count++;
        }


    }

    private void writeTurtleEntry(String geotype, int count, BigDecimal objectID, BigDecimal geomID, BigDecimal classificationID,
                                  String name, String subclass, Date since, Date until, String wkt) throws IOException {

        this.subClasses.add(subclass);
        StringBuilder b = new StringBuilder(); // good for debugging

        /*
        template:
        _:ID1 a ohdm:subClass ;
         ohdm:name "AName" ;
         ohdm:objectid "42"^^xsd:integer ;
         ohdm:geometryid "42"^^xsd:integer ;
         ohdm:classificationid "42"^^xsd:integer ;
         ohdm:validsince "2001-01-01"^^xsd:date ;
         ohdm:validuntil "2010-01-01"^^xsd:date ;
         geo:hasGeometry geom:subClass_ID1 .
         geom:subClass_ID1 a sf:LineString ;
         geo:asWKT "LINESTRING(13.5 52.4, 13.6 52.5) <http://www.opengis.net/def/crs/OGC/1.3/CRS84>"^^geo:wktLiteral .
         */

        b.append("_:ID");
        b.append(this.lfd);
        b.append(" a ohdm:").append(subclass);
        b.append(" ;\n");

        if (name != null) {
            // name
            b.append(" ohdm:name '");
            b.append(name);
            b.append("'");
            b.append(" ;\n");
        }

        // osm object ID
        b.append(" ohdm:objectID \"");
        b.append(objectID.toString());
        b.append("\"^^xsd:integer ;\n");

        // geom ID
        b.append(" ohdm:geometryID \"");
        b.append(geomID.toString());
        b.append("\"^^xsd:integer ;\n");

        // classID
        b.append(" ohdm:classificationID \"");
        b.append(classificationID.toString());
        b.append("\"^^xsd:integer ;\n");

        // validsince
        b.append(" ohdm:validsince \"");
        b.append(since.toString()); // sql.Date.toString produces "yyyy-mm-tt" not util.Date!!!
        b.append("\"^^xsd:date ;\n");

        // validuntil
        b.append(" ohdm:validuntil \"");
        b.append(until.toString()); // sql.Date.toString produces "yyyy-mm-tt" not util.Date!!!
        b.append("\"^^xsd:date ;\n");

        // hasGeometry
        b.append(" geo:hasGeometry ");
        b.append("geom:").append(subclass).append("_ID").append(count);
        b.append(" . \n");

        // geometry type
        b.append("geom:").append(subclass).append("_ID").append(count);
        b.append(" a ");
        b.append(" sf:").append(geotype).append(" ;");
        b.append("\n");

        // wkt string
        b.append(" geo:asWKT \"");
        b.append("<http://www.opengis.net/def/crs/OGC/1.3/CRS84> ");
        b.append(wkt);
        b.append("\"^^geo:wktLiteral");

        b.append(" .\n");
        this.lfd++;

        this.out.write(b.toString());
    }
}
