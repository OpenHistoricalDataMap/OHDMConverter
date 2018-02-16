package osm2inter;

import util.*;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;

/**
 *
 * @author thsc
 * @author FlorianSauer
 */
@SuppressWarnings("Duplicates")
public class OSMImport {
    private static final String DEFAULT_OSM_FILENAME = "test.osm";
    private static final String INTER_DB_SETTINGS_FILENAME = "db_inter.txt";

    public static void main(String[] args) throws SQLException {
        System.out.println("Started with arguments: "+Arrays.toString(args));
        HashMap<String, CopyConnector> connectors = null;
        Parameter dbConnectionSettings = null;
        long past = System.currentTimeMillis();
        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            SAXParser newSAXParser = spf.newSAXParser();
            String osmFileName = DEFAULT_OSM_FILENAME;
            if(args.length > 0) {
                osmFileName = args[0];
                System.out.println("you selected the custom osmFileName "+osmFileName);
            } else {
                System.out.println("using osmFileName "+osmFileName);
            }

            File osmFile = new File(osmFileName);

            String parameterFile = INTER_DB_SETTINGS_FILENAME;
            if(args.length > 1) {
                parameterFile = args[1];
                System.out.println("you selected the custom parameterFile "+parameterFile);
            } else {
                System.out.println("using parameterFile "+parameterFile);
            }

            dbConnectionSettings = new Parameter(parameterFile);
//            OSMClassification osmClassification = OSMClassification.getOSMClassification();
            System.out.println("schema: "+dbConnectionSettings.getSchema());
            System.out.println("connection type: "+dbConnectionSettings.getConnectionType());
            System.out.println("delimiter: "+dbConnectionSettings.getDelimiter());

            System.out.println("creating connections");
            connectors = new HashMap<>();
            String[] tablenames = COPY_OSMImporter.connsNames;
            for (String tablename : tablenames){
                connectors.put(tablename, new CopyConnector(dbConnectionSettings, tablename));
            }

            // 2BTested
//            newSAXParser.parse(osmFile, new XML_SAXHandler(connectors));
            System.out.println("starting parser");
            // replacing SQL importer with COPY importer
            newSAXParser.parse(osmFile, new COPY_OSMImporter(connectors, dbConnectionSettings.getSerTagsSize()));

            for (CopyConnector connector : connectors.values()){
                System.out.println("wrote "+connector.endCopy()+" lines to "+connector.getTablename());
                connector.close();
            }
//            newSAXParser.parse(osmFile, new SQL_OSMImporter(dbConnectionSettings, osmClassification));
//        newSAXParser.parse(osmFile, new SQL_OSMImporter(dbConnectionSettings, osmClassification));
//        newSAXParser.parse(osmFile, new Dump_OSMImporter(dbConnectionSettings, osmClassification));

        } catch (Throwable t) {
            PrintStream err = System.err;
            // maybe another stream was defined and could be opened
            try {
                err = dbConnectionSettings.getErrStream();
            }
            catch(Throwable tt) {
                // ignore that..
            }

            Util.printExceptionMessage(err, t, null, "in main OSM2Inter", false);
        }
        long present = System.currentTimeMillis();
        System.out.println("That took "+(present-past)+" ms");
    }
}
