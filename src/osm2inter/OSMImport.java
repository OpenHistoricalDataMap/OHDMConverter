package osm2inter;

import org.xml.sax.helpers.DefaultHandler;
import osm.OSMClassification;
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
            
            SQLStatementQueue sq = new SQLStatementQueue(dbConnectionSettings);
            // drop database
            System.out.println("drop and recreate intermediate tables");
            InterDB.createTables(sq, dbConnectionSettings.getSchema());

            // set up xml handler - class that actually does the importing
            DefaultHandler osmImporter = null;

            String connectionType = dbConnectionSettings.getConnectionType();
            if(!connectionType.equalsIgnoreCase("insert")) {
                System.out.println("use copy insert - fast version");

                System.out.println("creating connections");
                connectors = new HashMap<>();
                String[] tablenames = COPY_OSMImporter.connsNames;
                for (String tablename : tablenames){
                    connectors.put(tablename, new CopyConnector(dbConnectionSettings, tablename));
                }

                osmImporter = new COPY_OSMImporter(connectors, dbConnectionSettings.getSerTagsSize());
            } else {
                // do inserts
                System.out.println("use sql-insert - copy is much faster!");

                osmImporter = new SQL_OSMImporter(
                        dbConnectionSettings,
                        OSMClassification.getOSMClassification());
            }

            System.out.println("starting parser");
            newSAXParser.parse(osmFile, osmImporter);

            if(!connectionType.equalsIgnoreCase("insert")) {
                for (CopyConnector connector : connectors.values()) {
                    System.out.println("wrote " + connector.endCopy() + " lines to " + connector.getTablename());
                    connector.close();
                }
            }

        } catch (Exception t) {
            PrintStream err = System.err;
            // maybe another stream was defined and could be opened
            try {
                err = dbConnectionSettings.getErrStream();
            }
            catch(Exception tt) {
            }

            Util.printExceptionMessage(err, t, null, "in main OSM2Inter", false);
        }
        long present = System.currentTimeMillis();
        System.out.println("That took "+(present-past)+" ms");
    }
}
