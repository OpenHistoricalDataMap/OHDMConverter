package osm2inter;

import java.io.File;
import java.io.PrintStream;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import osm.OSMClassification;
import util.Parameter;
import util.Util;

/**
 *
 * @author thsc
 */
public class OSM2Inter {
    private static final String DEFAULT_OSM_FILENAME = "sample.osm";
    private static final String INTER_DB_SETTINGS_FILENAME = "db_inter.txt";

    public static void main(String[] args) {
        Parameter dbConnectionSettings = null;
        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            SAXParser newSAXParser = spf.newSAXParser();

            String osmFileName = DEFAULT_OSM_FILENAME;
            if(args.length > 1) {
                osmFileName = args[0];
            }

            File osmFile = new File(osmFileName);

            String parameterFile = INTER_DB_SETTINGS_FILENAME;
        if(args.length > 0) {
            parameterFile = args[1];
        }

        dbConnectionSettings = new Parameter(parameterFile);
        OSMClassification osmClassification = OSMClassification.getOSMClassification();

        // 2BTested
        newSAXParser.parse(osmFile, new FileSQL_OSMImporter(dbConnectionSettings, osmClassification));
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
    }
}
