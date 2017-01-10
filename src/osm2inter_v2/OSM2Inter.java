package osm2inter_v2;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;
import osm.OSMClassification;
import util.Parameter;

/**
 *
 * @author thsc
 */
public class OSM2Inter {
    private static final String DEFAULT_OSM_FILENAME = "sample.osm";
    private static final String INTER_DB_SETTINGS_FILENAME = "db_inter.txt";

    public static void main(String[] args) {
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
    
    Parameter dbConnectionSettings = new Parameter(parameterFile);
    OSMClassification osmClassification = OSMClassification.getOSMClassification();
    
    newSAXParser.parse(osmFile, new SQL_OSMImporter(dbConnectionSettings, osmClassification));
      
    } catch (ParserConfigurationException ex) {
        Logger.getLogger(OSM2Inter.class.getName()).log(Level.SEVERE, null, ex);
    } catch (SAXException ex) {
        Logger.getLogger(OSM2Inter.class.getName()).log(Level.SEVERE, null, ex);
    } catch (IOException ex) {
        Logger.getLogger(OSM2Inter.class.getName()).log(Level.SEVERE, null, ex);
    } catch (Exception ex) {
        Logger.getLogger(OSM2Inter.class.getName()).log(Level.SEVERE, null, ex);
    }
    }
}
