package osm2inter;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;
import util.Parameter;

/**
 *
 * @author thsc
 */
public class OSM2Inter {
    private static final String CONFIG_FILE_NAME = "config.xml";
    private static final String WHITELIST_FILE_NAME = "whitelist.xml";
    private static final String SUBDIR_SEPARATER = "\\";
    private static final String DEFAULT_CONFIG_DIR = "conf/";
    private static final String DEFAULT_OSM_FILENAME = "sample.osm";
    
    private static final String INTER_DB_SETTINGS_FILENAME = "db_inter.txt";

  public static void main(String[] args) {
    try {
      SAXParserFactory spf = SAXParserFactory.newInstance();
      SAXParser newSAXParser = spf.newSAXParser();
      
//      String configDir = DEFAULT_CONFIG_DIR;
//      if(args.length > 0) {
//          configDir = args[0];
//      }
      
//      String configFileName = configDir + SUBDIR_SEPARATER + CONFIG_FILE_NAME;
//      String whiteListFileName = configDir + SUBDIR_SEPARATER + WHITELIST_FILE_NAME;
//      
//      newSAXParser.parse(new File(configFileName), Config.getInstance());
//      newSAXParser.parse(new File(whiteListFileName), Whitelist.getInstance());
      
      String osmFileName = DEFAULT_OSM_FILENAME;
      if(args.length > 1) {
          osmFileName = args[0];
      }
      
      File osmFile = new File(osmFileName);

      MyLogger.getInstance().print(0, "+++ OSM Update WIzard +++", true);
  
    
    String parameterFile = INTER_DB_SETTINGS_FILENAME;
    if(args.length > 0) {
        parameterFile = args[1];
    }
    
    Parameter dbConnectionSettings = new Parameter(parameterFile);
      
    newSAXParser.parse(osmFile, new OSMImporter(dbConnectionSettings));
      
    } catch (ParserConfigurationException ex) {
      Logger.getLogger(OSM2Inter.class.getName()).log(Level.SEVERE, null, ex);
    } catch (SAXException ex) {
      Logger.getLogger(OSM2Inter.class.getName()).log(Level.SEVERE, null, ex);
    } catch (IOException ex) {
      Logger.getLogger(OSM2Inter.class.getName()).log(Level.SEVERE, null, ex);
    } catch (Exception ex) {
      Logger.getLogger(OSM2Inter.class.getName()).log(Level.SEVERE, null, ex);
    }
    MyLogger.getInstance().print(0, "+++ OSM Update Wizard finished import +++", true);
    }
}
