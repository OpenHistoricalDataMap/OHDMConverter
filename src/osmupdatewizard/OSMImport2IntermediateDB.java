package osmupdatewizard;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;

/**
 *
 * @author thsc
 */
public class OSMImport2IntermediateDB {
    private static final String CONFIG_FILE_NAME = "config.xml";
    private static final String WHITELIST_FILE_NAME = "whitelist.xml";
    private static final String SUBDIR_SEPARATER = "\\";
    private static final String DEFAULT_CONFIG_DIR = "conf/";
    private static final String DEFAULT_OSM_FILENAME = "sample.osm";
    

  public static void main(String[] args) {
    try {
      SAXParserFactory spf = SAXParserFactory.newInstance();
      SAXParser newSAXParser = spf.newSAXParser();
      
      String configDir = DEFAULT_CONFIG_DIR;
      if(args.length > 0) {
          configDir = args[0];
      }
      
      String configFileName = configDir + SUBDIR_SEPARATER + CONFIG_FILE_NAME;
      String whiteListFileName = configDir + SUBDIR_SEPARATER + WHITELIST_FILE_NAME;
      
      newSAXParser.parse(new File(configFileName), Config.getInstance());
      newSAXParser.parse(new File(whiteListFileName), Whitelist.getInstance());
      
      String osmFileName = DEFAULT_OSM_FILENAME;
      if(args.length > 1) {
          osmFileName = args[1];
      }
      
      File osmFile = new File(osmFileName);

      MyLogger.getInstance().print(0, "+++ OSM Update WIzard +++", true);
      
      newSAXParser.parse(osmFile, new OSMImporter());
      
    } catch (ParserConfigurationException ex) {
      Logger.getLogger(OSMImport2IntermediateDB.class.getName()).log(Level.SEVERE, null, ex);
    } catch (SAXException ex) {
      Logger.getLogger(OSMImport2IntermediateDB.class.getName()).log(Level.SEVERE, null, ex);
    } catch (IOException ex) {
      Logger.getLogger(OSMImport2IntermediateDB.class.getName()).log(Level.SEVERE, null, ex);
    } catch (Exception ex) {
      Logger.getLogger(OSMImport2IntermediateDB.class.getName()).log(Level.SEVERE, null, ex);
    }
    MyLogger.getInstance().print(0, "+++ OSM Update Wizard finished import +++", true);
    }
}
