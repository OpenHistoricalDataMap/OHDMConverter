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
public class OSMUpdateWizard {
    private static final String CONFIG_FILE_NAME = "config.xml";
    private static final String WHITELIST_FILE_NAME = "whitelist.xml";

  public static void main(String[] args) {
    try {
      SAXParserFactory spf = SAXParserFactory.newInstance();
      SAXParser newSAXParser = spf.newSAXParser();
      
      String configDir = "conf/";
      if(args.length > 0) {
          configDir = args[0];
      }

      String configFileName = configDir +  CONFIG_FILE_NAME;
      String whiteListFileName = configDir +  WHITELIST_FILE_NAME;
      
      newSAXParser.parse(new File(configFileName), Config.getInstance());
      newSAXParser.parse(new File(whiteListFileName), Whitelist.getInstance());
      
      MyLogger.getInstance().print(0, "+++ OSM Update WIzard +++", true);
      newSAXParser.parse(new File(Config.getInstance().getValue("osm_sourceFile")), new OSMImporter());
    } catch (ParserConfigurationException ex) {
      Logger.getLogger(OSMUpdateWizard.class.getName()).log(Level.SEVERE, null, ex);
    } catch (SAXException ex) {
      Logger.getLogger(OSMUpdateWizard.class.getName()).log(Level.SEVERE, null, ex);
    } catch (IOException ex) {
      Logger.getLogger(OSMUpdateWizard.class.getName()).log(Level.SEVERE, null, ex);
    } catch (Exception ex) {
      Logger.getLogger(OSMUpdateWizard.class.getName()).log(Level.SEVERE, null, ex);
    }
    MyLogger.getInstance().print(0, "+++ OSM Update Wizard finished import +++", true);
  }

}
