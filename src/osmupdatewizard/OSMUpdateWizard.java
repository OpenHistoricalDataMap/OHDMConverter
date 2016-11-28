package osmupdatewizard;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
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
      Logger.getLogger(OSMUpdateWizard.class.getName()).log(Level.SEVERE, null, ex);
    } catch (SAXException ex) {
      Logger.getLogger(OSMUpdateWizard.class.getName()).log(Level.SEVERE, null, ex);
    } catch (IOException ex) {
      Logger.getLogger(OSMUpdateWizard.class.getName()).log(Level.SEVERE, null, ex);
    } catch (Exception ex) {
      Logger.getLogger(OSMUpdateWizard.class.getName()).log(Level.SEVERE, null, ex);
    }
    MyLogger.getInstance().print(0, "+++ OSM Update Wizard finished import +++", true);
    
        // let's fill OHDM database
        MyLogger logger = MyLogger.getInstance();

        // connect to OHDM rendering database
        String serverName = "localhost";
        String portNumber = "5432";
        String path = "";
        String user = "";
        String pwd = "";

        try {
            Properties connProps = new Properties();
            connProps.put("user", user);
            connProps.put("password", pwd);
            Connection connection = DriverManager.getConnection(
                    "jdbc:postgresql://" + serverName
                    + ":" + portNumber + "/" + path, connProps);
          
            Intermediate2OHDMRendering renderDBFiller = 
                    new Intermediate2OHDMRendering(connection);
            
            renderDBFiller.go();
  
        } catch (SQLException e) {
          logger.print(0, "cannot connect to database: " + e.getLocalizedMessage());
        }
    }
}
