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

  public static void main(String[] args) {
    try {
      SAXParserFactory spf = SAXParserFactory.newInstance();
      SAXParser newSAXParser = spf.newSAXParser();
      
      newSAXParser.parse(new File("conf/whitelist.xml"), Whitelist.getInstance());
      newSAXParser.parse(new File("conf/config.xml"), Config.getInstance());
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
  }

}
