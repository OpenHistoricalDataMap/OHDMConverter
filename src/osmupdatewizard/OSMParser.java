package osmupdatewizard;

import java.io.File;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;

/**
 *
 * @author thsc
 */
public class OSMParser {

  public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, Exception {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    SAXParser newSAXParser = spf.newSAXParser();

    newSAXParser.parse(new File("conf/whitelist.xml"), Whitelist.getInstance());
    newSAXParser.parse(new File("conf/config.xml"), Config.getInstance());
    
    ImportCommandBuilder builder = new SQLImportCommandBuilder();
    
    //System.out.println("Config: " + Config.getInstance().toString());
    //System.out.println("Whitelist: " + Whitelist.getInstance().toString());

    /*System.out.println("dump: " + whitelist.toString());
     System.out.println("test reduce fail: " + whitelist.reduce("highway", "bla"));
     System.out.println("test reduce success: " + whitelist.reduce("highway", "traffic_signals"));*/

    /*OSMImporter importer = new OSMImporter();
     File f = new File("bremen-latest.osm");
     newSAXParser.parse(f, importer);*/
  }

}
