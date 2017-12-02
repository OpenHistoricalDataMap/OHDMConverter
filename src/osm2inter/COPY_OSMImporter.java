package osm2inter;

import org.xml.sax.helpers.DefaultHandler;
import util.DBCopyConnector;

import java.util.HashMap;

public class COPY_OSMImporter extends DefaultHandler {

    public COPY_OSMImporter(HashMap<String, DBCopyConnector> conns){}
    public static final String[] connsNames = { "nodes", "relationmember", "relations", "waynodes", "ways" };

    // TODO: 02.12.2017 wait for jan to finish the parser
}


