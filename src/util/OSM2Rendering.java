package util;


import inter2ohdm.Inter2OHDM;
import java.io.IOException;
import java.sql.SQLException;
import ohdm2rendering.OHDM2Rendering;
import osm2inter.OSM2Inter;

/**
 *
 * @author thsc
 */
public class OSM2Rendering {

    public static void main(String[] args) throws IOException, SQLException {
        if(args.length < 4) {
            System.err.println("invalid number of parameters (expected 4): " + args.length);
            System.err.println("use: osm-filename interDBConfig ohdmDBConfig renderingDBConfig");
            System.exit(0);
        }
        
        String osmFile = args[0];
        String interDBConfig = args[1];
        String ohdmDBConfig = args[2];
        String renderingDBConfig = args[3];
        
        OSM2Inter.main(new String[]{osmFile, interDBConfig});
        Inter2OHDM.main(new String[]{interDBConfig, ohdmDBConfig});
        OHDM2Rendering.main(new String[]{ohdmDBConfig, renderingDBConfig});
    }
}
