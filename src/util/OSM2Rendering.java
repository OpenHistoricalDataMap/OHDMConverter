package util;


import inter2ohdm.OSMExtract;
import java.io.IOException;
import java.io.PrintStream;
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
            /* at least two parameter are required which are 
            defined with at least four arguments
            */
            OSM2Rendering.printUsageAndExit("at least two parameters are required");
        }
        
        String osmFile = null;
        String importInterDBConfig = null;
        String updateInterDBConfig = null;
        String ohdmDBConfig = null;
        String renderingDBConfig = null;
        
        int i = 0;
        while(i < args.length-1) {
            switch(args[i]) { 
                case "-o": // osm file
                    osmFile = args[i+1];
                    break;
                case "-i": // import - intermediate db
                    importInterDBConfig = args[i+1];
                    break;
                case "-u": // update - intermediate db
                    updateInterDBConfig = args[i+1];
                    break;
                case "-d": // ohdm database
                    ohdmDBConfig = args[i+1];
                    break;
                case "-r": // rendering data base
                    renderingDBConfig = args[i+1];
                    break;
            }
            
            i += 2;
        }
        
        // check consistency
        
        // decide wether import or exit
        if(importInterDBConfig != null && updateInterDBConfig != null) {
            OSM2Rendering.printUsageAndExit("cannot import and update simultaneously");
        }
        
        // unclear what to do: import / update into intermediate db or import / update ohdm from intermediate db
        if( (importInterDBConfig != null || updateInterDBConfig != null) && osmFile == null && ohdmDBConfig == null)  {
            OSM2Rendering.printUsageAndExit("unclear what to do: import / update into intermediate db or import / update ohdm from intermediate db");
        }
        
        // osm file but neither import nor update declared
        if( importInterDBConfig == null && updateInterDBConfig == null && osmFile != null)  {
            OSM2Rendering.printUsageAndExit("osm file declared but no import or update configuration");
        }
        
        // unclear what to do: import into ohdm or create rendering database out of ohdm
        if( ohdmDBConfig != null && importInterDBConfig == null && updateInterDBConfig == null && renderingDBConfig == null)  {
            OSM2Rendering.printUsageAndExit("unclear what to do: import into ohdm or create rendering database out of ohdm");
        }
        
        // producing rendering requires definition of ohdm database
        if( renderingDBConfig != null && ohdmDBConfig == null)  {
            OSM2Rendering.printUsageAndExit("producing rendering requires definition of ohdm database");
        }
        
        // debug
        System.err.println("osmFile: " + osmFile);
        System.err.println("importInterDBConfig: " + importInterDBConfig);
        System.err.println("updateInterDBConfig: " + updateInterDBConfig);
        System.err.println("ohdmDBConfig: " + ohdmDBConfig);
        System.err.println("renderingDBConfig: " + renderingDBConfig);
        
        OSM2Rendering.printMessage("start processes:");
        if(importInterDBConfig != null && osmFile != null) {
            OSM2Rendering.printMessage("import from osm file into intermediate db");
        }
        
        if(updateInterDBConfig != null && osmFile != null) {
            OSM2Rendering.printMessage("update intermediate db from osm file");
        }
        
        if(importInterDBConfig != null && ohdmDBConfig != null) {
            OSM2Rendering.printMessage("import from intermediate db into ohdm db");
        }
        
        if(updateInterDBConfig != null && ohdmDBConfig != null) {
            OSM2Rendering.printMessage("update ohdm db from intermediate db");
        }
        
        if(renderingDBConfig != null && ohdmDBConfig != null) {
            OSM2Rendering.printMessage("produce rendering db");
        }
        
        if(osmFile != null && importInterDBConfig != null) {
            OSM2Inter.main(new String[]{osmFile, importInterDBConfig});
        }

        if(importInterDBConfig != null &&  ohdmDBConfig != null) {
            OSMExtract.main(new String[]{importInterDBConfig, ohdmDBConfig});
        }
        
        if(updateInterDBConfig != null &&  ohdmDBConfig != null) {
            // TODO
//            SQL_OSM2Inter_Updater.main(new String[]{updateInterDBConfig, ohdmDBConfig});
        }
        
        if(ohdmDBConfig != null &&  renderingDBConfig != null) {
            OHDM2Rendering.main(new String[]{ohdmDBConfig, renderingDBConfig});
        }
    }

    private static void printUsageAndExit(String message) {
        OSM2Rendering.printUsageAndExit(message, System.err);
    }
    
    private static void printMessage(String message) {
        OSM2Rendering.printMessage(message, System.err);
    }
    
    private static void printMessage(String message, PrintStream out) {
        out.println(message);
    }
    
    private static void printUsageAndExit(String message, PrintStream out) {
        out.println("Failure:");
        out.println(message);
        out.println("\nUsage:");
        OSM2Rendering.printUsageAndExit(out);
    }
    
    private static void printUsageAndExit() {
        OSM2Rendering.printUsageAndExit(System.out);
    }
    
    private static void printUsageAndExit(PrintStream out) {
        out.println("-o [osmfilename]");
        out.println("-i [parameter file intermediateDB import]");
        out.println("-u [parameter file intermediateDB update]");
        out.println("-d [parameter file OHDM DB]");
        out.println("-r [parameter file rendering DB]");
        out.println("Note 1: -i and -u exclude each other: It's either an import or an update, never both");
        out.println("Note 2: The process comprises up to three steps");
        out.println("1) Import or update OSM file to intermediate DB (requires -o and (-i or -u) )");
        out.println("2) Import/Update OHDM from intermediate DB (requires (-i or -u) and -d)");
        out.println("3) Create rendering tables from OHDM from intermediate DB (requires -d und -r)");
        out.println("Each step is performed if all required parameters are found");
        out.println("Enter e.g. all parameter to create rendering tables from an OSM file");
        out.println("Enter only e.g. -o, -u and -d to update OHDM with a new .osm file");
        out.println("Enter only e.g. -d and -r to produce new rendering table from OHDM");
        System.exit(0);
    }
}
