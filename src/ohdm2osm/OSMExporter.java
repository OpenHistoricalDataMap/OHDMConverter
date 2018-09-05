package ohdm2osm;

import java.io.DataOutputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import util.Parameter;

/**
 *
 * @author thsc
 */
public class OSMExporter {
    
    private final Connection sourceConnection;
    private final Parameter exportParameters;
    private final File tmpDirectory;
    private final DataOutputStream targetStream;

    /**
     * 
     * @param sourceConnection ohdm database connection
     * @param exportParameters parameters (required?)
     * @param tmpDirectory directory to keep three tmp file: nods, ways, relations
     * @param targetStream final osm data are streamed into that output stream
     */
    public OSMExporter(Connection sourceConnection, Parameter exportParameters,
           File tmpDirectory, DataOutputStream targetStream) {
        
        this.sourceConnection = sourceConnection;
        this.exportParameters = exportParameters;
        this.tmpDirectory = tmpDirectory;
        this.targetStream = targetStream;
    }

    void export() {
        // crete three tmp files
        File nodesFile, waysFile, relationsFile;
        DataOutputStream nodesDOS, waysDOS, relationsDOS;
        
        Date valid;
        
        // get all nodes which are valid at that date from database
        
        // export all nodes which into nodes files
        
        /*
        id = ohdm id
        user = source_user
        ... to be continued
        */
    }
    
    public static void main(String[] args) {
        System.out.println("this becomes the export tool of OHDM to OSM data");
        
        OSMExporter exporter = new OSMExporter(null, null, null, null);
        
        exporter.export();
    }
}
