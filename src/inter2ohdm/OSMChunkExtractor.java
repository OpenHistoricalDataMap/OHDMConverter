package inter2ohdm;

import util.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * Created by thsc on 06.07.2017.
 */
public class OSMChunkExtractor {
    public static void main(String args[]) throws IOException, SQLException {
        // let's fill OHDM database
        System.out.println("Start chunk extracting ODHM data from intermediate DB");
        SQLStatementQueue sourceQueue = null;

        Trigger trigger = null;
        OSMExtractor extractor = null;
        OHDMImporter ohdmImporter = null;
        SQLStatementQueue targetQueue = null;

        try {
            // setup parameter default
            StringBuilder usage = new StringBuilder("use ");
            usage.append("-i intermediate_db_parameters (default: db_inter.txt)");
            String sourceParameterFileName = "db_inter.txt";

            usage.append("-d ohdm_db_parameters  (default: db_ohdm.txt)");
            String targetParameterFileName = "db_ohdm.txt";

            usage.append("-reset (if set, intermediate db is reset, ohdm tables are re-created (take care) - default: false");
            boolean reset = false;

            usage.append("-from [OSM_ID (start with this id (default: 0)] ");
            long fromOSM_ID = 0;

            usage.append("-to [OSM_ID (stop before(!) this id (default: maxID] ");
            long toOSM_ID = 0;

            usage.append("-nodes (nodes are imported) - default: false");
            boolean importNodes = false;

            usage.append("-ways (ways are imported) - default: false");
            boolean importWays = false;

            usage.append("-relations (relations are imported) - default: false");
            boolean importRelations = false;

            // now get real parameters
            HashMap<String, String> argumentMap = Util.parametersToMap(args, false, usage.toString());

            try {
                if(argumentMap != null) {
                    // got some - overwrite defaults
                    String value = argumentMap.get("-i");
                    if (value != null) {
                        sourceParameterFileName = value;
                    }

                    value = argumentMap.get("-d");
                    if (value != null) {
                        targetParameterFileName = value;
                    }

                    reset = argumentMap.containsKey("-reset");

                    value = argumentMap.get("-from");
                    if (value != null) {
                        fromOSM_ID = Integer.parseInt(value);
                    }

                    value = argumentMap.get("-to");
                    if (value != null) {
                        toOSM_ID = Integer.parseInt(value);
                    }

                    importNodes = argumentMap.containsKey("-nodes");
                    importWays = argumentMap.containsKey("-ways");
                    importRelations = argumentMap.containsKey("-relations");

                    // only one type can be extracted at once
                    int counter = 0;
                    counter = importNodes ? counter+1 : counter;
                    counter = importWays ? counter+1 : counter;
                    counter = importRelations ? counter+1 : counter;

                    if(counter != 1) {
                        System.err.println("only one type (nodes, ways, relations) can be extracted at once");
                        System.err.println("importNodes == " + importNodes);
                        System.err.println("importWays == " + importWays);
                        System.err.println("importRelations == " + importRelations);
                        System.exit(0);
                    }
                }
            }
            catch(RuntimeException re) {
                System.err.println("error while parsing parameters: " + re.getLocalizedMessage());
                System.exit(0);
            }

            Parameter sourceParameter = new Parameter(sourceParameterFileName);
            Parameter targetParameter = new Parameter(targetParameterFileName);

            Connection sourceConnection = DB.createConnection(sourceParameter);
            Connection targetConnection = DB.createConnection(targetParameter);

            IntermediateDB intermediateDB = new IntermediateDB(sourceConnection, sourceParameter.getSchema());

            String sourceSchema = sourceParameter.getSchema();
            String targetSchema = targetParameter.getSchema();

            System.out.println("intermediate update queue uses jdbc");
            SQLStatementQueue updateQueue = new SQLStatementQueue(sourceParameter);

            // Importer to OHDM database
            ohdmImporter = new OHDMImporter(intermediateDB, targetParameter.getOsmfilecreationdate(),
                    sourceConnection,
                    targetConnection, sourceSchema, targetSchema, updateQueue);

            int stepLen = 10000; // default
            String stepLenString = sourceParameter.getReadStepLen();
            try {
                if (stepLenString != null) {
                    stepLen = Integer.parseInt(stepLenString);
                }
            } catch (NumberFormatException e) {
                // ignore and work with default
            }

            // extractor from intermediate to importer
            extractor = new OSMExtractor(sourceConnection, sourceSchema, ohdmImporter, stepLen);
            
            try {
                if (reset) {
/*
                    System.out.println("remove ohdm entries in intermediate database");

                    // reset can take a while: log each 40 minutes
                    trigger = new Trigger(extractor, 1000 * 60 * 30);
                    trigger.start();
                    ohdmImporter.forgetPreviousNodesImport();
                    ohdmImporter.forgetPreviousWaysImport();
                    ohdmImporter.forgetPreviousRelationsImport();
 */

                    System.out.println("drop and re-create ohdm database");
                    // reset ohdm
                    OHDM_DB.dropOHDMTables(targetConnection, targetSchema);

                    // setup
                    OHDM_DB.createOHDMTables(targetConnection, targetSchema);
                    
                    // stop trigger
//                    trigger.end();
                }
            } catch (Exception e) {
                System.err.println("problems during setting old data (non-fatal): " + e.getLocalizedMessage());
                System.exit(0);
            }

            System.out.println("intermediate select queue uses jdbc");
            sourceQueue = DB.createSQLStatementQueue(sourceConnection, sourceParameter);

            System.out.println("ohdm insert queue uses jdbc");
            targetQueue = new SQLStatementQueue(targetParameter);

            System.out.println("start insert data into ohdm DB from intermediate DB");

            // start stats trigger each [interval] minutes (default 5)
            int logMessageInterval = targetParameter.getLogMessageInterval();
            trigger = new Trigger(extractor, 1000 * 60 * logMessageInterval);
            trigger.start();

            if (importNodes) {
                extractor.processNodes(sourceQueue, true, fromOSM_ID, toOSM_ID);
                ohdmImporter.forceExecute();
            } else if(importWays) {
                extractor.processWays(sourceQueue, false, fromOSM_ID, toOSM_ID);
                ohdmImporter.forceExecute();
            } else if (importRelations) {
                extractor.processRelations(sourceQueue, false, fromOSM_ID, toOSM_ID);
                ohdmImporter.forceExecute();

                // TODO
//                ohdmImporter.postProcessGGTable();
            }

        } catch (IOException | SQLException e) {
            Util.printExceptionMessage(e, sourceQueue, "main method in OSMChunkExtractor", false);
        } finally {
            if (trigger != null) {
                trigger.end();
                trigger.interrupt();
            }

            if (targetQueue != null) {
                targetQueue.forceExecute();
                targetQueue.join();
            }
            if (ohdmImporter != null) ohdmImporter.close();
        }
    }
}
