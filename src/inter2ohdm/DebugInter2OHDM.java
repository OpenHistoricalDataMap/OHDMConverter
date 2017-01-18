package inter2ohdm;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import util.DB;
import util.Parameter;
import util.SQLStatementQueue;
import util.Util;

/**
 *
 * @author thsc
 */
public class DebugInter2OHDM extends Inter2OHDM {

    public DebugInter2OHDM(IntermediateDB intermediateDB, Connection sourceConnection, Connection targetConnection, String sourceSchema, String targetSchema) {
        super(intermediateDB, sourceConnection, targetConnection, sourceSchema, targetSchema);
    }
    
    public static void main(String args[]) throws IOException {
        SQLStatementQueue sourceQueue = null;
        
        System.out.println(Util.getIntWithDots(1000000));
        System.exit(0);
        
        try {
            String sourceParameterFileName = "db_inter_f4_test.txt";
            String targetParameterFileName = "db_ohdm_local.txt";
            
            if(args.length > 0) {
                sourceParameterFileName = args[0];
            }
            
            if(args.length > 1) {
                targetParameterFileName = args[1];
            }
            
//            Connection sourceConnection = Importer.createLocalTestSourceConnection();
//            Connection targetConnection = Importer.createLocalTestTargetConnection();
            Parameter sourceParameter = new Parameter(sourceParameterFileName);
            Parameter targetParameter = new Parameter(targetParameterFileName);
            
            Connection sourceConnection = Importer.createConnection(sourceParameter);
            Connection targetConnection = Importer.createConnection(targetParameter);
            
            IntermediateDB iDB = new IntermediateDB(sourceConnection, sourceParameter.getSchema());
            
            String sourceSchema = sourceParameter.getSchema();
            String targetSchema = targetParameter.getSchema();
            
            Inter2OHDM ohdmImporter = new Inter2OHDM(iDB, sourceConnection, 
                    targetConnection, sourceSchema, targetSchema);
            
//            try {
//                ohdmImporter.forgetPreviousImport();
//            }
//            catch(Exception e) {
//                System.err.println("problems during setting old data (non-fatal): " + e.getLocalizedMessage());
//            }
            
//            try {
//                ohdmImporter.dropOHDMTables(targetConnection);
//            }
//            catch(Exception e) {
//                System.err.println("problems during setting old data (non-fatal): " + e.getLocalizedMessage());
//            }
            
//            ohdmImporter.createOHDMTables(targetConnection);
            
            String stepLenString = sourceParameter.getReadStepLen();
            int stepLen = 10000;
            try {
                if(stepLenString != null) {
                    stepLen = Integer.parseInt(stepLenString);
                }
            }
            catch(NumberFormatException e) {
                    // ignore and work with default
            }

            ExportIntermediateDB exporter = 
                    new ExportIntermediateDB(sourceConnection, sourceSchema, ohdmImporter, stepLen);
            
            sourceQueue = DB.createSQLStatementQueue(sourceConnection, sourceParameter);
            
            ResultSet qResult = null; 
            // do some sql here..
//            sourceQueue.append("SELECT * FROM intermediate.ways where osm_id = 186022626;");
            sourceQueue.append("SELECT * FROM intermediate.relations;");
            qResult = sourceQueue.executeWithResult();
            
            while(qResult.next()) {
//            exporter.processNode(qResult, sourceQueue, ExportIntermediateDB.RELATION);
//            exporter.processWay(qResult, sourceQueue, ExportIntermediateDB.RELATION);
            exporter.processElement(qResult, sourceQueue, ExportIntermediateDB.RELATION);
                
            }
        } catch (Exception e) {
            Util.printExceptionMessage(e, sourceQueue, "main method in DebugInter2OHDM", false);
        }
    }
}
