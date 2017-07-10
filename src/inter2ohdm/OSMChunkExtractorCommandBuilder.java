package inter2ohdm;

import util.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by thsc on 06.07.2017.
 */
public class OSMChunkExtractorCommandBuilder {

    private long minID, maxID;

    public static void main(String args[]) {
        String jarFileNames = null;

        String path = OSMChunkExtractorCommandBuilder.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        File f;
        try {
            File jarFile = new File(OSMChunkExtractorCommandBuilder.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
            jarFileNames = jarFile.getCanonicalPath();
        } catch (Exception e) {
            System.err.println("cannot figure out jar name - abort: " + e.getLocalizedMessage());
            System.exit(0);
        }

        StringBuilder usage = new StringBuilder("use ");
        usage.append("-i intermediate_db_parameters (default: db_inter.txt)");
        usage.append("\n");
        String sourceParameterFileName = "db_inter.txt";

        usage.append("-d ohdm_db_parameters  (default: db_ohdm.txt)");
        usage.append("\n");
        String targetParameterFileName = "db_ohdm.txt";

        usage.append("-size [value] (size of each chunk default: 1.000.000)");
        usage.append("\n");
        long size = 1000000;
        
        usage.append("-jar [additional jar files - optional]");
        usage.append("\n");

        // now get real parameters
        HashMap<String, String> argumentMap = Util.parametersToMap(args, false, usage.toString());
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

            value = argumentMap.get("-size");
            if (value != null) {
                size = Integer.parseInt(value);
            }
            
            value = argumentMap.get("-jar");
            if (value != null) {
                jarFileNames = jarFileNames + ":" + value;
            }
        }

        if(jarFileNames == null) {
            System.err.println("jar filename missing:");
            System.err.println(usage);
            System.exit(0);
        }


        OSMChunkExtractorCommandBuilder cef = new OSMChunkExtractorCommandBuilder();

        try {
            Parameter sourceParameter = new Parameter(sourceParameterFileName);
            SQLStatementQueue sourceSQL = new SQLStatementQueue(sourceParameter);

            LocalDate date = LocalDate.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
            String chunkImport = "chunkImport_" + date.format(formatter);

            String logFile = chunkImport + "_log";
            String errorLogFile = chunkImport + "_err";

            boolean first = true;

            String entityTypes[] = new String[] {"nodes", "ways", "relations"};

            for(String entityType : entityTypes) {
                String fullTableName = null;

                switch(entityType) {
                    case "nodes":
                        fullTableName = DB.getFullTableName(sourceParameter.getSchema(), InterDB.NODETABLE);
                        break;
                    case "ways":
                        fullTableName = DB.getFullTableName(sourceParameter.getSchema(), InterDB.WAYTABLE);
                        break;
                    case "relations":
                        fullTableName = DB.getFullTableName(sourceParameter.getSchema(), InterDB.RELATIONTABLE);
                        break;
                }

                // get min and max id
                cef.getMaxID(sourceSQL, fullTableName);

                // nodes
                long from = cef.minID;
                long to = from + size;
                if(to > cef.maxID) to = cef.maxID;

                if(from <= to) {
                    do {
                        cef.writeCommand(
                                jarFileNames,
                                sourceParameterFileName,
                                targetParameterFileName,
                                first,
                                from, to,
                                entityType,
                                logFile,
                                errorLogFile,
                                false);

                        first = false;

                        // next loop
                        from = to+1; // plus 1: its an including interval
                        to = from + size;
                        if(to > cef.maxID) to = cef.maxID;
                    } while(from < cef.maxID);
                }
            }
        }
        catch(Throwable t) {
            System.err.println("catched something while producing chunk extractor processes: " + t.getLocalizedMessage());
            System.exit(0);
        }

            // find max id from nodes, ways and relations

        // launch a chunk extractor for a chunk...


    }

    private String jvmPath = null;

    private void setJVMPath() {
        if(this.jvmPath == null) {
            if (System.getProperty("os.name").startsWith("Win")) {
                this.jvmPath = System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java.exe";
                // be on the save side and handle potential spaces
                this.jvmPath = "\"" + this.jvmPath + "\"";
            } else {
                this.jvmPath = System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java";
            }

//            System.out.println("use " + this.jvmPath + " as JVM");
        }
    }


    private void writeCommand(String jarFiles, String dbInter, String dbOHDM,
           boolean reset, long from, long to, String entityName, String logFile,
           String errorLogFile, boolean parallel) throws IOException {

        // create command line
        StringBuilder sb = new StringBuilder();
        this.setJVMPath(); // figure out JVM path on that machine
        sb.append(this.jvmPath);
        sb.append(" -classpath ");
        sb.append(jarFiles);
        
        sb.append(" ");
        sb.append("util.OSM2Rendering");

        sb.append(" ");
        sb.append(OSM2Rendering.CHUNK_PROCESS);

        sb.append(" -i ");
        sb.append(dbInter);

        sb.append(" -d ");
        sb.append(dbOHDM);

        if(reset) { sb.append(" -reset "); }

        sb.append(" -from ");
        sb.append(from);

        sb.append(" -to ");
        sb.append(to);

        sb.append(" -");
        sb.append(entityName);

        sb.append(" 1>> ");
        sb.append(logFile);

        sb.append(" 2>> ");
        sb.append(errorLogFile);

        if(parallel) {
            sb.append(" &");
        }

        String cmd = sb.toString();
        
        System.out.println(cmd);
    }

    private void getMaxID(SQLStatementQueue sql, String fulltableName) {
        // first: figure out min and max osm_id in nodes table

        try {
            sql.append("SELECT min(id), max(id) FROM ");
            sql.append(fulltableName);
            sql.append(";");

            ResultSet result = sql.executeWithResult();
            result.next();

            BigDecimal minID_bd = result.getBigDecimal(1);
            BigDecimal maxID_bd = result.getBigDecimal(2);

            this.minID =  minID_bd.longValue();
            this.maxID =  maxID_bd.longValue();
        }
        catch(SQLException se) {
            Util.printExceptionMessage(se, sql, "when calculating initial min max ids for select of nodes, ways or relations", true);
        }
    }

}
