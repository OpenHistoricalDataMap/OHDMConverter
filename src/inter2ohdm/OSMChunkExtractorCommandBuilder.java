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
        String mainJarName = null;

        String path = OSMChunkExtractorCommandBuilder.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        File f;
        try {
            File jarFile = new File(OSMChunkExtractorCommandBuilder.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
//            jarFileNames = jarFile.getCanonicalPath();
            mainJarName = jarFile.getCanonicalPath();
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

        usage.append("-classpath [additional jar files - optional]");
        usage.append("\n");

        usage.append("-parallel [number of parallel processes - default: 0 (default: 2)]");
        usage.append("\n");
        int parallelProcs = 0;

        usage.append("-notnice [if set - process does not start with lower schedule priority (default: false)]");
        usage.append("\n");
        boolean nice = true;

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

            value = argumentMap.get("-classpath");
            if (value != null) {
                if(jarFileNames == null) {
                    jarFileNames = value;
                } else {
                    jarFileNames = jarFileNames + ":" + value;
                }
            }

            value = argumentMap.get("-parallel");
            if (value != null) {
                parallelProcs = Integer.parseInt(value);
                if(parallelProcs < 0) parallelProcs = 2;
            }

            if(argumentMap.containsKey("-notnice")) {
                nice = false;
            }
        }

        /*
        if(jarFileNames == null) {
            System.err.println("jar filename missing:");
            System.err.println(usage);
            System.exit(0);
        }
*/

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
                long effectiveSize = size;
                String fullTableName = null;

                switch(entityType) {
                    case "nodes":
                        fullTableName = DB.getFullTableName(sourceParameter.getSchema(), InterDB.NODETABLE);
                        effectiveSize *= 10; // make bigger steps when processing nodes
                        break;
                    case "ways":
                        fullTableName = DB.getFullTableName(sourceParameter.getSchema(), InterDB.WAYTABLE);
                        break;
                    case "relations":
                        fullTableName = DB.getFullTableName(sourceParameter.getSchema(), InterDB.RELATIONTABLE);
                        parallelProcs = 0; // relations must be imported sequentially
                        break;
                }

                // get min and max id
                cef.getMaxID(sourceSQL, fullTableName);

                // start
                long from = cef.minID;
                long to = from + effectiveSize;
                to = to > cef.maxID ? cef.maxID : to;

                int parallelCounter = parallelProcs;

                if(from <= to) {
                    do {
                        boolean parallel = false;

                        if(to + effectiveSize*5 < cef.maxID && from > cef.minID) {
                        /* last couple of processes are not parallel to end import of one
                        entity before starting another one
                          */
                            if (parallelCounter < 0){
                                // rewind
                                parallelCounter = parallelProcs;
                            }

                            if(parallelCounter == 0) {
                                if(parallelProcs > 0) {
                                    // ready for rewind next round
                                    parallelCounter = -1;
                                }
                            }

                            if(parallelCounter > 0) {
                                parallelCounter--;
                                parallel = true;
                            }
                        }

                        cef.writeCommand(
                                mainJarName,
                                jarFileNames,
                                sourceParameterFileName,
                                targetParameterFileName,
                                first,
                                from, to,
                                entityType,
                                logFile,
                                errorLogFile,
                                parallel,
                                nice
                        );

                        first = false;

                        // next loop
                        from = to+1; // plus 1: its an including interval
                        to = from + effectiveSize;
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


    private void writeCommand(String mainJarFile, String jarFiles, String dbInter, String dbOHDM,
           boolean reset, long from, long to, String entityName, String logFile,
           String errorLogFile, boolean parallel, boolean nice) throws IOException {

        // create command line
        StringBuilder sb = new StringBuilder();
        this.setJVMPath(); // figure out JVM path on that machine

        if(nice) {
            sb.append(" nice ");
        }

        // java
        sb.append(this.jvmPath);

        // -jar
        sb.append(" -jar ");
        sb.append(mainJarFile);
//        sb.append("OHDMConverter");

        if(jarFiles != null) {
            sb.append(" -classpath ");
            sb.append(jarFiles);
        }
        
        sb.append(" ");
        sb.append(OHDMConverter.CHUNK_PROCESS);

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
