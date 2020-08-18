package util;

import inter2ohdm.OSMChunkExtractor;
import ohdm2rendering.OHDM2Rendering;
import osm.OSMClassification;

import java.io.*;
import java.text.*;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author thsc
 */
//todo add parameter description
public class Parameter {
    private final String parameterFilename;
    private String servername;
    private String portnumber;
    private String username;
    private String pwd;
    private String dbname;
    private String schema;
    private String maxThreads = "2";
    private String recordFileName = "recordFile.txt";
    private String readStepLen;
    private boolean usePSQL = false;

    private static final String STDOUT = "stdout";
    private static final String STDERR = "stderr";

    private String outFile = STDOUT;
    private String logFile = STDOUT;
    private String errFile = STDERR;

    private PrintStream outStream;
    private PrintStream logStream;
    private PrintStream errStream;

    private boolean forgetPreviousImport = true;
    private boolean importNodes = true;
    private boolean importWays = true;
    private boolean importRelations = true;
    private String fullPSQLPath = "psql";
    private int maxSQLFileSize = 1;
    private int maxPSQLProcesses = 1;
    private String renderoutput = OHDM2Rendering.GENERIC;
    private int logMessageInterval = 5;
    private int SerTagsSize = 200000;
    private String osmfilecreationdatestring;

    private String columnNameObjectName;
    private String columnNameGeometry;
    private String tableName;
    private int classificationID = -1;
    private String validSince;
    private String validUntil;
    private boolean dropAndRecreate = true;

    public String columnValidSinceYear = null;
    public String columnValidSinceMonth = null;
    public String columnValidSinceDay = null;
    public String columnValidUntilYear = null;
    public String columnValidUntilMonth = null;
    public String columnValidUntilDay = null;
    private String sourceURL = "";
    private String sourceLicense = "";
    private String sourceComments = "";
    private String sourceCitation = "";

    public String getConnectionType() {
        return connectionType;
    }

    public String getDelimiter() {
        return delimiter;
    }

    // added parameters for 'COPY' support
    private String connectionType = "insert"; // use 'copy' to init connectors as Copy Connectors
    private String delimiter = "|";
    private String[] nodesColumnNames;
    private String[] relationmemberColumnNames;
    private String[] relationsColumnNames;

    public String[] getNodesColumnNames() {
        return nodesColumnNames;
    }

    public String[] getRelationmemberColumnNames() {
        return relationmemberColumnNames;
    }

    public String[] getRelationsColumnNames() {
        return relationsColumnNames;
    }

    public String[] getWaynodesColumnNames() {
        return waynodesColumnNames;
    }

    public String[] getWaysColumnNames() {
        return waysColumnNames;
    }

    public int getClassificationID() { return this.classificationID; }
    public String getValidSince() { return this.validSince; }
    public String getValidUntil() { return this.validUntil; }
    public boolean getDropAndRecreate() { return this.dropAndRecreate; }

    private String[] waynodesColumnNames;
    private String[] waysColumnNames;


    public Parameter(String filename) throws FileNotFoundException, IOException {
        long now = System.currentTimeMillis();

        this.parameterFilename = filename;

        FileInputStream fInput = new FileInputStream(filename);
        File file = new File(filename);
        FileReader fr = new FileReader(file);

        BufferedReader br = new BufferedReader(fr);

        String inLine = br.readLine();

        boolean first = true;
        boolean inComment = false;
        boolean skip = false;

        while(inLine != null) {
            skip = false;

            // ignore comments like //
            if(inLine.startsWith("//")) {
                skip = true;
            }

            if(!inComment) {
                if(inLine.startsWith("/*")) {
                    inComment = true;
                    skip = true;
                }
            } else { // in comment
                if(inLine.contains("*/")) {
                    inComment = false;
                }
                // in any case:
                skip = true;
            }

            if(!skip) {
                // find delimiter
                String key, value;
                int delimiterIndex = inLine.indexOf(":");
                if(delimiterIndex < 1) {inLine = br.readLine(); continue;} // missing or first character - both invalid
//                StringTokenizer st = new StringTokenizer(inLine, ":");
//                if(st.hasMoreTokens()) {
//                String key, value;
//                key = st.nextToken();
//                if(st.hasMoreTokens()) {
//                value = st.nextToken();

                key = inLine.substring(0, delimiterIndex);
                key = key.trim();
                value = inLine.substring(delimiterIndex+1); // just behind ":"
                value = value.trim();

                // fill parameters
                switch(key) {
                    case "servername": this.servername = value; break;
                    case "portnumber": this.portnumber = value; break;
                    case "username": this.username = value; break;
                    case "pwd": this.pwd = value; break;
                    case "dbname": this.dbname = value; break;
                    case "schema": this.schema = value; break;
                    case "maxThreads": this.maxThreads = value; break;
                    case "recordFileName": this.recordFileName = value; break;
                    case "readsteplen": this.readStepLen = value; break;
                    case "outFile": this.outFile = value; break;
                    case "logFile": this.logFile = value; break;
                    case "errFile": this.errFile = value; break;
                    case "usePSQL": this.usePSQL = this.getTrueOrFalse(value); break;
                    case "forgetPreviousImport": this.forgetPreviousImport = this.getTrueOrFalse(value); break;
                    case "importNodes": this.importNodes = this.getTrueOrFalse(value); break;
                    case "importWays": this.importWays = this.getTrueOrFalse(value); break;
                    case "importRelations": this.importRelations = this.getTrueOrFalse(value); break;
                    case "fullPSQLPath": this.fullPSQLPath = value; break;
                    case "maxSQLFileSize": this.maxSQLFileSize = Integer.parseInt(value); break;
                    case "maxPSQLProcesses": this.maxPSQLProcesses = Integer.parseInt(value); break;
                    case "renderoutput": this.renderoutput = value.toLowerCase(); break;
                    case "logMessageInterval": this.logMessageInterval = Integer.parseInt(value); break;
                    case "connectionType": this.connectionType = value; break;
                    case "delimiter": this.delimiter = value; break;
                    case "nodesColumnNames": this.nodesColumnNames = value.split("\\|"); break;
                    case "relationmemberColumnNames": this.relationmemberColumnNames = value.split("\\|"); break;
                    case "relationsColumnNames": this.relationsColumnNames = value.split("\\|"); break;
                    case "waynodesColumnNames": this.waynodesColumnNames = value.split("\\|"); break;
                    case "waysColumnNames": this.waysColumnNames = value.split("\\|"); break;
                    case "serTagsSize": this.SerTagsSize = Integer.parseInt(value); break;
                    case "osmfilecreationdate": this.checkDateFormat(value); break;
                    case "columnNameObjectName": this.columnNameObjectName = value; break;
                    case "columnNameGeometry": this.columnNameGeometry = value; break;
                    case "tableName": this.tableName = value; break;
                    case "validSince": this.validSince = value; this.checkDateFormat(validSince); break;
                    case "validUntil": this.validUntil = value; this.checkDateFormat(validUntil); break;
                    case "classificationID": this.classificationID = Integer.parseInt(value); break;
                    case "dropAndRecreate": this.dropAndRecreate = this.getTrueOrFalse(value); break;
                    case "columnValidSinceYear": this.columnValidSinceYear = value; break;
                    case "columnValidSinceMonth": this.columnValidSinceMonth = value; break;
                    case "columnValidSinceDay": this.columnValidSinceDay = value; break;
                    case "columnValidUntilYear": this.columnValidUntilYear = value; break;
                    case "columnValidUntilMonth": this.columnValidUntilMonth = value; break;
                    case "columnValidUntilDay": this.columnValidUntilDay = value; break;
                    case "sourceURL": this.sourceURL = value; break;
                    case "sourceLicense": this.sourceLicense = value; break;
                    case "sourceComments": this.sourceComments = value; break;
                    case "sourceCitation": this.sourceCitation = value; break;
                }
//                }
//                }
            }
            // next line
            inLine = br.readLine();
        }
    }

    public String getParameterFilename() { return this.parameterFilename; }

    private boolean getTrueOrFalse(String value) {
        return (value.equalsIgnoreCase("yes")
                || value.equalsIgnoreCase("true"));
    }

    public String getServerName() { return this.servername ;}
    public String getPortNumber() { return this.portnumber ;}
    public String getUserName() { return this.username ;}
    public String getPWD() { return this.pwd ;}
    public String getdbName() { return this.dbname ;}
    public String getSchema() { return this.schema ;}
    public String getMaxThread() { return this.maxThreads ;}
    public String getRecordFileName() { return this.recordFileName; }
    public String getReadStepLen() { return this.readStepLen; }
    public String getOsmfilecreationdate() {
        if(this.osmfilecreationdatestring == null) {
            Date now = new Date();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            this.osmfilecreationdatestring = df.format(new Date());
        }
        return this.osmfilecreationdatestring;
    }
    public String getColumnNameObjectName() { return this.columnNameObjectName; }
    public String getColumnNameGeometry() { return this.columnNameGeometry; }
    public String getTableName() { return this.tableName; }


    private String paddWithLeadingZeros(String value, int requiredLength) {
        if(value.length() >= requiredLength) return value;

        StringBuilder sb = new StringBuilder();

        for(int i = value.length(); i < requiredLength; i++) {
            sb.append("0");
        }

        sb.append(value);
        return sb.toString();
    }

    public String getPath() { return this.getdbName() ;}
    public boolean usePSQL() { return this.usePSQL ;}
    public boolean forgetPreviousImport() { return this.forgetPreviousImport; }

    public int getLogMessageInterval() {return this.logMessageInterval; }

    private boolean checkDateFormat(String dateString) {
        DateFormat df = new SimpleDateFormat("YYYY-MM-DD");
        try {
            df.parse(dateString);
            // valid string
            this.osmfilecreationdatestring = dateString;
        } catch (ParseException ex) {
            // forget it
            System.err.println("dateString has no valid date format (YYYY-MM-DD), ignore and take today instead: " + dateString);
            return false;
        }
        
        return true;
    }

    public int getMaxSQLFileSize() { return this.maxSQLFileSize; }
    public int getMaxPSQLProcesses() { return this.maxPSQLProcesses; }

    public String getFullPSQLPath() { return this.fullPSQLPath;  }

    public String getRenderoutput() { return this.renderoutput;  }
    
    
    public int getSerTagsSize() { return this.SerTagsSize;  }

    public PrintStream getOutStream() throws FileNotFoundException {
        if(this.outStream == null) {
            this.outStream = this.getOutStream(this.outFile);
        }

        return this.outStream;
    }

    public PrintStream getOutStream(String name) throws FileNotFoundException {
        return this.getStream(this.outFile, name);
    }

    public PrintStream getLogStream() throws FileNotFoundException {
        if(this.logStream == null) {
            this.logStream = this.getOutStream(this.logFile);
        }

        return this.logStream;
    }

    public PrintStream getLogStream(String name) throws FileNotFoundException {
        return this.getStream(this.logFile, name);
    }

    public PrintStream getErrStream() throws FileNotFoundException {
        if(this.errStream == null) {
            this.errStream = this.getOutStream(this.errFile);
        }

        return this.errStream;
    }

    public PrintStream getErrStream(String name) throws FileNotFoundException {
        return this.getStream(this.errFile, name);
    }

    private PrintStream getStream(String outFile, String name) throws FileNotFoundException {

        if(outFile == null || outFile.length() == 0) {
            throw new FileNotFoundException("empty filename");
        }

        PrintStream stream = null;

        // yes we are
        if(outFile.equalsIgnoreCase(STDOUT)) {
            stream = System.out;
        }
        else if(outFile.equalsIgnoreCase(STDERR)) {
            stream = System.err;
        }
        else {
            // open file and create PrintStream
            if(name != null) {
                outFile = name;
            }
            try {
                stream = new PrintStream(new FileOutputStream(outFile), true, "UTF-8");
            }
            catch(UnsupportedEncodingException e) {
                // utf-8 should be well-known.. anyway: but in case: hide it as file not found..
                throw new FileNotFoundException("weired: that system cannot handle UTF-8.. fatal");
            }
        }

        return stream;
    }

    public String getColumnValidSinceYear() { return this.columnValidSinceYear; }
    public String getColumnValidSinceMonth() { return this.columnValidSinceMonth; }
    public String getColumnValidSinceDay() { return this.columnValidSinceDay; }

    public String getColumnValidUntilYear() { return this.columnValidUntilYear;}
    public String getColumnValidUntilMonth() { return this.columnValidUntilMonth;}
    public String getColumnValidUntilDay() { return this.columnValidUntilDay;}

    public String getSourceURL() { return this.sourceURL; }
    public String geSourceLicense() { return this.sourceLicense; }
    public String geSourceComments() { return this.sourceComments; }
    public String geSourceCitation() { return this.sourceCitation; }
}
