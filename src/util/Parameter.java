package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

/**
 * @author thsc
 */
public class Parameter {
    private String servername;
    private String portnumber;
    private String username;
    private String pwd;
    private String dbname;
    private String schema;
    private String maxThreads = "2";
    private String recordFileName = "recordFile.txt";
    private String readStepLen;
    private boolean useJDBC = true;
    
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
    
    public Parameter(String filename) throws FileNotFoundException, IOException {
        long now = System.currentTimeMillis();
        
        FileInputStream fInput = new FileInputStream(filename);
        File file = new File(filename);
        FileReader fr = new FileReader(file);
        
        BufferedReader br = new BufferedReader(fr);
        
        String inLine = br.readLine();
        while(inLine != null) {
            StringTokenizer st = new StringTokenizer(inLine, ":");
            if(st.hasMoreTokens()) {
                String key, value;
                key = st.nextToken();
                if(st.hasMoreTokens()) {
                    value = st.nextToken();
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
                        case "useJDBC": this.useJDBC = value.equalsIgnoreCase("yes"); break;
                        case "forgetPreviousImport": this.forgetPreviousImport = value.equalsIgnoreCase("yes"); break;
                        case "importNodes": this.importNodes = value.equalsIgnoreCase("yes"); break;
                        case "importWays": this.importWays = value.equalsIgnoreCase("yes"); break;
                        case "importRelations": this.importRelations = value.equalsIgnoreCase("yes"); break;
                        case "fullPSQLPath": this.fullPSQLPath = value; break;
                        case "maxSQLFileSize": this.maxSQLFileSize = Integer.parseInt(value); break;
                    }
                }
            }
            // next line
            inLine = br.readLine();
        }
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
    
    public String getPath() { return this.getdbName() ;}
    public boolean useJDBC() { return this.useJDBC ;}
    public boolean forgetPreviousImport() { return this.forgetPreviousImport; }
    public boolean importNodes() { return this.importNodes; }
    public boolean importWays() { return this.importWays; }
    public boolean importRelations() { return this.importRelations; }
    public int getMaxSQLFileSize() { return this.maxSQLFileSize; }

    public String getFullPSQLPath() { return this.fullPSQLPath;  }
    
    
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
}
