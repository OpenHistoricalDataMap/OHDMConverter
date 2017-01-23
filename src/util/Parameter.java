package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
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
    
    private String outFile;
    private String logFile;
    private String errFile;
    
    private final PrintStream outStream;
    private final PrintStream logStream;
    private final PrintStream errStream;
    
    private static final String STDOUT = "stdout";
    private static final String STDERR = "stderr";
    
    public Parameter(String filename) throws FileNotFoundException, IOException {
        long now = System.currentTimeMillis();
        this.logFile = STDOUT;
        this.errFile = STDERR;
//        this.logFile = "log_" + now;
//        this.errFile = "err_" + now;
        this.outFile = "out_" + now;
        
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
                    }
                }
            }
            // next line
            inLine = br.readLine();
        }
        
        this.outStream = this.getStream(this.outFile);
        this.logStream = this.getStream(this.logFile);
        this.errStream = this.getStream(this.errFile);
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
    
    public PrintStream getOutStream() { return this.outStream; }
    public PrintStream getLogStream() { return this.logStream; }
    public PrintStream getErrStream() { return this.errStream; }
    
    public String getPath() { return this.getdbName() ;}
    public boolean useJDBC() { return this.useJDBC ;}
    
    private PrintStream getStream(String outFile) throws FileNotFoundException {
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
            stream = new PrintStream(new FileOutputStream(outFile));
        }
        
        return stream;
    }
}
