package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

/**
 *
 * @author thsc
 */
public class ManagedFileSQLStatementQueue extends FileSQLStatementQueue {

    private final Parameter parameter;
    File currentFile;
    private final int maxMByte;
    private final String name;
    private String currentFileName;
    
    public ManagedFileSQLStatementQueue(String name, Parameter parameter, int maxMByte) throws FileNotFoundException {
        super(new File(name));
        this.name = name;
        this.parameter = parameter;
        this.currentFileName = name;
        this.currentFile = new File(name);
        this.maxMByte = maxMByte;
    }
    
    private final int COUNTDOWNSTART = 10;
    private int forceCountDown = COUNTDOWNSTART;
    
    private int currentFileNumber = 1;
    
    @Override
    public void forceExecute() {
        if(--this.forceCountDown <= 0) {
            if(this.currentFile.length() > this.maxMByte*1024*1024) {
                try {
                    System.out.println("feed sql file to psql: " + this.currentFileName);
                    
                    // remember current sql file name
                    String fileName = this.currentFileName;
                    
                    // create new sql file
                    this.currentFileName = this.name + this.currentFileNumber++;
                    this.currentFile = new File(this.currentFileName);
                    
                    // end, flush, close old and enter new PrintStream
                    this.switchFile(new File(this.currentFileName));
                    
                    // let a new psql process process that finished sql file and remove it
                    Util.feedPSQL(parameter, fileName, true, true);
                } catch (IOException ex) {
                    System.err.println("could not start psql: " + ex);
                }
            }
        } else {
            super.forceExecute();
        }
    }
    
    @Override
    public void close() {
        super.close();
        
        // feed final file to psql
        System.out.println("feed final sql file to psql: " + this.currentFileName);
        try {
            Util.feedPSQL(parameter, this.currentFileName, false, true);
        } catch (IOException ex) {
            System.err.println("could not start psql: " + ex);
        }
    }
}
