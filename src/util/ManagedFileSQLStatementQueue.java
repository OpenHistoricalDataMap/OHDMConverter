package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

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
    
    public ManagedFileSQLStatementQueue(String name, Parameter parameter) throws FileNotFoundException {
        super(new File(name));
        this.name = name;
        this.parameter = parameter;
        this.currentFileName = name;
        this.currentFile = new File(name);
        this.maxMByte = parameter.getMaxSQLFileSize();
    }
    
    private final int COUNTDOWNSTART = 10;
    
    private int currentFileNumber = 1;
    
    @Override
    public void forceExecute() {
        // write command
        super.forceExecute();
        
        // hang out file and give it psql
        this.executeFileAndSetupNew();
        
    }
    
    private void executeFileAndSetupNew() {
        try {
            // remember current sql file name
            String fileName = this.currentFileName;

            // create new sql file
            this.currentFileName = this.name + this.currentFileNumber++;
            this.currentFile = new File(this.currentFileName);

            // end, flush, close old and enter new PrintStream
            /*
            Note: switch has the side effect that the former
            stream (file) is closed and given to psql. We don't have 
            anything to do here after switching the file
            */
            this.switchFile(new File(this.currentFileName));
            System.out.println("feed sql file to psql when switching file: " + fileName);
            // false: not parallel, true: delete tmp sql file
            Util.feedPSQL(parameter, this.currentFileName, false, true);


        } catch (IOException ex) {
            System.err.println("could not start psql: " + ex);
        }
        
    }
    
    @Override
    public void couldExecute() {
        // propagate to super method
        super.couldExecute();
        
        // if file has exceed max length. Hang up and let it executed
//        if(this.currentFile.length() > this.maxMByte*1024*1024) {
        if(this.currentFile.length() > this.maxMByte*1024*1024) { // each time for debugging
            
            // clear memory
            super.forceExecute();
            
            // execute file and create new one
            this.executeFileAndSetupNew();
        }
    }
    
    @Override
    public void close() {
        super.close();
        
        // feed final file to psql
        System.out.println("feed sql file to psql after closing: " + this.currentFileName);
        try {
            // false: not parallel, false: don't delete tmp sql file
//            Util.feedPSQL(parameter, this.currentFileName, false, false);
            Util.feedPSQL(parameter, this.currentFileName, false, true);
        } catch (IOException ex) {
            System.err.println("could not start psql: " + ex);
        }
    }
}
