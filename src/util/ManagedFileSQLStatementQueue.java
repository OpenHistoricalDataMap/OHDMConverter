package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

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
        super(new File(name + "0"));
        this.name = name;
        this.parameter = parameter;
        this.currentFileName = this.name + "0";
        this.currentFile = new File(this.name);
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
    
    private int nextFileNumber() {
        if(this.currentFileNumber > this.parameter.getMaxPSQLProcesses() * 5) {
            this.currentFileNumber = 0;
        } else {
            this.currentFileNumber++;
        }
        
        return this.currentFileNumber;
    }
    
    private void executeFileAndSetupNew() {
        try {
            // remember current sql file name
            String fileName = this.currentFileName;

            // create new sql file
            this.currentFileName = this.name + this.nextFileNumber();
            this.currentFile = new File(this.currentFileName);

            // end, flush, close old and enter new PrintStream
            /*
            Note: switch has the side effect that the former
            stream (file) is closed and given to psql. We don't have 
            anything to do here after switching the file
            */
            this.switchFile(new File(this.currentFileName));
//            System.out.println("spawn new psql process to execute: " + fileName);
            // false: not parallel, true: delete tmp sql file
            this.wait4FreeSlot();
            this.rememberPSQLProcess(Util.feedPSQL(parameter, fileName, true, false));


        } catch (IOException ex) {
            System.err.println("could not start psql: " + ex);
        }
        
    }
    
    ArrayList<Process> psqlProcessses = new ArrayList<>();
    
    /**
     * 
     * @return true is empty
     */
    private void clearPSQLProcessList() {
        ArrayList<Process> dead = new ArrayList<>();
        
        for(Process p : this.psqlProcessses) {
            if(!p.isAlive()) {
//                System.out.println("psql process dead and removed: " + p.toString());
                dead.add(p);
            }
        }
        
        for(Process deadP : dead) {
            this.psqlProcessses.remove(deadP);
        }
    }
    
    private void rememberPSQLProcess(Process psqlProcess) {
//        System.out.println("remember psql process: " + psqlProcess.toString());
        this.psqlProcessses.add(psqlProcess);
    }
    
    @Override
    public void couldExecute() {
        // propagate to super method
        super.couldExecute();
        
        // if file has exceed max length. Hang up and let it executed
        if(this.writtenByte > this.maxMByte*1024*1024) { // unit: megabyte
            
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
//        System.out.println("feed sql file to psql after closing: " + this.currentFileName);
        try {
            // false: not parallel, false: don't delete tmp sql file
//            Util.feedPSQL(parameter, this.currentFileName, false, false);
            this.rememberPSQLProcess(Util.feedPSQL(parameter, this.currentFileName, false, false));
        } catch (IOException ex) {
            System.err.println("could not start psql: " + ex);
        }
    }
    
    private final int sleepTime = 10*100; // 1 seconds
    @Override
    public void join() {
        while(this.psqlProcessses.isEmpty()) {
            try {
                Thread.sleep(this.sleepTime);
                this.clearPSQLProcessList();
            } catch (InterruptedException ex) {
                // next loop
            }
        }
    }
    
    public void wait4FreeSlot() {
        while(this.psqlProcessses.size() >= parameter.getMaxPSQLProcesses()) {
            try {
                Thread.sleep(this.sleepTime);
                this.clearPSQLProcessList();
            } catch (InterruptedException ex) {
                // next loop
            }
        }
    }
}
