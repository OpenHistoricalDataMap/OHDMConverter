package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author thsc
 */
public class FileSQLStatementQueue extends SQLStatementQueue {

    private File sqlFile;
    private PrintStream fileOutStream;
    
    public FileSQLStatementQueue(File sqlFile) throws FileNotFoundException {
        this.sqlFile = sqlFile;
        this.fileOutStream = this.createFileOutputStream(sqlFile);
    }
    
    private PrintStream createFileOutputStream(File sqlFile) throws FileNotFoundException {
        try {
            return new PrintStream(sqlFile, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            // UTF-8 exists.
        }
        
        return null; // never reached.. really
    }
    
    @Override
    public void couldExecute() {
        if(this.sqlQueue == null || this.sqlQueue.length() < 1) {
            return;
        }
        
        if(this.sqlQueue.length() >= MAX_BUFFER_LENGTH) {
            this.flush2File();
        }
    }
    
    protected long writtenByte = 0;
    
    private void flush2File() {
        if(this.sqlQueue == null || this.sqlQueue.length() < 1) {
            return;
        }
        
        this.fileOutStream.print(this.sqlQueue.toString());
        this.fileOutStream.flush();
        
        this.writtenByte += this.sqlQueue.length();

        this.resetStatement();
    }
    
    @Override
    public void forceExecute() {
        this.flush2File();
    }
    
    @Override
    public void forceExecute(boolean parallel, String recordEntry) {
        this.forceExecute();
    }
    
    @Override
    public void forceExecute(boolean parallel) {
        this.forceExecute();
    }
    
    public void switchFile(File newFile) throws FileNotFoundException {
        this.flush2File();
        this.fileOutStream.close();
        this.sqlFile = newFile;
        this.fileOutStream = this.createFileOutputStream(this.sqlFile);
        this.writtenByte =0;
    }
    
    @Override
    public void close() {
        this.flush2File();
        this.fileOutStream.close();
    }
    
    @Override
    public void join() {
        // nothing todo
    }
}
