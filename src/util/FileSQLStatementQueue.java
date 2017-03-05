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
        this.forceExecute();
    }
    
    @Override
    public void forceExecute(boolean parallel, String recordEntry) {
        this.forceExecute();
    }
    
    @Override
    public void forceExecute(boolean parallel) {
        this.forceExecute();
    }
    
    @Override
    public void forceExecute() {
        if(this.sqlQueue == null || this.sqlQueue.length() < 1) {
            return;
        }
//        byte[] utf8Bytes = this.sqlQueue.toString().getBytes(StandardCharsets.UTF_8);
//        String utf8String = new String(utf8Bytes, StandardCharsets.UTF_8);
//        this.outStream.print(utf8String);
        this.fileOutStream.print(this.sqlQueue.toString());
        this.fileOutStream.flush();

        this.resetStatement();
    }
    
    public void switchFile(File newFile) throws FileNotFoundException {
        this.close();
        this.sqlFile = newFile;
        this.fileOutStream = this.createFileOutputStream(this.sqlFile);
    }
    
    @Override
    public void close() {
        this.forceExecute();
        this.fileOutStream.close();
    }
    
    @Override
    public void join() {
        // nothing todo
    }
}
