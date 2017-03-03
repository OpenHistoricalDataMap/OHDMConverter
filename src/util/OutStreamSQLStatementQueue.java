package util;

import java.io.PrintStream;

/**
 *
 * @author thsc
 */
public class OutStreamSQLStatementQueue extends SQLStatementQueue {

    private PrintStream outStream;
    
    public OutStreamSQLStatementQueue(PrintStream out) {
        this.outStream = out;
    }
    
    @Override
    public void forceExecute(boolean parallel, String recordEntry) {
        this.forceExcecute();
    }
    
    public void forceExcecute(boolean parallel) {
        this.forceExcecute();
    }
    
    public void forceExcecute() {
        if(this.sqlQueue == null || this.sqlQueue.length() < 1) {
            return;
        }
        
        this.outStream.print(this.sqlQueue.toString());
        this.outStream.flush();
        
        this.resetStatement();
    }
    
    public void switchStream(PrintStream out) {
        this.close();
        this.outStream = out;
    }
    
    @Override
    public void close() {
        this.forceExcecute();
        this.outStream.close();
    }
    
    @Override
    public void join() {
        // nothing todo
    }
}
