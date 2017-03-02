package util;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * @author thsc
 */
public class OutStreamSQLStatementQueue extends SQLStatementQueue {

    private PrintStream outStream;
    
    public OutStreamSQLStatementQueue(PrintStream out) {
        this.outStream = out;
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
        this.outStream.close();
        this.outStream = out;
    }
    
    @Override
    public void close() {
        this.outStream.close();
    }
}
