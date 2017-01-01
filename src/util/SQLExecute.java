package util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author thsc
 */
class SQLExecute extends Thread {
    private final Connection connection;
    private String sqlStatement; 
    private String recordEntry;
    private final SQLStatementQueue recordKeeper;

    SQLExecute(Connection connection, String sqlStatement, String recordEntry, SQLStatementQueue recordKeeper) {
        this.connection = connection;
        this.sqlStatement = sqlStatement;
        this.recordEntry = recordEntry;
        this.recordKeeper = recordKeeper;
    }
    
    static void doExec(Connection connection, String sqlStatement) throws SQLException {
        if(sqlStatement == null) return;
        
        SQLException e = null;
        
        PreparedStatement stmt = null;
        
        try {
            stmt = connection.prepareStatement(sqlStatement);
            stmt.execute();
            stmt.close();
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
            e = ex;
        }
        finally {
            if(e != null) throw e;
        }
    }
    
    private boolean done = false;
    boolean isDone() {
        return this.done;
    }
    
    private Thread thisThread = null;
    
    synchronized boolean execNext(String sqlStatement, String recordEntry) {
        if(!this.done) return false;
        
        this.sqlStatement = sqlStatement;
        this.recordEntry = recordEntry;
        
        this.thisThread.notify();
        
        return true;
    }
    
    private boolean stopped = false;
    void stopExec() {
        this.stopped = true;
    }
    
    @Override
    public void run() {
        // remember this newly created thread
        this.thisThread = Thread.currentThread();
        
        while(!stopped) {
            // do exec
            System.out.println("exec threat issues sql statement " + this.recordEntry);
            try {
                SQLExecute.doExec(connection, sqlStatement);

                // ok, statement executed
                this.recordKeeper.writeLog(this.recordEntry);
                this.recordKeeper.done(this);
                System.out.println("exec threat successfully issued sql statement: " + this.recordEntry);
                
                
            }
            catch(SQLException e) {
                System.err.println("sql error: (error / statement): \n" + e.getMessage() + "\n" + this.sqlStatement);
            } catch (IOException ex) {
                System.err.println("cannot write record entry: " + this.recordEntry);
            } 
//            catch (InterruptedException ex) {
//                // woke up.. ok, next round?
//            }
            finally {
                this.done = true; // in any case.. we are ready here
            }
        }
    }

    void wakeup() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
