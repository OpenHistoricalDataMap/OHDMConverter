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
    private final StringBuilder sqlQueue; 
    private final String recordEntry;
    private final SQLStatementQueue recordKeeper;

    SQLExecute(Connection connection, StringBuilder sqlQueue, String recordEntry, SQLStatementQueue recordKeeper) {
        this.connection = connection;
        this.sqlQueue = sqlQueue;
        this.recordEntry = recordEntry;
        this.recordKeeper = recordKeeper;
    }
    
    static void doExec(Connection connection, StringBuilder sqlQueue) throws SQLException {
        if(sqlQueue == null) return;
        
        SQLException e = null;
        
        PreparedStatement stmt = null;
        
        try {
            stmt = connection.prepareStatement(sqlQueue.toString());
            stmt.execute();
            stmt.close();
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
            e = ex;
        }
        finally {
            stmt.close();
            if(e != null) throw e;
        }
        
    }
    
    @Override
    public void run() {
        // do exec
        try {
            SQLExecute.doExec(connection, sqlQueue);
            
            // ok, statement executed
            this.recordKeeper.writeLog(this.recordEntry);
        }
        catch(SQLException e) {
            // ignore
        } catch (IOException ex) {
            System.err.println("cannot write record entry: " + this.recordEntry);
        }
    }    
}
