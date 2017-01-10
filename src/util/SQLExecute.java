package util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
        } catch (SQLException ex) {
            e = ex;
        }
        finally {
            stmt.close();
            if(e != null) throw e;
        }
    }
    
    private boolean done = false;
    boolean isDone() {
        return this.done;
    }
    
    @Override
    public void run() {
        // do exec
//        System.out.println("exec threat issues sql statement " + this.recordEntry);
        try {
            SQLExecute.doExec(connection, sqlStatement);

            // ok, statement executed
            this.recordKeeper.writeLog(this.recordEntry);
            this.recordKeeper.done(this);
//            System.out.print(this.recordEntry + ", ");
//            System.out.println("exec threat successfully issued sql statement: " + this.recordEntry);
        }
        catch(SQLException e) {
            System.err.println("sql error: (error / statement): \n" + e.getMessage() + "\n" + this.sqlStatement);
        } catch (IOException ex) {
            System.err.println("cannot write record entry: " + this.recordEntry);
        } 
        finally {
            this.done = true; // in any case.. we are ready here
        }
    }
}
