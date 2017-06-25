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
            if(connection == null) {
                System.err.println("no connection to database - cannot perform sql statement");
                throw new SQLException("connection is null");
            }
            if(sqlStatement == null) {
                System.err.println("cannot execute empty (null) sqlStatement - continue");
                return;
            }
            stmt = connection.prepareStatement(sqlStatement);
            stmt.execute();
            stmt.close();
//            connection.commit();
        } catch (SQLException ex) {
            e = ex;
        }
        catch (RuntimeException rex) {
            System.err.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            System.err.println("runtime exception caught when executing sql statement: " + rex);
            System.err.println("sql statement:\n" + sqlStatement);
            System.err.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            throw rex;
        }
        finally {
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

    Connection getConnection() {
        return this.connection;
    }
}
