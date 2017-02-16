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
/*
import from osm file into intermediate db
Exception in thread "Thread-446720582" java.lang.ArrayIndexOutOfBoundsException
        at java.lang.AbstractStringBuilder.append(AbstractStringBuilder.java:597
)
        at java.lang.StringBuilder.append(StringBuilder.java:190)
        at org.postgresql.core.Parser.parseSql(Parser.java:1026)
        at org.postgresql.core.Parser.replaceProcessing(Parser.java:972)
        at org.postgresql.core.CachedQueryCreateAction.create(CachedQueryCreateA
ction.java:41)
        at org.postgresql.core.CachedQueryCreateAction.create(CachedQueryCreateA
ction.java:17)
        at org.postgresql.util.LruCache.borrow(LruCache.java:115)
        at org.postgresql.core.QueryExecutorBase.borrowQuery(QueryExecutorBase.j
ava:266)
        at org.postgresql.jdbc.PgConnection.borrowQuery(PgConnection.java:143)
        at org.postgresql.jdbc.PgPreparedStatement.<init>(PgPreparedStatement.ja
va:88)
        at org.postgresql.jdbc.PgConnection.prepareStatement(PgConnection.java:1
256)
        at org.postgresql.jdbc.PgConnection.prepareStatement(PgConnection.java:1
622)
        at org.postgresql.jdbc.PgConnection.prepareStatement(PgConnection.java:4
15)
        at util.SQLExecute.doExec(SQLExecute.java:41)
        at util.SQLExecute.run(SQLExecute.java:62)            
            */            
            stmt = connection.prepareStatement(sqlStatement);
            stmt.execute();
            stmt.close();
        } catch (SQLException ex) {
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
