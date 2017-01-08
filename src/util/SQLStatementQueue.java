package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import osm2inter.MyLogger;

/**
 *
 * @author thsc
 */
public class SQLStatementQueue {
    private final Connection connection;
    private final int maxLength;
    private final MyLogger logger;
    
    public static final int DEFAULT_MAX_SQL_STATEMENTS = 100;
    private final ArrayList<SQLExecute> execThreads = new ArrayList<>();
    private static final int MAX_EXEC_THREADS = 2;
    
    private StringBuilder sqlQueue;
    
    private int number = 0;
    private File recordFile = null;
    private int maxThreads = 1;
    
    public SQLStatementQueue(Connection connection, int maxStatements, MyLogger logger) {
        this.connection = connection;
        this.maxLength = maxStatements;
        this.logger = logger;
    }
    
    public SQLStatementQueue(Connection connection, MyLogger logger) {
        this(connection, DEFAULT_MAX_SQL_STATEMENTS, logger);
    }
    
    public SQLStatementQueue(Connection connection) {
        this(connection, (File)null);
    }
    
    public SQLStatementQueue(Connection connection, File recordFile) {
        this(connection, recordFile, MAX_EXEC_THREADS);
    }
    
    public SQLStatementQueue(Connection connection, File recordFile, int maxThreads) {
        this(connection, DEFAULT_MAX_SQL_STATEMENTS, null);
        
        this.recordFile = recordFile;
        this.maxThreads = maxThreads;
    }
    
    /**
     * when using only this method, flush *must* be called.
     * @param a 
     */
    public void append(String a) {
        if(this.sqlQueue == null) {
            this.sqlQueue = new StringBuilder(a);
        } else {
            this.sqlQueue.append(a);
        }
    }
    
    public void append(int a) {
        this.sqlQueue.append(Integer.toString(a));
    }
    
    public void append(long a) {
        this.sqlQueue.append(Long.toString(a));
    }
    
    public void exec(String sqlStatement) throws SQLException {
        // add sql statement to queue
        if(this.sqlQueue == null) {
            this.sqlQueue = new StringBuilder(sqlStatement);
        } else {
            this.sqlQueue.append(sqlStatement);
        }
        
        if(++this.number >= this.maxLength) {
            this.forceExecute();
        }
    }
    
    /**
     * Parallel execution of sql statement
     * @param recordEntry
     * @throws SQLException
     * @throws IOException 
     */
    public void forceExecute(String recordEntry) 
            throws SQLException, IOException {
        
        this.forceExecute(true, recordEntry);
    }
            
    public void forceExecute(boolean parallel) 
            throws SQLException {
        
        try {
            this.forceExecute(parallel, null);
        }
        catch(IOException e) {
            // cannot happen, because nothing is written
        }
    }
    
    private Thread t = null;
            
    public void forceExecute(boolean parallel, String recordEntry) 
            throws SQLException, IOException {
        
        if(this.sqlQueue == null || this.sqlQueue.length() < 1) {
            return;
        }
        
        if(!parallel) {
            this.forceExecute();
            // that point is reached if no sql exception has been thrown. write log
            this.writeLog(recordEntry);
        } else {
            // find thread
            boolean found = false;
            while(!found) {
                if(this.execThreads.size() < this.maxThreads) {
                    // create new thread
                    SQLExecute se = new SQLExecute(this.connection, this.sqlQueue.toString(), 
                            recordEntry, this);
                    this.execThreads.add(se);
                    se.start();
                    found = true;
                } else {
                    // no more thread allowed.. make an educated guess and wait for
                    // first one to end
                    Thread jThread = this.execThreads.get(0);
                    try {
//                        System.out.println("sqlQueue: no sql thread found.. wait for first one to die");
                        if(jThread != null) {
                            jThread.join();
                        }
//                        System.out.println("sqlQueue: first sql thread died");
                    } catch (InterruptedException ex) {
                        // ignore and go ahead
                    }
                }
            }
            this.resetStatement();
        }
    }
    
    /**
     * sequential execution of sql statement
     * @throws SQLException 
     */
    public void forceExecute() throws SQLException {
        if(this.sqlQueue == null || this.sqlQueue.length() < 1) {
            return;
        }
        
        SQLExecute.doExec(this.connection, this.sqlQueue.toString());

        // no exeption
        this.resetStatement();
    }
    
    void writeLog(String recordEntry) throws FileNotFoundException, IOException {
        if(this.recordFile == null || recordEntry == null) return;
        
        FileWriter fw = new FileWriter(this.recordFile);
        fw.write(recordEntry);
        fw.close();
    }
    
    public ResultSet executeWithResult() throws SQLException {
        PreparedStatement stmt = this.connection.prepareStatement(this.sqlQueue.toString());
        
        try {
            ResultSet result = stmt.executeQuery();
            this.resetStatement();
        
            return result;
        }
        catch(SQLException e) {
            throw e;
        }
        finally {
            this.resetStatement();
            stmt.closeOnCompletion();
        }
    }
    
    private String debugLastStatement;
    private void resetStatement() {
        if(this.sqlQueue != null) {
            this.debugLastStatement = this.sqlQueue.toString();
            this.sqlQueue = null;
        }
    }

    public void print(String message) {
        System.out.println(message);
        System.out.println(this.sqlQueue.toString());
    }
    
    @Override
    public String toString() {
        return "lastStatement:\n" + this.debugLastStatement;
    }

    synchronized void done(SQLExecute execThread) {
        this.execThreads.remove(execThread);
//        System.out.println("sqlQueue: exec thread removed");
    }
    
    /**
     * Wait until all pending threads came to end end.. flushes that queue
     */
    public void flushThreads() {
//        System.out.println("sqlQueue: flush called... wait for threads to finish");
        while(!this.execThreads.isEmpty()) {
            try {
                // each loop we wait for the first thread to finish until
                // array is empty
                this.execThreads.get(0).join();
            } catch (InterruptedException ex) {
                // go ahead
            }
        }
//        System.out.println("sqlQueue: flush finished");
    }
}
