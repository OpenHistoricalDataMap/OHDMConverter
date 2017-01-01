package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    
    private StringBuilder sqlQueue;
    
    private int number = 0;
    private File recordFile = null;
    
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
        this(connection, DEFAULT_MAX_SQL_STATEMENTS, null);
        
        this.recordFile = recordFile;
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
    
    private ArrayList<Thread> waitThreads = new ArrayList<>();
    
    /**
     * blocks this thread until all other threads initiated by
     * this sql queue are finished
     */
    public synchronized void waitUntilFinished(Thread waitThread) {
        while(!this.finished()) {
            try {
                this.waitThreads.add(waitThread);
                waitThread.wait();
            } catch (InterruptedException ex) {
                // awake again
                System.out.println("woke up and check out..");
            }
        }
        this.waitThreads.remove(waitThread);
    }
    
    public boolean finished() {
        return this.execThreads.isEmpty();
    }

    public int numberRunningThread() {
        return this.execThreads.size();
    }
    
    private final ArrayList<SQLExecute> execThreads = new ArrayList<>();

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
            // create thread
            SQLExecute se = new SQLExecute(this.connection, this.sqlQueue.toString(), 
                    recordEntry, this);
            
            this.execThreads.add(se);
            se.start();
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
        if(this.recordFile == null) return;
        
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
            this.resetStatement();
            throw e;
        }
        finally {
            stmt.closeOnCompletion();
        }
    }
    
    private String debugLastStatement;
    private void resetStatement() {
        this.debugLastStatement = this.sqlQueue.toString();
        this.sqlQueue = null;
    }

    public void print(String message) {
        System.out.println(message);
        System.out.println(this.sqlQueue.toString());
    }

    private synchronized void wakeup() {
        for(Thread waitThread : this.waitThreads) {
            waitThread.notify();
        }
    }
    
    synchronized void done(SQLExecute execThread) {
        // TODO enter some real code
        this.execThreads.remove(execThread);
        
        // wake wait process if any
        this.wakeup();
    }
}
