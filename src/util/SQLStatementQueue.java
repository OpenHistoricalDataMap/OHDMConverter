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
    
    private boolean done = true;
    public boolean finished() {
        return this.done;
    }
    
    private SQLExecute execThread = null;
    
    public void forceExecute(String recordEntry) 
            throws SQLException, IOException {
        
        this.forceExecute(true, recordEntry);
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
            this.execThread = new SQLExecute(this.connection, 
                    this.sqlQueue, recordEntry, this);
            
            this.execThread.start();
            this.resetStatement();
        }
    }
    
    void writeLog(String recordEntry) throws FileNotFoundException, IOException {
        if(this.recordFile == null) return;
        
        FileWriter fw = new FileWriter(this.recordFile);
        fw.write(recordEntry);
    }

    
    public void forceExecute() throws SQLException {
        if(this.sqlQueue == null) return;
        
        SQLExecute.doExec(this.connection, this.sqlQueue);

        // no exeption
        this.resetStatement();
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
}
