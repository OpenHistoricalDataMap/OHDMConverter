package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author thsc
 */
public class SQLStatementQueue {
    private List<Connection> connections = new ArrayList<>();
    private List<Boolean> freeConnection = new ArrayList<>();
    
//    protected static final long MAX_BUFFER_LENGTH = 1; // 1 Byte Debugging
    protected static final long MAX_BUFFER_LENGTH = 60 * 1024; // 60 kByte
//    private static final long MAX_BUFFER_LENGTH = 200; // 500 kByte
    
    private final ArrayList<SQLExecute> execThreads = new ArrayList<>();
    private static final int DEFAULT_MAX_EXEC_THREADS = 1;
    
    protected StringBuilder sqlQueue;
    
    private int number = 0;
    private File recordFile = null;
    private int maxThreads = 1;
    
    private PrintStream outStream = null;
    private PrintStream logStream = null;
    private PrintStream errStream = null;
    
    public SQLStatementQueue(Connection connection) {
        this.connections.add(connection);
        this.freeConnection.add(Boolean.TRUE);
    }
    
//    public SQLStatementQueue(Connection connection, MyLogger logger) {
//        this(connection, DEFAULT_MAX_SQL_STATEMENTS, logger);
//    }
    
    /**
     * @deprecated 
     * @param connection
     * @param recordFile
     * @param maxThreads 
     */
    public SQLStatementQueue(Connection connection, File recordFile, int maxThreads) {
        this(connection);
        
        this.recordFile = recordFile;
        this.maxThreads = maxThreads;
    }
    
    public SQLStatementQueue(Parameter parameter, int maxThreads) throws SQLException, FileNotFoundException {
        this(parameter, maxThreads, null, null, true);
    }
    
    public SQLStatementQueue(Parameter parameter) throws SQLException, FileNotFoundException {
        this(parameter, SQLStatementQueue.DEFAULT_MAX_EXEC_THREADS, null, null, true);
    }
    
    public SQLStatementQueue(Parameter parameter, int maxThreads, PrintStream outStream, String name) throws SQLException, FileNotFoundException {
        this(parameter, maxThreads, outStream, name, false);
    }
    
    public SQLStatementQueue() {
        // that look sooo ugly.. TODO
        this((Connection)null);
    }
    
    public SQLStatementQueue(Parameter parameter, int maxThreads, PrintStream outStream, String name, boolean forceJDBC) throws SQLException, FileNotFoundException {
        this();
        
        if(outStream != null) {
            this.outStream = outStream;
        } else {
            this.outStream = parameter.getOutStream(name);
        }
        this.errStream = parameter.getErrStream(name);
        this.logStream = parameter.getLogStream(name);
        
        this.connections.clear();
        this.freeConnection.clear();
        this.maxThreads = maxThreads;
        
        if(parameter.usePSQL() || forceJDBC) {
            // set up connections
            try {
                // try to create first
                this.connections.add(DB.createConnection(parameter));

                // no exception thrown... set up connections
                this.freeConnection.add(Boolean.TRUE);

//                this.logStream.println(this.hashCode() + ": create " + maxThreads + " connections");
                // create connections more connection
                for(int i=1; i < maxThreads; i++) {
                    this.connections.add(DB.createConnection(parameter));
                    this.freeConnection.add(Boolean.TRUE);
                }
            }
            catch(SQLException e) {
                Util.printExceptionMessage(errStream, e, this, "could not create db connection.. probably we work with psql", true);
                this.connections = null;
                this.freeConnection = null;
            }
        } else {
            this.connections = null;
            this.freeConnection = null;
        }
    }
    
    public void close() throws SQLException {
        this.forceExecute();
        this.join();
        if(this.connections != null) {
            for(Connection conn : this.connections) {
                conn.close();
            }
            this.connections.clear();
        }
    }
    
    private Connection getFreeConnection() {
        if(this.connections != null) {
            for(int i = 0; i < this.freeConnection.size(); i++) {
                if(this.freeConnection.get(i)) {
                    this.freeConnection.set(i, Boolean.FALSE);
    //                System.out.println(this.hashCode() + ": connection taken #"+ this.connections.get(i).hashCode());
                    return this.connections.get(i);
                }
            }

            // wait for all threads to end
            this.join();
            return this.getFreeConnection();
        }
        
        return null;
    }
    
    private void setFreeConnection(Connection conn) {
        if(this.connections != null) {
            for(int i = 0; i < this.connections.size(); i++) {
                if(this.connections.get(i) == conn) {
                    this.freeConnection.set(i, Boolean.TRUE);
    //                System.out.println(this.hashCode() + ": connection free " + conn.hashCode());
                }
            }
        }
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
        this.append(Integer.toString(a));
    }
    
    public void append(long a) {
        this.append(Long.toString(a));
    }
    
    /**
     * Parallel execution of sql statement
     * @param recordEntry
     * @throws SQLException
     * @throws IOException 
     * @deprecated 
     */
    public void forceExecute(String recordEntry) 
            throws SQLException, IOException {
        
        this.forceExecute(false, recordEntry);
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
    
    private boolean useJDBC() {
        return this.connections != null;
    }
    
    private void doNonJDBCExec() {
        // print it to outputstream
        this.outStream.println(this.sqlQueue.toString());
        this.resetStatement();
    }
            
    public void forceExecute(boolean parallel, String recordEntry) 
            throws SQLException, IOException {
        
        if(this.sqlQueue == null || this.sqlQueue.length() < 1) {
            return;
        }
        
        if(!this.useJDBC()) {
            this.doNonJDBCExec();
        }
        // using JDBC
        else {
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
                        Connection conn = this.getFreeConnection();
                        SQLExecute se = new SQLExecute(conn, this.sqlQueue.toString(), 
                                recordEntry, this);
                        this.execThreads.add(se);
                        se.start();
                        found = true;
                    } else {
                        try {
                            // no more thread allowed.. make an educated guess and wait for
                            // first one to end
                            Thread jThread = this.execThreads.get(0);
    //                        System.out.println("sqlQueue: no sql thread found.. wait for first one to die");
                            if(jThread != null) {
                                jThread.join();
                            }
    //                        System.out.println("sqlQueue: first sql thread died");
                        } catch (InterruptedException ex) {
                            // ignore and go ahead
                        }
                        catch(IndexOutOfBoundsException e) {
                            // was to slowly .. list already empty.. go ahead
                        }
                    }
                }
                this.resetStatement();
            }
        }
    }
    
    /**
     * wait until all issued threads are finished
     * 
     */
    public void join() {
        if(!this.useJDBC()) return;
        
        boolean done = false;
        while(!done) {
            if(this.execThreads.isEmpty()) return;
                try {
                    SQLExecute execThread = this.execThreads.get(0);
                if(execThread != null) {
                    try {
                        execThread.join();
                    } catch (InterruptedException ex) {
                        // wont happen
                    }
                } else {
                    /* it assumed that now other threads are created after this
                    method was called - it can be assumed that failing a single
                    thread in the list indicates that all threads are finished
                    */
                    done = true;
                }
            }
            catch(IndexOutOfBoundsException e) {
                // was to slowly .. empty.. again
            }
        }
    }
    
    /**
     * Accumulated sql statements could now be excecuted but don't have to.
     * Use that method as often as possible. Can increase performance dramatically.
     * @throws java.sql.SQLException
     */
    public void couldExecute() throws SQLException {
        if(this.sqlQueue == null) return;
        
        if(this.alwaysForce) {
            this.forceExecute();
        }
        
        if(this.sqlQueue.length() > SQLStatementQueue.MAX_BUFFER_LENGTH) {
            this.forceExecute(true);
        }
    }
    
    private boolean alwaysForce = false;
    
    /**
     * A couldExcecute is handled as forceExecute. That can cause
     * serious performance problems, handle with care
     * @param alwaysForce 
     */
    public void setCouldForce(boolean alwaysForce) {
        this.alwaysForce = alwaysForce;
    }
    
    /**
     * sequential execution of sql statement
     * @throws SQLException 
     */
    public void forceExecute() throws SQLException {
        if(this.sqlQueue == null || this.sqlQueue.length() < 1) {
            return;
        }
        
        if(!this.useJDBC()) {
            this.doNonJDBCExec();
            this.resetStatement();
        } else { // JDBC
            Connection conn = this.getFreeConnection();
            try {
                SQLExecute.doExec(conn, this.sqlQueue.toString());
            }
            catch(SQLException e) {
                Util.printExceptionMessage(errStream, e, this);
                throw e;
            }
            finally {
                // in any case
                this.resetStatement();
                this.setFreeConnection(conn);
            }
        }
    }
    
    void writeLog(String recordEntry) throws FileNotFoundException, IOException {
        if(this.recordFile == null || recordEntry == null) return;
        
        FileWriter fw = new FileWriter(this.recordFile);
        fw.write(recordEntry);
        fw.close();
    }
    
    public ResultSet executeWithResult() throws SQLException {
        Connection conn = this.getFreeConnection();
        PreparedStatement stmt = conn.prepareStatement(this.sqlQueue.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        
        try {
            ResultSet result = stmt.executeQuery();
        
            return result;
        }
        catch(SQLException e) {
            throw e;
        }
        finally {
            this.resetStatement();
            stmt.closeOnCompletion();
            this.setFreeConnection(conn);
        }
    }
    
    private String debugLastStatement;
    public void resetStatement() {
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
        StringBuilder sb = new StringBuilder();
        if(this.sqlQueue != null) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'||'HH:mm:ss.SSSXXX");
            sb.append("time: ");
            sb.append(df.format(new Date()));
            sb.append("\ncurrentStatement (not null): ");
            sb.append(this.sqlQueue);
        }
        sb.append("\nlastStatement was:\n");
        sb.append(this.debugLastStatement);
        
        return sb.toString();
    }
    
    public String getCurrentStatement() {
        if(this.sqlQueue == null) {
            return null;
        }
        
        return this.sqlQueue.toString();
    }

    synchronized void done(SQLExecute execThread) {
        this.execThreads.remove(execThread);
        Connection conn = execThread.getConnection();
        this.setFreeConnection(conn);
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
