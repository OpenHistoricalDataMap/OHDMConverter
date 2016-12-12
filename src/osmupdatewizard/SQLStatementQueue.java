package osmupdatewizard;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    
    public SQLStatementQueue(Connection connection, int maxStatements, MyLogger logger) {
        this.connection = connection;
        this.maxLength = maxStatements;
        this.logger = logger;
    }
    
    public SQLStatementQueue(Connection connection, MyLogger logger) {
        this(connection, DEFAULT_MAX_SQL_STATEMENTS, logger);
    }
    
    public SQLStatementQueue(Connection connection) {
        this(connection, DEFAULT_MAX_SQL_STATEMENTS, null);
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
    
    public void append(BigDecimal a) {
        this.sqlQueue.append(a);
    }
    
    public void append(long a) {
        this.sqlQueue.append(Long.toString(a));
    }
    
    public void exec(String sqlStatement) {
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
    
    public void forceExecute() {
        if(this.sqlQueue == null) return;
        
        PreparedStatement stmt;
        try {
            stmt = connection.prepareStatement(this.sqlQueue.toString());
            stmt.execute();
            stmt.close();
        } catch (SQLException ex) {
            if(this.logger != null) {
                logger.print(1, ex.getLocalizedMessage(), true);
                logger.print(4, this.sqlQueue.toString());
            } else {
                System.err.println(ex.getMessage());
            }
        }
        
        this.sqlQueue = null;
    }
    
    public ResultSet executeWithResult() throws SQLException {
        PreparedStatement stmt = this.connection.prepareStatement(this.sqlQueue.toString());
        
        try {
            ResultSet result = stmt.executeQuery();
            this.sqlQueue = null;
        
            return result;
        }
        catch(SQLException e) {
            this.sqlQueue = null;
            throw e;
        }
    }
}
