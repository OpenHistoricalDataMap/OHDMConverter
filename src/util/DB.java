package util;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

/**
 *
 * @author thsc
 */
public class DB {
    private static final String TIME_STAMP_TABLE = "creationInformation";

    ////////////////////////////////////////////////////////////////////////
    //                          CREATE STRUCTURES                         //
    ////////////////////////////////////////////////////////////////////////

    // ids are defined identically in each table
    static public String getCreateTableBegin(String schema, String tableName) {
        StringBuilder sb = new StringBuilder();

        sb.append("DROP TABLE IF EXISTS ");
        sb.append(DB.getFullTableName(schema, tableName));
        sb.append("; ");

        sb.append("CREATE TABLE IF NOT EXISTS ");
        sb.append(DB.getFullTableName(schema, tableName));
        sb.append(" (");
        sb.append(DB.getCreatePrimaryKeyDescription(schema, tableName));
        
        return sb.toString();
    }
    
    // primary key are created identically
    static public String getCreatePrimaryKeyDescription(String schema, String tableName) {
        return  "id bigint PRIMARY KEY DEFAULT nextval('"
                + DB.getSequenceName(DB.getFullTableName(schema, tableName))
                + "'::regclass)";

    }
    
    // primary key are created identically
//    static public String getCreatePrimaryKeyDescription(String schema, String tableName) {
//        return "id bigint NOT NULL DEFAULT nextval('"
//                + DB.getSequenceName(DB.getFullTableName(schema, tableName))
//                + "'::regclass),"
//                + " CONSTRAINT "
//                + tableName
//                + "_pkey PRIMARY KEY (id)";
//    }
    
    static public void createSequence(Connection targetConnection, String schema, String tableName) throws SQLException {
        DB.createSequence(new SQLStatementQueue(targetConnection), schema, tableName);
    }
    
    static public void createSequence(SQLStatementQueue targetQueue, String schema, String tableName) throws SQLException {
        
        targetQueue.append("CREATE SEQUENCE IF NOT EXISTS ");
        targetQueue.append(DB.getSequenceName(DB.getFullTableName(schema, tableName)));
        targetQueue.append(" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;");
        targetQueue.forceExecute();
    }
    
    static public void drop(Connection targetConnection, String schema, String tableName) throws SQLException {
        DB.drop(new SQLStatementQueue(targetConnection), schema, tableName);
    }
    
    /**
     * Drop table - at least try.. SQL Exceptions are just reported to stderr
     * @param sq
     * @param schema
     * @param tableName 
     */
    static public void drop(SQLStatementQueue sq, String schema, String tableName) {
        drop(sq, DB.getFullTableName(schema, tableName));
    }

    static public void drop(SQLStatementQueue sq, String fullTableName) {
        try {
            sq.append("DROP SEQUENCE IF EXISTS ");
            sq.append(DB.getSequenceName(fullTableName));
            sq.append(" CASCADE;");
            sq.forceExecute();
        }
        catch(SQLException e) {
            System.err.println("cannot drop sequence (prob. non-fatal): " + sq.toString());
        }
        try {
            sq.append("DROP TABLE ");
            sq.append(fullTableName);
            sq.append(" CASCADE;");
            sq.forceExecute();
        }
        catch(SQLException e) {
            System.out.println("cannot drop table (can be ignored in most cases): " + sq.toString());
        }
    }
    
    ////////////////////////////////////////////////////////////////////////
    //                                names                               //
    ////////////////////////////////////////////////////////////////////////


    static public String getFullTableName_(String schema, String tableName) {
        return schema + ".\"" + tableName ;
    }static public String getSequenceName(String tableName) {
        return tableName.substring(0, tableName.length() - 1) + "_id\" ";
    }
    
    static public String getFullTableName(String schema, String tableName) {
        return schema + ".\"" + tableName+"\"";
    }



    ////////////////////////////////////////////////////////////////////////
    //                                helper                               //
    ////////////////////////////////////////////////////////////////////////
    
    public static Connection createConnection(Parameter parameter) throws SQLException{
        Properties connProps = new Properties();
        connProps.put("user", parameter.getUserName());
        connProps.put("password", parameter.getPWD());
        
        connProps.put("tcpKeepAlive", "true");
        
        Driver d = null;
//        System.out.println("debugging: find jdbc driver by class name: org.postgresql.Driver");
//        if(!drivers.hasMoreElements()) {
            try {
                Class driverClass = Class.forName("org.postgresql.Driver");
//                System.out.println("debugging: found class.. try to cast");

                Object newInstance = driverClass.getConstructor().newInstance();
                
                d = (Driver)newInstance;
            }
            catch(Throwable re) {
                System.out.println("debugging: runtime problem: " + re.getMessage());
                re.printStackTrace();
            }
            
//        }
        
        Connection connection;
        try {
            connection = d.connect(
                    "jdbc:postgresql://" + parameter.getServerName()
                    + ":" + parameter.getPortNumber() + "/" + parameter.getdbName(), connProps);
            
            
//            connection = DriverManager.getConnection(
//                    "jdbc:postgresql://" + parameter.getServerName()
//                    + ":" + parameter.getPortNumber() + "/" + parameter.getdbName(), connProps);
        
            if (parameter.getSchema() != null && !parameter.getSchema().equalsIgnoreCase("")) {
                StringBuilder sql = new StringBuilder("SET search_path = public, ");
                sql.append(parameter.getSchema());
                PreparedStatement stmt = connection.prepareStatement(sql.toString());
                stmt.execute();
            }
        
//            connection.setAutoCommit(false);
            return connection;
        
        } catch (SQLException ex) {
            System.err.println("cannot connect to database - fatal - exit\n" + ex.getMessage());
            ex.printStackTrace(System.err);
        }
        
        System.exit(1);
        return null;
    }
    
    public static SQLStatementQueue createSQLStatementQueue(Connection target, Parameter parameter) {
        File recordFile = new File(parameter.getRecordFileName());
        int maxThreads = 1;
        try {
            String v = parameter.getMaxThread();
            maxThreads = Integer.parseInt(v.trim());
            maxThreads = maxThreads > 0 ? maxThreads : 1;
        }
        catch(NumberFormatException e) {
            System.err.println("no integer value (run single threaded instead): " + parameter.getMaxThread());
            maxThreads = 1;
        }
        
        return new SQLStatementQueue(target, recordFile, maxThreads);
    }

    public static void writeTimeStamp(SQLStatementQueue sq, String targetSchema) throws SQLException {
        DB.drop(sq, targetSchema, DB.TIME_STAMP_TABLE);

        sq.append("CREATE TABLE IF NOT EXISTS ");
        sq.append(DB.getFullTableName(targetSchema, DB.TIME_STAMP_TABLE));
        sq.append(" (");
        sq.append("timestampstring character varying");
        sq.append(");");
        sq.forceExecute();

        // get time string
        System.currentTimeMillis();
        Date time = Calendar.getInstance().getTime();
        DateFormat df = DateFormat.getInstance();
        String timeString = df.format(time);

        sq.append("INSERT INTO ");
        sq.append(DB.getFullTableName(targetSchema, DB.TIME_STAMP_TABLE));
        sq.append(" VALUES (' ");
        sq.append(timeString);
        sq.append(" '); ");

        sq.forceExecute();

        /*
        sq.append("INSERT ");
        sq.append(DB.getFullTableName(targetSchema, TABLE_IMPORTS_UPDATES));
        sq.append(" SET lastupdate = '");
        sq.append(osmfilecreationdate);
        sq.append("' WHERE externalsystemID = 0;");
        sq.forceExecute();

         */
    }
}
