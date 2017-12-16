package util;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author FlorianSauer
 */
public class DBCopyConnector {
    private String tablename;
    private String delimiter;
    private CopyManager copyManager;
    private CopyIn copyIn;

    public String getTablename() {
        return tablename;
    }

    public Connection getConnection() {
        return connection;
    }

    private Connection connection;
    private long writtenLines = 0;

    public DBCopyConnector(Parameter parameter, String tablename) throws IOException {
        this.tablename = tablename;
        this.delimiter = parameter.getDelimiter();

        try {
            this.connection = DB.createConnection(parameter);
            this.copyManager = new CopyManager((BaseConnection) connection);
            String sql = "COPY "+tablename+" FROM STDIN DELIMITER '"+delimiter+"' NULL 'NULL'";
            System.out.println(sql);
            this.copyIn = this.copyManager.copyIn(sql);
        } catch (SQLException ex) {
            System.err.println("cannot connect to database - fatal - exit\n" + ex.getMessage());
            ex.printStackTrace(System.err);
            System.exit(1);
        }


    }

    public void write(String csv) throws SQLException {
        //write csv string to stdin/stream of psql COPY
        System.out.println("writing >"+csv+"< to "+this.tablename);
        csv += "\n";
        byte[] bytes = csv.getBytes();
        try {
            this.copyIn.writeToCopy(bytes, 0, bytes.length);
            this.copyIn.flushCopy();
        } catch (Exception e){
            // TODO: 07.12.2017 remove try catch, only for debugging
            System.out.println("could not write >"+csv.replaceAll("(\\r|\\n)", "")+"< to "+this.tablename);
            e.printStackTrace();
            throw e;
        }
        this.writtenLines += 1;

    }

    public String getDelimiter() {
        return this.delimiter;
    }

    public long endCopy() throws SQLException {
        //close connection+stdin/stream of COPY
        long postgres_writtenrows = 0;
        postgres_writtenrows = this.copyIn.endCopy();
        return postgres_writtenrows;
    }
    public void close() throws SQLException {
        //close connection+stdin/stream of COPY
        this.connection.close();
    }

    public long getWrittenLines() {
        return writtenLines;
    }
}
