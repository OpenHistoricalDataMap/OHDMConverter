package util;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;


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
            this.copyIn = this.copyManager.copyIn("COPY "+tablename+" FROM STDIN WITH DELIMITER '"+delimiter+"'");
        } catch (SQLException ex) {
            System.err.println("cannot connect to database - fatal - exit\n" + ex.getMessage());
            ex.printStackTrace(System.err);
            System.exit(1);
        }


    }

    public void write(String csv) throws SQLException {
        //write csv string to stdin/stream of psql COPY
        csv += "\n";
        byte[] bytes = csv.getBytes();
        this.copyIn.writeToCopy(bytes, 0, bytes.length);
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
