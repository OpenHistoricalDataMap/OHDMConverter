package util;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author FlorianSauer
 */
public class CopyConnector {
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


    public CopyConnector(Parameter parameter, String tablename) throws IOException {
        String[] selectedColumns = null;
        this.tablename = tablename;
        this.delimiter = parameter.getDelimiter();
        switch (tablename) {
            case "nodes": {
//                System.out.println("nodes");
                selectedColumns = parameter.getNodesColumnNames();
                break;
            }
            case "relationmember": {
//                System.out.println("relationmember");
                selectedColumns = parameter.getRelationmemberColumnNames();
                break;
            }
            case "relations": {
//                System.out.println("relations");
                selectedColumns = parameter.getRelationsColumnNames();
                break;
            }
            case "waynodes": {
//                System.out.println("waynodes");
                selectedColumns = parameter.getWaynodesColumnNames();
                break;
            }
            case "ways": {
//                System.out.println("ways");
                selectedColumns = parameter.getWaysColumnNames();
                break;
            }
        }
        System.out.println("selectedColumns "+Arrays.toString(selectedColumns));
        try {
            this.connection = DB.createConnection(parameter);
            this.copyManager = new CopyManager((BaseConnection) connection);
            String sql = "COPY "+tablename+"("+String.join(", ", selectedColumns)+") FROM STDIN DELIMITER '"+delimiter+"' NULL 'NULL'";
//            System.out.println("SQL: "+sql);
            this.copyIn = this.copyManager.copyIn(sql);
        } catch (SQLException ex) {
            System.err.println("cannot connect to database - fatal - exit\n" + ex.getMessage());
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

    public void write(String csv) throws SQLException {
        //write csv string to stdin/stream of psql COPY
//        System.out.println("writing >"+csv+"< to "+this.tablename);
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

    public void write(List<String> csv) throws SQLException {
        this.write(this.escapeStrings((String[]) csv.toArray()));
    }
    public void write(String[] csv) throws SQLException {
        this.write(this.escapeStrings(csv));
    }

    private String escapeStrings(String[] list){
        String finalString = "";
        String tmp;
        for (String s : list){
            tmp = s;
            tmp = tmp.replace("\\", "\\\\");
            tmp = tmp.replace(this.delimiter, "\\"+this.delimiter);
            tmp = tmp.replace("\"", "\\\"");
            tmp = tmp.replace("\r", "\\\r");
            tmp = tmp.replace("\n", "\\\n");

//            if (tmp.contains(" ")){
//                tmp = "\""+tmp+"\"";
//            }




            finalString = finalString+tmp+this.delimiter;
        }
        return finalString.substring(0, finalString.length() - 1);
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
