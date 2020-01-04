package update;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DBUtil {

    public static int affectedRows(ResultSet resultSet) {
        int affectedRows = 0;
        try {
            while (resultSet.next()) {
                affectedRows++;
            }
        } catch (SQLException e) {
            //swallow//
            return 0;
        }
        return affectedRows;
    }
}
