package update;

import org.junit.jupiter.api.*;
import util.DB;
import util.OHDMConverter;
import util.Parameter;
import util.SQLStatementQueue;

import java.sql.ResultSet;


public class ImportUpdateTest {

    private static final String MY_IMPORT_MAP = "test/resources/myImportMap.osm";
    private static final String MY_UPDATE_MAP = "test/resources/myUpdateMap.osm";
    private static final String DB_INTER_SETTINGS = "db_inter.txt";
    private static final String DB_UPDATE_SETTINGS = "test/resources/db_update.txt";

    private static SQLStatementQueue sqlInter, sqlUpdate;

    @BeforeAll
    public static void init() throws Exception {
        sqlInter = new SQLStatementQueue(DB.createConnection(new Parameter(DB_INTER_SETTINGS)));
        sqlUpdate = new SQLStatementQueue(DB.createConnection(new Parameter(DB_UPDATE_SETTINGS)));
        //insert import map into intermediate db
        String[] args = {"-o", MY_IMPORT_MAP, "-i", DB_INTER_SETTINGS};
        OHDMConverter.main(args);
    }

    @AfterAll
    public static void destroy() throws Exception {
        sqlInter.close();
        sqlUpdate.close();
    }

    /**
     * Since we now that this should work -> only assert that we have the correct number of rows inserted into the DB
     *
     * @throws Exception throws exception if something fails
     */
    @Test
    @Order(1)
    public void importIntoInterDatabase() throws Exception {
        //Assert that 30 nodes are inserted into the intermediate database
        sqlInter.append("SELECT * FROM nodes;");
        ResultSet resNodes = sqlInter.executeWithResult();
        Assertions.assertEquals(30, DBUtil.affectedRows(resNodes));
        resNodes.close();

        //Assert that 6 ways are inserted into the intermediate database
        sqlInter.append("SELECT * FROM ways;");
        ResultSet resWays = sqlInter.executeWithResult();
        Assertions.assertEquals(6, DBUtil.affectedRows(resWays));
        resWays.close();

        //Assert that 2 relationships are inserted into intermediate database
        sqlInter.append("SELECT * FROM relations;");
        ResultSet resRel = sqlInter.executeWithResult();
        Assertions.assertEquals(2, DBUtil.affectedRows(resRel));
        resRel.close();
    }

    @Test
    @Order(2)
    public void importIntoUpdateDatabase() throws Exception {
        String[] args = {"-o", MY_UPDATE_MAP, "-i", DB_UPDATE_SETTINGS};
        OHDMConverter.main(args);

        //TODO!! THIS IS CURRENTLY JUST FOR DEV! CHANGE IT TO THE SETTINGS DOWN THERE
        /*
            //This is the actual call to the update process here!!

        String[] args = {"-o", MY_UPDATE_MAP, "-i", DB_INTER_SETTINGS, "-u", DB_UPDATE_SETTINGS, "-d", DB_OHDM_SETTINGS}; //"-d", DB_OHDM_SETTINGS, "-t", "2019-01-01"
        OHDMConverter.main(args);
         */

        //Assert that 32 nodes are inserted into the intermediate database
        sqlUpdate.append("SELECT * FROM nodes;");
        ResultSet resNodes = sqlUpdate.executeWithResult();
        Assertions.assertEquals(31, DBUtil.affectedRows(resNodes));
        resNodes.close();

        //Assert that 6 ways are inserted into the intermediate database
        sqlUpdate.append("SELECT * FROM ways;");
        ResultSet resWays = sqlUpdate.executeWithResult();
        Assertions.assertEquals(8, DBUtil.affectedRows(resWays));
        resWays.close();

        //Assert that 2 relationships are inserted into intermediate database
        sqlUpdate.append("SELECT * FROM relations;");
        ResultSet resRel = sqlUpdate.executeWithResult();
        Assertions.assertEquals(2, DBUtil.affectedRows(resRel));
        resRel.close();
    }

    @Test
    @Order(3)
    public void nodesChanged() throws Exception {

        //Assert nodes changed
        sqlUpdate.append("SELECT * FROM nodes WHERE osm_id = -102286 OR osm_id = -102292;");
        ResultSet resRel = sqlUpdate.executeWithResult();
        Assertions.assertEquals(2, DBUtil.affectedRows(resRel));

        resRel.first();
        String actualTimestamp = resRel.getString("tstamp");
        Assertions.assertEquals("2019-11-01", actualTimestamp);

        resRel.next();
        actualTimestamp = resRel.getString("tstamp");
        Assertions.assertEquals("2019-11-01", actualTimestamp);

        resRel.close();
    }

    @Test
    @Order(4)
    public void nodesDeleted() throws Exception {

        //Assert nodes deleted
        sqlUpdate.append("SELECT * FROM nodes WHERE osm_id IN (-102262, -102264, -102266, -102268, -102270, -102272);");
        ResultSet resRel = sqlUpdate.executeWithResult();
        Assertions.assertEquals(0, DBUtil.affectedRows(resRel));
        resRel.close();
    }


    @Test
    @Order(5)
    public void nodesAdded() throws Exception {

        //Assert nodes added
        sqlUpdate.append("SELECT * FROM nodes WHERE osm_id IN (-107533, -107534, -107536, -107537, -107705, -107751, -107775);");
        ResultSet resRel = sqlUpdate.executeWithResult();
        Assertions.assertEquals(7, DBUtil.affectedRows(resRel));

        resRel.first();
        String actualTimestamp;
        while (resRel.next()) {
            actualTimestamp = resRel.getString("tstamp");
            Assertions.assertEquals("2019-11-01", actualTimestamp);
        }

        resRel.close();
    }

    @Test
    @Order(6)
    public void waysChanged() throws Exception {

        //Assert ways changed
        sqlUpdate.append("SELECT * FROM ways WHERE osm_id IN (-102324, -102325, -102327);");
        ResultSet resRel = sqlUpdate.executeWithResult();
        Assertions.assertEquals(3, DBUtil.affectedRows(resRel));

        resRel.first();
        String actualTimestamp;
        while (resRel.next()) {
            actualTimestamp = resRel.getString("tstamp");
            Assertions.assertEquals("2019-11-01", actualTimestamp);
        }

        resRel.close();
    }

    @Test
    @Order(7)
    public void waysDeleted() throws Exception {

        //Assert ways deleted
        sqlUpdate.append("SELECT * FROM ways WHERE osm_id = -102322;");
        ResultSet resRel = sqlUpdate.executeWithResult();
        Assertions.assertEquals(0, DBUtil.affectedRows(resRel));
        resRel.close();
    }

    @Test
    @Order(8)
    public void waysAdded() throws Exception {

        //Assert ways added
        sqlUpdate.append("SELECT * FROM ways WHERE osm_id IN (-107535, -107538, -107563);");
        ResultSet resRel = sqlUpdate.executeWithResult();
        Assertions.assertEquals(3, DBUtil.affectedRows(resRel));

        resRel.first();
        String actualTimestamp;
        while (resRel.next()) {
            actualTimestamp = resRel.getString("tstamp");
            Assertions.assertEquals("2019-11-01", actualTimestamp);
        }

        resRel.close();
    }

    @Test
    @Order(9)
    public void relationsChanged() throws Exception {

        //Assert relations changed
        sqlUpdate.append("SELECT * FROM relations WHERE CAST(tstamp AS DATE) = '2019-11-01';");
        ResultSet resRel = sqlUpdate.executeWithResult();
        Assertions.assertEquals(0, DBUtil.affectedRows(resRel));
        resRel.close();
    }

    @Test
    @Order(10)
    public void relationsDeleted() throws Exception {

        //Assert relations deleted
        sqlUpdate.append("SELECT * FROM relations WHERE CAST(tstamp AS DATE) = '2019-11-01';");
        ResultSet resRel = sqlUpdate.executeWithResult();
        Assertions.assertEquals(0, DBUtil.affectedRows(resRel));
        resRel.close();
    }

    @Test
    @Order(11)
    public void relationsAdded() throws Exception {

        //Assert relations added
        sqlUpdate.append("SELECT * FROM relations WHERE CAST(tstamp AS DATE) = '2019-11-01';");
        ResultSet resRel = sqlUpdate.executeWithResult();
        Assertions.assertEquals(0, DBUtil.affectedRows(resRel));
        resRel.close();
    }

}