package inter2ohdm;

import inter2ohdm.Util.TestUtil;
import inter2ohdm.Util.UpdateBufferUtil;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;


/**
 *
 * @author PyroFourTwenty
 */

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OHDMImporterPolygonTest {

    private String testMapV1Path = "test/testdata/testV1.osm";
    private String testMapV2Path = "test/testdata/testV2.osm";
    private String ohdmProperties;
    private String intermediateProperties;
    private String updateProperties;
    UpdateBufferUtil bufferUtil;

    @Test
    @Order(1)
    void importOsmTestFileV1() throws SQLException, IOException {
        String[] args = {"-o", testMapV1Path, "-i", intermediateProperties, "-d", ohdmProperties};
        TestUtil.importOsmTestFile(ohdmProperties, args);
        assertTrue(TestUtil.setBufferUtilResultSetsForTestMapV1());
    }

    @Test
    @Order(2)
    void testCase19PolygonIsImportedCorrectlyTestMapV1() throws SQLException {
        assertTrue(UpdateBufferUtil.tc19V1.next());
    }

    @Test
    @Order(3)
    void testCase20PolygonIsImportedTestMapV2() throws SQLException {
        assertTrue(UpdateBufferUtil.tc20V1.next());
    }

    @Test
    @Order(4)
    void testCase21PolygonIsNotExistentInTestMapV1() throws SQLException {
        assertFalse(UpdateBufferUtil.tc21V1.next());
    }

    @Test
    @Order(5)
    void testCase22PolygonIsExistentInTestMapV1() throws SQLException {
        assertTrue(UpdateBufferUtil.tc22V1.next());
    }

    @Test
    @Order(6)
    void testCase23PolygonIsExistentInTestMapV1() throws SQLException {
        assertTrue(UpdateBufferUtil.tc23V1.next());
    }

    @Test
    @Order(7)
    void testCase24PolygonIsExistentInTestMapV1() throws SQLException {
        assertTrue(UpdateBufferUtil.tc24V1.next());
    }

    @Test
    @Order(8)
    void testCase25PolygonIsExistentInTestMapV1() throws SQLException {
        assertTrue(UpdateBufferUtil.tc25V1.next());
    }

    @Test
    @Order(9)
    void testCase26PolygonIsExistentInTestMapV1() throws SQLException {
        assertTrue(UpdateBufferUtil.tc26V1.next());
    }

    @Test
    @Order(10)
    void testCase27PolygonIsExistentInTestMapV1() throws SQLException {
        assertTrue(UpdateBufferUtil.tc26V1.next());
    }

    @Test
    @Order(11)
    void importOsmTestFileV2() throws SQLException, IOException {
        String[] args = {"-o", testMapV2Path, "-i", intermediateProperties, "-d", ohdmProperties};
        TestUtil.importOsmTestFile(ohdmProperties, args);
        assertTrue(TestUtil.setBufferUtilResultSetsForTestMapV2());
    }

    @Test
    @Order(12)
    void testCase19ValidUntilChanges() throws SQLException {
        Date v1 = UpdateBufferUtil.tc19V1.getDate("valid_until");
        Date v2 = UpdateBufferUtil.tc19V2.getDate("valid_until");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(13)
    void testCase20PolygonIsNotUpdated() throws SQLException {
        Date v1 = UpdateBufferUtil.tc20V1.getDate("valid_until");
        Date v2 = UpdateBufferUtil.tc20V2.getDate("valid_until");
        assertEquals(v1, v2);
    }

    @Test
    @Order(14)
    void testCase21GeometryHasForeignKey() throws SQLException {
        long foreignKey = UpdateBufferUtil.tc21V2.getLong("id_polygone");
        assertNotNull(foreignKey);
    }

    @Test
    @Order(15)
    void testCase22ValidUntilDidNotChange() throws SQLException {
        Date v1 = UpdateBufferUtil.tc22V1.getDate("valid_until");
        Date v2 = UpdateBufferUtil.tc22V2.getDate("valid_until");
        assertEquals(v1, v2);
    }

    @Test
    @Order(16)
    void testCase22ValidSinceChanges() throws SQLException {
        Date v1 = UpdateBufferUtil.tc22V1.getDate("valid_since");
        Date v2 = UpdateBufferUtil.tc22V2.getDate("valid_since");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(17)
    void testCase22ForeignPolygonKeyChanges() throws SQLException {
        long v1 = UpdateBufferUtil.tc22V1.getLong("id_polygone");
        long v2 = UpdateBufferUtil.tc22V2.getLong("id_polygone");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(18)
    void testCase23ValidUntilDidNotChange() throws SQLException {
        Date v1 = UpdateBufferUtil.tc23V1.getDate("valid_until");
        Date v2 = UpdateBufferUtil.tc23V2.getDate("valid_until");
        assertEquals(v1, v2);
    }

    @Test
    @Order(19)
    void testCase23PrimaryKeyDiffers() throws SQLException {
        long v1 = UpdateBufferUtil.tc23V1.getLong("id");
        long v2 = UpdateBufferUtil.tc23V2.getLong("id");
        assertEquals(v1, v2);
    }

    @Test
    @Order(20)
    void testCase23ForeignPolygonKeyDiffers() throws SQLException {
        long v1 = UpdateBufferUtil.tc23V1.getLong("id_polygone");
        long v2 = UpdateBufferUtil.tc23V2.getLong("id_polygone");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(21)
    void testCase23ValidSinceDiffers() throws SQLException {
        Date v1 = UpdateBufferUtil.tc23V1.getDate("valid_since");
        Date v2 = UpdateBufferUtil.tc23V2.getDate("valid_since");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(22)
    void testCase24ValidUntilDidNotChange() throws SQLException {
        Date v1 = UpdateBufferUtil.tc24V1.getDate("valid_until");
        Date v2 = UpdateBufferUtil.tc24V2.getDate("valid_until");
        assertEquals(v1, v2);
    }

    @Test
    @Order(23)
    void testCase24PrimaryKeyChanges() throws SQLException {
        long v1 = UpdateBufferUtil.tc24V1.getLong("id");
        long v2 = UpdateBufferUtil.tc24V2.getLong("id");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(24)
    void testCase24ForeignPolygonKeyDiffers() throws SQLException {
        long v1 = UpdateBufferUtil.tc24V1.getLong("id_polygone");
        long v2 = UpdateBufferUtil.tc24V2.getLong("id_polygone");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(24)
    void testCase24ValidSinceDiffers() throws SQLException {
        Date v1 = UpdateBufferUtil.tc24V1.getDate("valid_since");
        Date v2 = UpdateBufferUtil.tc24V2.getDate("valid_since");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(25)
    void testCase25PrimaryKeyDiffers() throws SQLException {
        long v1 = UpdateBufferUtil.tc25V1.getLong("id");
        long v2 = UpdateBufferUtil.tc25V2.getLong("id");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(26)
    void testCase25ForeignPolygonKeyDidNotChange() throws SQLException {
        long v1 = UpdateBufferUtil.tc25V1.getLong("id_polygone");
        long v2 = UpdateBufferUtil.tc25V2.getLong("id_polygone");
        assertEquals(v1, v2);
    }

    @Test
    @Order(27)
    void testCase25ForeignClassificationKeyDiffers() throws SQLException {
        long v1 = UpdateBufferUtil.tc25V1.getLong("classification_id");
        long v2 = UpdateBufferUtil.tc25V2.getLong("classification_id");
        assertNotEquals(v1 ,v2);
    }

    @Test
    @Order(28)
    void testCase25ValidUntilDidNotChange() throws SQLException {
        Date v1 = UpdateBufferUtil.tc25V1.getDate("valid_until");
        Date v2 = UpdateBufferUtil.tc25V2.getDate("valid_until");
        assertEquals(v1, v2);
    }

    @Test
    @Order(29)
    void testCase26ValidUntilDidNotChange() throws SQLException {
        Date v1 = UpdateBufferUtil.tc26V1.getDate("valid_until");
        Date v2 = UpdateBufferUtil.tc26V2.getDate("valid_until");
        assertEquals(v1, v2);
    }

    @Test
    @Order(30)
    void testCase26PrimaryKeyDiffers() throws SQLException {
        long v1 = UpdateBufferUtil.tc26V1.getLong("id");
        long v2 = UpdateBufferUtil.tc26V2.getLong("id");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(31)
    void testCase26ForeignLinesKeyDidNotChange() throws SQLException {
        long v1 = UpdateBufferUtil.tc26V1.getLong("id_line");
        long v2 = UpdateBufferUtil.tc26V2.getLong("id_line");
        assertEquals(v1, v2);
    }

    @Test
    @Order(31)
    void testCase26ForeignClassificationKeyDidNotChange() throws SQLException {
        long v1 = UpdateBufferUtil.tc26V1.getLong("classification_id");
        long v2 = UpdateBufferUtil.tc26V2.getLong("classification_id");
        assertEquals(v1, v2);
    }

    @Test
    @Order(32)
    void testCase27ValidUntilDidNotChange() throws SQLException {
        Date v1 = UpdateBufferUtil.tc27V1.getDate("valid_until");
        Date v2 = UpdateBufferUtil.tc27V2.getDate("valid_until");
        assertEquals(v1, v2);
    }

    @Test
    @Order(33)
    void testCase27PrimaryKeyDiffers() throws SQLException {
        long v1 = UpdateBufferUtil.tc27V1.getLong("id");
        long v2 = UpdateBufferUtil.tc27V2.getLong("id");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(34)
    void testCase27ForeignPolygonKeyDiffers() throws SQLException {
        long v1 = UpdateBufferUtil.tc27V1.getLong("id_polygone");
        long v2 = UpdateBufferUtil.tc27V1.getLong("id_polygone");
        assertNotEquals(v1, v2);
    }

    @Test
    @Order(35)
    void testCase27ForeignValidSinceDiffers() throws SQLException {
        Date v1 = UpdateBufferUtil.tc27V1.getDate("valid_since");
        Date v2 = UpdateBufferUtil.tc27V2.getDate("valid_since");
        assertNotEquals(v1, v2);
    }

}
