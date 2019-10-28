package inter2ohdm;

import org.junit.jupiter.api.Test;
import util.*;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author kakaoKeks
 */


/**
 * These tests are created for testing the basic functionalities of the system.
 * first a test map is imported to a test database.
 * With the data imported we request all the parameters we want to test via sql statements and save the results in variables.
 * Next we update the database using a second test map where we have changed some aspects to cover all test cases.
 * And last we are fetching the new data again and check if it has changed (or in some cases has not) as it was supposed to.
 * **/

class OHDMImporterTest {

    private String geoobject_geometry = OHDM_DB.TABLE_GEOOBJECT_GEOMETRY;
    private String geoobject =OHDM_DB.TABLE_GEOOBJECT;
    private String classification = OHDM_DB.TABLE_CLASSIFICATION;

    private Parameter parameter = null;
    private SQLStatementQueue insertSQL = null;

    /** hier bitte die Pfade angeben*/
    static String ohdmProperties;
    static String intermediateProperties;
    static String updateProperties;
    private String testMapV1 = "test/testdata/testMapV1.osm";
    private String testMapV2 = "test/testdata/testMapV2.osm";

    @Test
    public void nodeUnchanged() throws IOException, SQLException {
        //Import
        parameter = new Parameter(ohdmProperties);
        insertSQL = new SQLStatementQueue(DB.createConnection(parameter));
        String [] args = {"-o",testMapV1,"-i",intermediateProperties,"-d",ohdmProperties};
        OHDMConverter.main(args);

        //TC-04 Node unchanged
        Date date04 =getDate("04-TC");
        long id04 = getIdByName("04-TC");

        //TC-05 Node moves
        Date date05 = getDate("TC-05");
        long id05 = getIdByName("TC-05");

        //TC-06 Node gets deleted
        Date date06TC = getDate("06-TC");
        long id06 = getIdByName("06-TC");

        //TC-07 classification changes
        insertSQL.append("select valid_until");
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(".");
        insertSQL.append("id from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(geoobject);
        insertSQL.append(".id and classification_id =");
        insertSQL.append(classification);
        insertSQL.append(".id and name = '07-TC'");

        ResultSet resultSet07 = insertSQL.executeWithResult();
        resultSet07.next();
        Date date07 = resultSet07.getDate("valid_until");
        long id07 = resultSet07.getLong("id");
        long geoobjGeomID07 = resultSet07.getLong("geoobj_geom_id");
        insertSQL.flushThreads();

        //08-TC Object changes

        insertSQL.append("select valid_until, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(".");
        insertSQL.append("id as class_id from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(".id and name = '08-TC'");
        ResultSet resultSet08 = insertSQL.executeWithResult();
        resultSet08.next();
        Date date08 = resultSet08.getDate("valid_until");
        long geoobjGeomID08 = resultSet08.getLong("geoobj_geom_id");
        long classID08 = resultSet08.getLong("class_id");
        insertSQL.flushThreads();

        //28-TC
        insertSQL.append("select valid_until");
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(".");
        insertSQL.append("id from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(geoobject);
        insertSQL.append(".id and classification_id =");
        insertSQL.append(classification);
        insertSQL.append(".id and name = '28-TC' and ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(".subclassname = 'hotel';");


        ResultSet resultSet28 = insertSQL.executeWithResult();
        resultSet28.next();
        Date date28 = resultSet28.getDate("valid_until");
        long classid28 = resultSet28.getLong("id");
        long geoobjGeomID28 = resultSet28.getLong("geoobj_geom_id");
        insertSQL.flushThreads();

        //29-TC New object is linked to relation
        long geoobjGeomID29 = getIdByName("29-TC");
        Date date29 = getDateById(geoobjGeomID29);
        long geoobjID29 = getObjectIDByGeobjGeomID(geoobjGeomID29);

        //30-TC new polygone is added to Relation
        insertSQL.append("SELECT name,count(name) FROM ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(" GROUP BY name HAVING name='30-TC';");

        ResultSet resultSet30 = insertSQL.executeWithResult();
        resultSet30.next();
        int numberOfObjects = resultSet30.getInt("count");

        //33-TC Way of a relation changes
        insertSQL.append("select ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id_target from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(geoobject);
        insertSQL.append(".id and name = '33-TC' and ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".type_target = 2;");

        ResultSet resultSet33 = insertSQL.executeWithResult();
        resultSet33.next();
        long id_target33 = resultSet33.getLong("id_target");
        long geoobjGeomID33 = resultSet33.getLong("geoobj_geom_id");
        insertSQL.flushThreads();

        //34-TC Polygone of a relation changes
        insertSQL.append("select ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id_target from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(geoobject);
        insertSQL.append(".id and name = '34-TC' and ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".type_target = 3;");

        ResultSet resultSet34 = insertSQL.executeWithResult();
        resultSet34.next();
        long id_target34 = resultSet34.getLong("id_target");
        long geoobjGeomID34 = resultSet34.getLong("geoobj_geom_id");
        insertSQL.flushThreads();

        //36-TC polygon gets deleted from relation
        //that means that nothing changes for the record. It stays exactly the same
        ArrayList<Long> idList36 = getListOfIds("36-TC");
        ArrayList<Date> dateList36 = getListOfDates("36-TC");

        //37-TC node gets deleted from relation
        //that means that nothing changes for the record. It stays exactly the same
        ArrayList<Long> idList37 = getListOfIds("37-TC");
        ArrayList<Date> dateList37 = getListOfDates("37-TC");

        //38-TC way gets deleted from relation
        //that means that nothing changes for the record. It stays exactly the same
        ArrayList<Long> idList38 = getListOfIds("38-TC");
        ArrayList<Date> dateList38 = getListOfDates("38-TC");

        //39-TC relation doesn't change
        //that means that the valid_until field for all components of the relation are getting updated
        //first fetch all the ids and valid_until records from the relation
        ArrayList<Long> idList39 = getListOfIds("39-TC");
        ArrayList<Date> dateList39 = getListOfDates("39-TC");

        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        //update -o -i -u -d
        //String [] args2 = {"-o",testMapV2,"-i",intermediateProperties,"-u",updateProperties,"-d",ohdmProperties};
        //OHDMConverter.main(args2);
        //////////////////////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////////////////////


        assertNotEquals(date04,getDateById(id04));

        //TC-05 Node moves
        //check that the date hasn't updated
        String valid = date05.toString();
        assertEquals(date05,getDateById(id05));
        //check that there is a new record in the geoobject-geometry table having a foreign key to the record
        //in the geoobject table with the name '05-TC' plus having the current update date as 'valid_until' value
        insertSQL.append("select valid_until from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(geoobject);
        insertSQL.append(".id and name = '05-TC'");
        insertSQL.append("and valid_until <> '");
        insertSQL.append(valid);
        insertSQL.append("';");

        ResultSet resultSet05V2 = insertSQL.executeWithResult();
        resultSet05V2.next();
        Date date05V2 = resultSet05V2.getDate("valid_until");
        insertSQL.flushThreads();

        assertNotEquals(date05,date05V2);

        //06-TC node got deleted - check that the date hasn't changed
        assertEquals(date06TC,getDate("06-TC"));

        //TC-07 classification has changed
        insertSQL.append("select valid_until");
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(".");
        insertSQL.append("id from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(geoobject);
        insertSQL.append(".id and classification_id =");
        insertSQL.append(classification);
        insertSQL.append(".id and name = '07-TC'");
        insertSQL.append("and valid_until <> '");
        insertSQL.append(date07.toString());
        insertSQL.append("';");

        ResultSet resultSet07B = insertSQL.executeWithResult();
        resultSet07B.next();
        assertNotEquals(id07,resultSet07B.getLong("id"));
        insertSQL.flushThreads();

        //TC07 valid_until from the record before the update hasn't changed
        assertEquals(date07, getDateById(geoobjGeomID07));

        //TC08 valid_until from the record before the update hasn't changed
        assertEquals(date08, getDateById(geoobjGeomID08));

        //08-TC Object changes
        insertSQL.append("select valid_until, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(".");
        insertSQL.append("id as class_id from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(".id and name = '08-TCV2'");
        ResultSet resultSet08V2 = insertSQL.executeWithResult();
        resultSet08V2.next();
        Date date08V2 = resultSet08V2.getDate("valid_until");
        long geoobjGeomID08V2 = resultSet08V2.getLong("geoobj_geom_id");
        long classID08V2 = resultSet08V2.getLong("class_id");
        insertSQL.flushThreads();

        assertNotEquals(date08,date08V2);
        assertEquals(classID08,classID08V2);
        assertNotEquals(geoobjGeomID08,geoobjGeomID08V2);

        //09-TC
        insertSQL.append("select valid_from from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(this.geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(this.geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(this.geoobject);
        insertSQL.append(".id and name = '09-TC';");
        ResultSet resultSet = insertSQL.executeWithResult();
        resultSet.next();
        Date dateActual = resultSet.getDate("valid_from");
        insertSQL.flushThreads();
        Date dateExpected = new Date(2001-04-21);
        assertEquals(dateExpected, dateActual);

        //28-TC Classification of Relation changes
        insertSQL.append("select valid_until");
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(".");
        insertSQL.append("id from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(geoobject);
        insertSQL.append(".id and classification_id =");
        insertSQL.append(classification);
        insertSQL.append(".id and name = '28-TC' and ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(classification);
        insertSQL.append(".subclassname = 'university';");


        ResultSet resultSet28b = insertSQL.executeWithResult();
        resultSet28b.next();
        Date date28b = resultSet28b.getDate("valid_until");
        long classid28b = resultSet28b.getLong("id");
        long geoobjGeomID28b = resultSet28b.getLong("geoobj_geom_id");
        insertSQL.flushThreads();
        assertNotEquals(date28,date28b);
        assertNotEquals(classid28,classid28b);
        assertNotEquals(geoobjGeomID28,geoobjGeomID28b);

        //New Object is linked to relation
        //check if the date from the old record stays the same
        assertEquals(date29,getDateById(geoobjGeomID29));
        //check that there is a new record with the new Object with another valid_until date
        long id29V2 = getIdByName("29_TCV2");
        assertNotEquals(date29,getDateById(id29V2));
        //check that the id_object_source is not the same for the two records
        assertNotEquals(geoobjID29,getObjectIDByGeobjGeomID(id29V2));

        //30-TC Geometry (Node/way/polygone) is added to Relation
        insertSQL.append("SELECT name,count(name) FROM ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(" GROUP BY name HAVING name='30-TC';");

        ResultSet resultSet30b = insertSQL.executeWithResult();
        resultSet30b.next();
        int numberOfObjectsb = resultSet30.getInt("count");
        assertNotEquals(numberOfObjects,numberOfObjectsb);

        //33-TC Way of a relation has changed
        insertSQL.append("select ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id_target from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(geoobject);
        insertSQL.append(".id and name = '33-TC' and ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".type_target = 2 and ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".id <> ");
        insertSQL.append(geoobjGeomID33);
        insertSQL.append(";");

        ResultSet resultSet33b = insertSQL.executeWithResult();
        resultSet33b.next();
        long id_target33b = resultSet33b.getLong("id_target");
        long geoobjGeomID33b = resultSet33b.getLong("geoobj_geom_id");
        insertSQL.flushThreads();
        //the new record in the geoobject_geometry table points to another way id
        assertNotEquals(id_target33,id_target33b);
        assertNotEquals(getDateById(geoobjGeomID33),getDateById(geoobjGeomID33b));

        //34-TC polygon of a relation has changed
        insertSQL.append("select ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id, ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id_target from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(geoobject);
        insertSQL.append(".id and name = '34-TC' and ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".type_target = 3 and ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".id <> ");
        insertSQL.append(geoobjGeomID34);
        insertSQL.append(";");

        ResultSet resultSet34b = insertSQL.executeWithResult();
        resultSet34b.next();
        long id_target34b = resultSet34b.getLong("id_target");
        long geoobjGeomID34b = resultSet34b.getLong("geoobj_geom_id");
        insertSQL.flushThreads();
        //the new record in the geoobject_geometry table points to another way id
        assertNotEquals(id_target34,id_target34b);
        assertNotEquals(getDateById(geoobjGeomID34),getDateById(geoobjGeomID34b));

        //36-TC polygon has been deleted from relation
        //iterating through list of ids to get the dates after the update
        ArrayList<Date> dateList36b=new ArrayList<>();
        for(Long id:idList36){
            dateList36b.add(getDateById(id));
        }
        //check that the dates are still the same after the update
        assertEquals(dateList36,dateList36b);

        //37-TC node has been deleted from relation
        //iterating through list of ids to get the dates after the update
        ArrayList<Date> dateList37b=new ArrayList<>();
        for(Long id:idList37){
            dateList37b.add(getDateById(id));
        }
        //check that the dates are still the same after the update
        assertEquals(dateList37,dateList37b);

        //38-TC way has been deleted from relation
        //iterating through list of ids to get the dates after the update
        ArrayList<Date> dateList38b=new ArrayList<>();
        for(Long id:idList38){
            dateList38b.add(getDateById(id));
        }
        //check that the dates are still the same after the update
        assertEquals(dateList38,dateList38b);

        //39-TC ralation was not changed
        //so now after the update the valid_until record should be up to date of the latest update
        //check if the date is not the same as before anymore
        ArrayList<Date> dateList39b=new ArrayList<>();
        for(Long id:idList39){
            dateList39b.add(getDateById(id));
        }
        assertNotEquals(dateList39,dateList39b);

    }

    private Date getDate(String name){
        Date date = null;
        try {
            insertSQL.append("select valid_until from ");
            insertSQL.append(parameter.getSchema());
            insertSQL.append(".");
            insertSQL.append(this.geoobject_geometry);
            insertSQL.append(",");
            insertSQL.append(parameter.getSchema());
            insertSQL.append(".");
            insertSQL.append(this.geoobject);
            insertSQL.append(" where id_geoobject_source=");
            insertSQL.append(parameter.getSchema());
            insertSQL.append(".");
            insertSQL.append(this.geoobject);
            insertSQL.append(".id and name = '" + name + "';");
            ResultSet resultSet = insertSQL.executeWithResult();
            resultSet.next();
            date = resultSet.getDate("valid_until");
            insertSQL.flushThreads();
            System.out.println("Date: " + date);
        }catch(SQLException e){
            e.printStackTrace();
        }
        return date;
    }
    private Date getDateById(long id){
        Date date = null;
        try {
            insertSQL.append("select valid_until from ");
            insertSQL.append(parameter.getSchema());
            insertSQL.append(".");
            insertSQL.append(this.geoobject_geometry);
            insertSQL.append(" where id = " + id + ";");
            System.out.println(insertSQL.toString());
            ResultSet resultSet = insertSQL.executeWithResult();
            resultSet.next();
            date = resultSet.getDate("valid_until");
            insertSQL.flushThreads();
            System.out.println("Date: " + date);
        }catch(SQLException e){
            e.printStackTrace();
        }
        return date;
    }

    long getIdByName(String name) throws SQLException {
        insertSQL.append("select ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject);
        insertSQL.append(".id and name = '");
        insertSQL.append(name);
        insertSQL.append("';");
        ResultSet resultSet = insertSQL.executeWithResult();
        resultSet.next();
        long geoobjGeomID08 = resultSet.getLong("geoobj_geom_id");
        insertSQL.flushThreads();
        return geoobjGeomID08;
    }
    private long getObjectIDByGeobjGeomID(long id) throws SQLException {
        insertSQL.append("select ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id_geoobject_source from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(" where id = " + id + ";");
        ResultSet resultSet = insertSQL.executeWithResult();
        resultSet.next();
        long geoobjID = resultSet.getLong("id_geoobj_source");
        insertSQL.flushThreads();
        return geoobjID;
    }
    private ArrayList<Date> getListOfDates(String name) throws SQLException {
        ArrayList<Date> date = new ArrayList<>();
        insertSQL.append("select valid_until from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(this.geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(this.geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(this.geoobject);
        insertSQL.append(".id and name = '" + name + "';");
        ResultSet resultSet = insertSQL.executeWithResult();
        while(resultSet.next()){
            date.add(resultSet.getDate("valid_until"));
            System.out.println("Date: " + date);
        }
        insertSQL.flushThreads();
        return date;
    }
    private ArrayList<Long> getListOfIds(String name) throws SQLException {
        ArrayList<Long> idList = new ArrayList<>();
        insertSQL.append("select ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(geoobject_geometry);
        insertSQL.append(".");
        insertSQL.append("id as geoobj_geom_id from ");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(this.geoobject_geometry);
        insertSQL.append(",");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(this.geoobject);
        insertSQL.append(" where id_geoobject_source=");
        insertSQL.append(parameter.getSchema());
        insertSQL.append(".");
        insertSQL.append(this.geoobject);
        insertSQL.append(".id and name = '" + name + "';");
        ResultSet resultSet = insertSQL.executeWithResult();
        while(resultSet.next()){
            idList.add(resultSet.getLong("geoobj_geom_id"));
        }
        insertSQL.flushThreads();
        return idList;
    }
}
