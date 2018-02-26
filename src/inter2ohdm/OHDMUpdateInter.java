package inter2ohdm;

import java.io.IOException;
import java.sql.SQLException;
import util.Parameter;
import util.SQLStatementQueue;

/**
 * Created by thsc on 01.07.2017.
 * 
 * This class performs the update process.
 * It's assumed that two intermediate databases exists.
 * One contains a previous import (called old). The
 * other one contains a new import (called new).
 * 
 * At the end, all new and changed entities are moved
 * into old intermediate an updates and inserts are
 * triggered on OHDM database
 * 
 */
public class OHDMUpdateInter {
    private static final String TAG = "TODO";

    public static void main(String args[]) throws IOException, SQLException {
        if(args.length < 3) {
            System.err.println("three parameter file required: intermediate, update_intermediate, ohdm");
            System.exit(0);
        }

        String interDBParameterFile = args[0];
        String updateDBParameterFile = args[1];
        String ohdmParameterFile = args[2];

        Parameter interDBParameters = new Parameter(interDBParameterFile);
        Parameter updateDBParameters = new Parameter(updateDBParameterFile);
        Parameter ohdmParameter = new Parameter(ohdmParameterFile);

        // note: all must be in same database - most probably in different schemas
        String interDBName = interDBParameters.getdbName();

        if(!( interDBName.equalsIgnoreCase(updateDBParameters.getdbName()) &&
                interDBName.equalsIgnoreCase(ohdmParameter.getdbName())
            )) {
            System.err.println("intermediate, update and ohdm must be in same" +
                    "database (not necessarily same schema)");
            System.exit(0);

        }

        SQLStatementQueue sqlInterUpdate = new SQLStatementQueue(interDBParameters);

        SQLStatementQueue sqlOHDM = new SQLStatementQueue(ohdmParameter);

        String interSchema = interDBParameters.getSchema();
        String updateSchema = updateDBParameters.getSchema();

        try {
            // Start update process in intermediate

            /* Step 1: mark all entities in intermediate as valid
            which are unchanged.

            /* First: assume the opposite: whole world has changed
            and set all valid of all entities to false
             */

            System.out.println("set intermediate.nodes.valid to false (assume anything has changed");
            // NODES
            // update sample_osw.nodes set valid = false;
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes set valid = false");

            sqlInterUpdate.forceExecute();

            // WAYS
            // RELATIONS

            /*
            mark all unchanged entities as valid in intermediate
            unchanged: time stamps are identical in intermediate an update intermediate db

    update sample_osw.nodes set valid = true where sample_osw.nodes.osm_id IN
    (select nOld.osm_id from sample_osw.nodes as nOld, sample_osw_new.nodes as nNew where nOld.tstamp = nNew.tstamp AND nOld.osm_id = nNew.osm_id);
    */
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes set valid = true where ");
            sqlInterUpdate.append("osm_id IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("old.osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes as old, ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".nodes as new where old.tstamp = new.tstamp AND old.osm_id = new.osm_id)");

            sqlInterUpdate.forceExecute();
    /*
            no remove all entries from old new db

    delete from sample_osw_new.nodes where sample_osw_new.nodes.osm_id IN (select n.osm_id from sample_osw.nodes as n where n.valid);
            */

            /*
            Step 2:
            extend time in OHDM for those elements



            */

            /*
            Step 3:
            find entities which are in old but not new - those are deleted.
            Mark as deleted - and delete.

    update sample_osw.nodes set deleted = true where osm_id NOT IN (select osm_id from sample_osw_new.nodes);

           remove entries in waynodes and relationmember!!

            // remove
            delete from sample_osw.nodes where osm_id NOT IN (select osm_id from sample_osw_new.nodes);

            */

            /*
            Step 4:
            find changes in geometrie and/or objects - see document

            NODES
            +++++
            // find changes in geometry - marked with new tag
    update sample_osw_new.nodes set new = true
            where
            osm_id IN (select o.osm_id from sample_osw.nodes as o,
                sample_osw_new.nodes as n
                where o.osm_id = n.osm_id AND (o.longitude != n.longitude OR o.latitude != n.latitude));

            mark geometry changes to related ways!

            // find changes in object - marked with changed tag
    update sample_osw_new.nodes set changed = true where osm_id IN (select o.osm_id from sample_osw.nodes as o, sample_osw_new.nodes as n
    where o.osm_id = n.osm_id AND o.serializedtags != n.serializedtags);

            WAYS
            ++++
            // find changes in geometry - marked with new tag
    TODO

            mark relationed relations as changed (new)

            // find changes in object - marked with changed tag
    update sample_osw_new.ways set changed = true where osm_id IN (select o.osm_id from sample_osw.ways as o, sample_osw_new.ways as n
    where o.osm_id = n.osm_id AND o.serializedtags != n.serializedtags);

            RELATIONS
            +++++++++
            // find changes in geometry - marked with new tag
    TODO

            // find changes in object - marked with changed tag
    update sample_osw_new.relations set changed = true where osm_id IN (select o.osm_id from sample_osw.relations as o, sample_osw_new.relations as n
    where o.osm_id = n.osm_id AND o.serializedtags != n.serializedtags);


            */

            /*
            Step 5:
            handle changed entities - see document
            */
        }
        catch(SQLException se) {
            System.err.println("failure while executing sql statement: " + se.getMessage());

            System.err.println("SQL Inter/Update Queue:");
            System.err.println(sqlInterUpdate.getCurrentStatement());

            System.err.println("SQL OHDM Queue:");
            System.err.println(sqlOHDM.getCurrentStatement());
        }
    }
}
