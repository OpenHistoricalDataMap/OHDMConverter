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
    private static final String GEOMETRY_CHANGED_TAG = "new";
    private static final String OBJECT_CHANGED_TAG = "changed";

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                                   setup.. reset all flags                                                  //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            System.out.print("reset flags in intermediate.nodes");
            // NODES
            // update sample_osw.nodes set valid = false;
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes set valid = false");
            sqlInterUpdate.append(", deleted = false, ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" = false, ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append(" = false");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");


            // WAYS
            System.out.print("reset flags in intermediate.ways");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways set valid = false");
            sqlInterUpdate.append(", deleted = false, ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" = false, ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append(" = false");


            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // RELATIONS
            System.out.print("reset flags in intermediate.relations");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations set valid = false");
            sqlInterUpdate.append(", deleted = false, ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" = false, ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append(" = false");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            System.out.println("now start marking the valid entities");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                       mark tags as still valid in intermediate / remove from update db                     //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        /*
        mark all unchanged entities as valid in intermediate
            unchanged: time stamps are identical in intermediate an update intermediate db

            update sample_osw.nodes set valid = true where sample_osw.nodes.osm_id IN
            (select nOld.osm_id from sample_osw.nodes as nOld, sample_osw_new.nodes as nNew where nOld.tstamp = nNew.tstamp AND nOld.osm_id = nNew.osm_id);
        */
            System.out.print("set valid flag for unchanged nodes");
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
            System.out.println("...ok");

            // WAYS
            System.out.print("set valid flag for unchanged ways");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways set valid = true where ");
            sqlInterUpdate.append("osm_id IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("old.osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways as old, ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".ways as new where old.tstamp = new.tstamp AND old.osm_id = new.osm_id)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // RELATIONS
            System.out.print("set valid flag for unchanged relations");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations set valid = true where ");
            sqlInterUpdate.append("osm_id IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("old.osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations as old, ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".relations as new where old.tstamp = new.tstamp AND old.osm_id = new.osm_id)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");


    /* remove all valid (unchanged entities from update db) not required any longer in update db
        delete from sample_osw_new.nodes where sample_osw_new.nodes.osm_id IN (select n.osm_id from sample_osw.nodes as n where n.valid);
    */
            System.out.print("remove unchanged nodes from update db");
            sqlInterUpdate.append("delete from ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".nodes where ");
            sqlInterUpdate.append("osm_id IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes");
            sqlInterUpdate.append(" where valid = true)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // WAYS
            System.out.print("remove unchanged ways from update db");
            sqlInterUpdate.append("delete from ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".ways where ");
            sqlInterUpdate.append("osm_id IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways");
            sqlInterUpdate.append(" where valid = true)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // RELATIONS
            System.out.print("remove unchanged relations from update db");
            sqlInterUpdate.append("delete from ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".relations where ");
            sqlInterUpdate.append("osm_id IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations");
            sqlInterUpdate.append(" where valid = true)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                       mark nodes as deleted in intermediate an delete                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            /*
            Step 3:
            find entities which are in intermediate tables but not in update table they were deleted.
            a) Mark as deleted
            b) remove related entities - mark them as deleted as well
            c) delete.
            */

            // NODES
    //update sample_osw.nodes set deleted = true where osm_id NOT IN (select osm_id from sample_osw_new.nodes);
            System.out.print("mark deleted nodes as deleted in intermediate");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes set deleted = true where ");
            sqlInterUpdate.append("osm_id NOT IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("osm_id from ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".nodes)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // remove nodes from waynodes
            System.out.print("delete lines from waynodes table with removed nodes");
            sqlInterUpdate.append("delete from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".waynodes where node_id IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes where deleted = true)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // remove nodes from relationsmember
            System.out.print("delete lines from relationmember table with removed nodes");
            sqlInterUpdate.append("delete from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relationmember where node_id IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes where deleted = true)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // now remove deleted nodes from nodes
            System.out.print("delete nodes from nodes table");
            sqlInterUpdate.append("delete from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes where deleted = true");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // WAYS
            System.out.print("mark deleted ways as deleted in intermediate");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways set deleted = true where ");
            sqlInterUpdate.append("osm_id NOT IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("osm_id from ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".ways)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // remove nodes from relationsmember
            System.out.print("delete lines from relationmember table with removed ways");
            sqlInterUpdate.append("delete from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relationmember where way_id IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways where deleted = true)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // now remove deleted ways
            System.out.print("delete ways from ways table");
            sqlInterUpdate.append("delete from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways where deleted = true");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // RELATIONS
            System.out.print("mark deleted relations as deleted in intermediate");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations set deleted = true where ");
            sqlInterUpdate.append("osm_id NOT IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("osm_id from ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".relations)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // remove nodes from relationsmember
            System.out.print("delete lines from relationmember table with removed relations");
            sqlInterUpdate.append("delete from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relationmember where way_id IN (");
            sqlInterUpdate.append("select ");
            sqlInterUpdate.append("osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations where deleted = true)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // now remove deleted relations
            System.out.print("delete relations from relations table");
            sqlInterUpdate.append("delete from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations where deleted = true");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                       mark changes in geometries and/or objects in entities                                //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            // NODES - geometry change
            /*
            update sample_osw_new.nodes set new = true where
            osm_id IN (select o.osm_id from sample_osw.nodes as o,
                sample_osw_new.nodes as n
                where o.osm_id = n.osm_id AND (o.longitude != n.longitude OR o.latitude != n.latitude));

             */
            System.out.print("mark nodes in intermediate which geometries has changed by setting flag " + OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes set ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" = true where osm_id IN (");
            sqlInterUpdate.append("select old.osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes as old, ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".nodes as new where old.osm_id = new.osm_id AND (old.longitude != new.longitude OR old.latitude != new.latitude))");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // WAYS
            System.out.print("mark ways in intermediate which geometries has changed by setting flag " + OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways set ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" = true where osm_id IN (");
            sqlInterUpdate.append("select old.osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways as old, ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".ways as new where old.osm_id = new.osm_id AND old.node_ids != new.node_ids)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // RELATIONS
            System.out.print("mark relations in intermediate which geometries has changed by setting flag " + OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations set ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" = true where osm_id IN (");
            sqlInterUpdate.append("select old.osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations as old, ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".relations as new where old.osm_id = new.osm_id AND old.member_ids != new.member_ids)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // NODES - object changes
            /*

            // find changes in object - marked with changed tag
            update sample_osw_new.nodes set changed = true where osm_id IN (select o.osm_id from sample_osw.nodes as o,
            sample_osw_new.nodes as n
            where o.osm_id = n.osm_id AND o.serializedtags != n.serializedtags);
             */
            System.out.print("mark nodes in intermediate which objects has changed by setting flag " + OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes set ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append(" = true where osm_id IN (");
            sqlInterUpdate.append("select old.osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes as old, ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".nodes as new where old.osm_id = new.osm_id AND old.serializedtags != new.serializedtags)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // WAYS
            System.out.print("mark ways in intermediate which objects has changed by setting flag " + OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways set ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append(" = true where osm_id IN (");
            sqlInterUpdate.append("select old.osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways as old, ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".ways as new where old.osm_id = new.osm_id AND old.serializedtags != new.serializedtags)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // RELATIONS
            System.out.print("mark relations in intermediate which objects has changed by setting flag " + OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations set ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append(" = true where osm_id IN (");
            sqlInterUpdate.append("select old.osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations as old, ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".relations as new where old.osm_id = new.osm_id AND old.serializedtags != new.serializedtags)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                       extend validity of unchanged entities in ohdm                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            /*
            Step 2:
            extend time in OHDM for those elements
            TODO
            */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         import changes into OHDM                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
