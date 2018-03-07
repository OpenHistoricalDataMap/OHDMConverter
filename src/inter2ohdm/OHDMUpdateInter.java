package inter2ohdm;

import java.io.IOException;
import java.sql.ResultSet;
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
    private static final String OBJECT_NEW_TAG = "has_name";

    public static void main(String args[]) throws IOException, SQLException {
        if(args.length < 4) {
            System.err.println("parameter required: " +
                    "intermediate, " +
                    "update_intermediate, " +
                    "ohdm" +
                    "update-date-string (yyyy-mm-dd)");
            System.exit(0);
        }

        String interDBParameterFile = args[0];
        String updateDBParameterFile = args[1];
        String ohdmParameterFile = args[2];
        String updateDateString = args[3];

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

            // Status: intermediate flags reset, update unchanged

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                       mark tags as still valid in intermediate / remove from update db                     //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        /*
        mark all unchanged entities as valid in intermediate
            unchanged: time stamps are identical in intermediate an update intermediate db

            update sample_osw.nodes set valid = true where sample_osw.nodes.osm_id IN
            (select nOld.osm_id from sample_osw.nodes as nOld, sample_osw_new.nodes as nNew where nOld.tstamp = nNew.tstamp AND nOld.osm_id = nNew.osm_id);
        */
            System.out.print("set valid flag in intermediate for unchanged nodes");
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
            System.out.print("set valid flag intermediate for unchanged ways");
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
            System.out.print("set valid flag intermediate for unchanged relations");
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

            // status: intermediate: valid tags set for unchanged entries, update: unchanged entries removed

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                       mark nodes as deleted in intermediate                                                //
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

            // intermediate: entities marked as deleted, update: no change

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
            System.out.print("mark nodes in intermediate with changed geometry. Flag: " + OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
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

            // IS IT REALLY NECESSARY???
            // mark way geometrie changed if related node has changed geometry or was deleted

            /*
update intermediate.ways set new = true where osm_id IN
(
select wn.way_id from
(select osm_id, deleted, new from intermediate.nodes) as n,
(select  way_id, node_id from intermediate.waynodes) as wn
where (n.deleted OR n.new) AND n.osm_id = wn.node_id)
*/
            System.out.print("mark geometry change in intermediate ways as result of geometry change (or deletion) of related node. Flag: " + OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways set ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" = true where osm_id IN (");
            sqlInterUpdate.append("select wn.way_id from ");
            sqlInterUpdate.append("(select osm_id, deleted, " );
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes) as n,");
            sqlInterUpdate.append("(select  way_id, node_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".waynodes) as wn");
            sqlInterUpdate.append(" where (n.deleted OR n.");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(") AND n.osm_id = wn.node_id)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            System.out.print("mark geometry change in intermediate relations as result of geometry change (or deletion) of related node. Flag: " + OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations set ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" = true where osm_id IN (");
            sqlInterUpdate.append("select rm.relation_id from ");
            sqlInterUpdate.append("(select osm_id, deleted, " );
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes) as n,");
            sqlInterUpdate.append("(select  relation_id, node_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relationmember) as rm");
            sqlInterUpdate.append(" where (n.deleted OR n.");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(") AND n.osm_id = rm.node_id)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // WAYS
            System.out.print("mark ways in intermediate with changed geometry. Flag: " + OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
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

            // mark indirectly changed relations
            System.out.print("mark geometry change in relations as result of geometry change (or deletion) of related way. Flag: " + OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations set ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" = true where osm_id IN (");
            sqlInterUpdate.append("select rm.relation_id from ");
            sqlInterUpdate.append("(select osm_id, deleted, " );
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways) as w,");
            sqlInterUpdate.append("(select  relation_id, way_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relationmember) as rm");
            sqlInterUpdate.append(" where (w.deleted OR w.");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(") AND w.osm_id = rm.way_id)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // RELATIONS
            System.out.print("mark relations in intermediate with changed geometry. Flag: " + OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
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

            // mark indirectly changed relations
            System.out.print("mark geometry change in relations as result of geometry change (or deletion) of related relation. Flag: " + OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations set ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" = true where osm_id IN (");
            sqlInterUpdate.append("select rm.relation_id from ");
            sqlInterUpdate.append("(select osm_id, deleted, " );
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(" from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations) as r,");
            sqlInterUpdate.append("(select  relation_id, member_rel_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relationmember) as rm");
            sqlInterUpdate.append(" where (r.deleted OR r.");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(") AND r.osm_id = rm.member_rel_id)");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // status: intermediate: changed geometries marked, update: no change


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                       remove entities which are marked as deleted                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            ////////////// remove deleted nodes now

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

            //////////////////// remove deleted ways now

            // remove ways from relationsmember
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

            //////////////// remove deleted relations now

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

            // status: intermediate: entries marked as deleted are deleted, update: unchanged

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                              mark entities with changed object                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

            // status: intermediate: changed objects marked, update: unchanged

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                       extend validity of unchanged entities in ohdm                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            /**
             * Hints:
             * ohdm object id start with 0 (dummy object for unnamned entities)
             * ohdm geometry ids start with 1
             * ohdm_geom_type: 0 marks relation, 1 point, 2 ways, 3 polygon
             *
             *
             * Idea:
             nodes, ways and relations are source for objects and geometries in OHDM.
             object_geom table in OHDM links both together.
             Those links are still valid if both side are valid regardless their source.
             Thus, we need two independent data streams: on for valid object ids
             another for valid geometry id / type pair.

             All matching pairs are to be updated with an extended validity date.

             Here it is: TODO: activate that code.

             update ohdmupdatetest.geoobject_geometry set valid_until = '2020-02-02'
             where id IN
             (select gg.id from
             (SELECT ohdm_object_id FROM intermediate.nodes where valid OR NOT changed
             UNION
             SELECT ohdm_object_id FROM intermediate.ways where valid OR NOT changed
             UNION
             SELECT ohdm_object_id FROM intermediate.relations where valid OR NOT changed
             ) as o,

             (SELECT ohdm_geom_id, ohdm_geom_type FROM intermediate.nodes
             where (valid OR NOT new) AND ohdm_geom_id >= 1 AND ohdm_geom_type >= 1
             UNION
             SELECT ohdm_geom_id, ohdm_geom_type FROM intermediate.ways
             where (valid OR NOT new) AND ohdm_geom_id >= 1 AND ohdm_geom_type >= 1
             UNION
             SELECT ohdm_geom_id, ohdm_geom_type FROM intermediate.relations
             where (valid OR NOT new) AND ohdm_geom_id >= 1 AND ohdm_geom_type >= 1) as g,

             (SELECT id, id_target, id_geoobject_source, type_target from
             ohdmupdatetest.geoobject_geometry) as gg

             where gg.id_geoobject_source = o.ohdm_object_id
             AND gg.id_target = g.ohdm_geom_id AND gg.type_target = ohdm_geom_type)

             *
             *
             *
             */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                       distinguish new and changed entities in update db                                    //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            // set new tag in update to true:

            // NODES
            System.out.print("reset new flag in update.nodes");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".nodes set ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_NEW_TAG);
            sqlInterUpdate.append(" = true");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // WAYS
            System.out.print("reset new flag in update.ways");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".ways set ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_NEW_TAG);
            sqlInterUpdate.append(" = true");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // RELATIONS
            System.out.print("reset new flag in update.relations");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".relations set ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_NEW_TAG);
            sqlInterUpdate.append(" = true");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // set new = false for all changed but not new entities
            /* update updateintermediate.nodes set has_name = false
            where osm_id NOT IN
            (select osm_id from intermediate.nodes where valid OR has_name)
             */

            // Nodes
            System.out.print("set new flag to false for valid or changed but not new nodes");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".nodes set ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_NEW_TAG);
            sqlInterUpdate.append(" = false where osm_id IN (select osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".nodes where valid OR ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append(" OR ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(")");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // Ways
            System.out.print("set new flag to false for valid or changed but not new ways");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".ways set ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_NEW_TAG);
            sqlInterUpdate.append(" = false where osm_id NOT IN (select osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".ways where valid OR ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append(" OR ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(")");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // Relations
            System.out.print("set new flag to false for valid or changed but not new relations");
            sqlInterUpdate.append("update ");
            sqlInterUpdate.append(updateSchema);
            sqlInterUpdate.append(".relations set ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_NEW_TAG);
            sqlInterUpdate.append(" = false where osm_id NOT IN (select osm_id from ");
            sqlInterUpdate.append(interSchema);
            sqlInterUpdate.append(".relations where valid OR ");
            sqlInterUpdate.append(OHDMUpdateInter.OBJECT_CHANGED_TAG);
            sqlInterUpdate.append(" OR ");
            sqlInterUpdate.append(OHDMUpdateInter.GEOMETRY_CHANGED_TAG);
            sqlInterUpdate.append(")");

            sqlInterUpdate.forceExecute();
            System.out.println("...ok");

            // now: new tags are tagged as new in update db

            /*
             * Situation: Intermediate DB is tagged.
             * valid: entity hasn't changed (regarding timestamp)
             * geometry changed - geometry was directly or indirectly changed (and so id list in ways and relations)
             * object changed and so serializedAttributes
             *
             * update db
             * contains entries which are
             * changed since last update or
             * new
             */

            // update intermediate entries with changed objects

            /*
             * simple: update serialized tags, classcodes and tstamp
            UPDATE
                intermediate.ways
            SET
                serializedtags = uw.serializedtags,
                classcode = uw.classcode,
                otherclasscodes = uw.otherclasscodes,
                tstamp = uw.tstamp
            FROM
                intermediate.ways AS iw
                INNER JOIN updateintermediate.ways AS uw
                    ON iw.osm_id = uw.osm_id
            WHERE
                intermediate.ways.changed

            TODO: nodes + relations (same)
             */

            // copy new id list to intermediate for entities with changed geometry
/*
            ways:

            UPDATE
            intermediate.ways
                    SET
            node_ids = uw.node_ids,
                    tstamp = uw.tstamp
            FROM
            intermediate.ways AS iw
            INNER JOIN updateintermediate.ways AS uw
            ON iw.osm_id = uw.osm_id
            WHERE
            intermediate.ways.new

            nodes:
            UPDATE
                intermediate.nodes
            SET
                longitude = unodes.longitude,
                latitude = unodes.latitude,
                tstamp = unodes.tstamp
            FROM
                intermediate.nodes AS inodes
                INNER JOIN updateintermediate.nodes AS unodes
                    ON inodes.osm_id = unodes.osm_id
            WHERE
                inodes.new

                TODO relations
*/
        // TODO: copy all related waynodes and relationmember
            // duplicate supression

            /*
            Step 2:
            extend time in OHDM for those elements
            TODO
            unchanged means: valid AND same geometry AND same object
            */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         import changes into OHDM                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        /*
            Note: An initial import set the until coloumn to a default.
            With that update the until becomes clearer: It is today. We should
            set the default until to today! TODO
        */

        /* Situation: Intermediate is filled even with new data. Changed lines
           are tagged in nodes, ways and relations

           for new = true OR (geometry changed AND object changed)
           ->new import
            
           object-changed->import object (change OHDM object and obj_geom entry)
           geometry changed->import_geometry (change geom id and obj_geom entry)
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
