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

    public static void main(String args[]) throws IOException, SQLException {
        if(args.length < 2) {
            System.err.println("two parameters required: [parameter file old intermediate db] [from new db]");
            System.exit(0);
        }
        
        String interDBParameterFile = args[0];
        String ohdmParameterFile = args[1];
        
        Parameter interDBParameters = new Parameter(interDBParameterFile);
        SQLStatementQueue sqlInter = new SQLStatementQueue(interDBParameters);
        
        Parameter ohdmParameter = new Parameter(ohdmParameterFile);
        SQLStatementQueue sqlOHDM = new SQLStatementQueue(ohdmParameter);
        
        /*

        Step 1
        ++++++
        mark all unchanged entities
        
        set valid flag to false - standard
update sample_osw.nodes set valid = false;

        mark all entities which are unchanged as valid
        
update sample_osw.nodes set valid = true where sample_osw.nodes.osm_id IN 
(select nOld.osm_id from sample_osw.nodes as nOld, sample_osw_new.nodes as nNew where nOld.tstamp = nNew.tstamp AND nOld.osm_id = nNew.osm_id);
        
        no remove all entries from old new db
        
delete from sample_osw_new.nodes where sample_osw_new.nodes.osm_id IN (select n.osm_id from sample_osw.nodes as n where n.valid);
        */
        
        /*
        Step 2: 
        enhance time in OHDM for those elements
        
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
}
