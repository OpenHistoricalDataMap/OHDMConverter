package osm2inter;

/**
 *
 * @author thsc
 */
public class OSMUpdateInter {
    
    public static void main(String[] args) {
        // import new osm file into a 'temporary intermediate (TIDB)' db
        // !! waynodes table is not necessary. !!
        // !! relationmember is. !!

        // mark all entities which are in IDB but not TIDB as deleted
        /*
        use osm_id to figure that out.
         */

        // remove waynodes / relationsmember entries for deleted entries
        // remove deleted entries in IDB
        
        // mark all changed entities in 'intermediate' (IDB) as changed
        /*
        changed (nodes, ways, relations) if:

        1) Object changed: if
            a) tags differ (probably object changed)
            b) classcode differs (probably object changed)
        2) geometry changed if
            long/lat changed

        set for all in which both changed: new

        additionally (ways, relations) if:
        2.2) geometry changed if
            node_ids differ
            -> generate waynodes / relationmember entries
         */

        // copy all changes from TIDB into IDB
        // remove changed entries in TIDB
        // remove all unchanged entities in TIDB
        
        // -> now: TIDB contains only new entities
        /*
        copy all new entities into IDB and mark as new
        update all in tidb as new
        insert into ?? (select from ??)
         */
        
        // now: all data in IDB and marked. Old data already removed

        // remove TIDB

        /*
        update ohdm can do:

        a) increase valid_since now for all entries which are not tagged as new or changed

        maybe make a more finegrained decision what tags are to be changed to define a
        new object (author?)

        b) add new objects (new and both changed, so above)

        c) update geometry

        d) update object
         */
    }
}
