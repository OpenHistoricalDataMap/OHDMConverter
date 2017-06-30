package osm2inter;

/**
 *
 * @author thsc
 */
public class OSMUpdateInter {
    
    public static void main(String[] args) {
        // import new osm file into a 'temporary intermediate (TIDB)' db
        
        // mark all changed entities in 'intermediate' (IDB) as changed
        
        // remove all unchanged entities in TIDB
        
        // -> now: TIDB contains new and changed entities
        
        // mark all new entities in TIB as new (osm_id not in IDB)
        // now: changed and new can be distinguished
        
        // mark all entities in IDB as deleted which are not in TIDB
        
        /* -> now:
        We have identified: new, changed and deleted
        We haven't yet identified changed relations nodes->ways->relations
        (only if geometry has changed, not tags) ???
        */
    }
}
