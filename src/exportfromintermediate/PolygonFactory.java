package exportfromintermediate;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author thsc
 */
public class PolygonFactory {
    private final Connection target;
    
    ArrayList<String> wktList = new ArrayList<>();
    
    PolygonFactory(Connection target) {
        this.target = target;
    }
    
    /**
     * add an outer way that is *no* polygon
     * @param w 
     */
    void addOuterWay(OHDMWay w) {
        
    }
    
    /**
     * add an inner way that is *no* polygon
     * @param w 
     */
    void addInnerWay(OHDMWay w) {
        
    }
    
    /**
     * add an outer way that is a polygon and which is followed
     * by an inner way (polygon or not)
     * @param w 
     */
    void addOuterPolygon(OHDMWay p) {
        
    }
    
    void addInnerPolygon(OHDMWay p) {
        
    }
    
    Iterator<String> getPolygonWKT() {
        return this.wktList.iterator();
    }
}
