package exportfromintermediate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author thsc
 */
public class OHDMRelation extends OHDMElement {
    private final String memberIDs;
    private ArrayList<OHDMElement> members;
    private ArrayList<String> memberRoles;
    private ArrayList<String> memberIDList;
    private ArrayList<String> roleMemberIDList;
    
    OHDMRelation(IntermediateDB intermediateDB, String osmIDString, String classCodeString, String sTags, String memberIDs, String ohdmObjectIDString, String ohdmGeomIDString, boolean valid) {
        super(intermediateDB, osmIDString, classCodeString, null, sTags, ohdmObjectIDString, ohdmGeomIDString, valid);
        this.memberIDs = memberIDs;
    }
    
    OHDMElement getMember(int i) {
        return this.members.get(i);
    }
    
    String getRoleName(int i) {
        return this.memberRoles.get(i);
    }
    
    int getMemberSize() {
        return this.members.size();
    }

    private String wkt = null;
    /**
     * See also http://wiki.openstreetmap.org/wiki/Relation:multipolygon
     * type : multipolygon
     * @return 
     */
    @Override
    String getWKTGeometry() {
        // already created wkt?
        if(this.wkt != null) return this.wkt;
        
        if(!this.isPolygon()) return null;
        
        // we only create geometries out of mulitpolygons
        if(!this.getType().equalsIgnoreCase("multipolygon")) return null;
        
        // now... we are going to construct a wkt out of OSM multipolygon... good luck :/
        
        // create a polygon with hole
        // POLYGON ((10 10, 110 10, 110 110, 10 110), (20 20, 20 30, 30 30, 30 20), (40 20, 40 30, 50 30, 50 20))

        // following code don't work.. return until it's fixed
        if(this.isPolygon()) return null;
        
        ArrayList<OHDMWay> outerWays = new ArrayList<>();
        ArrayList<OHDMWay> innerWays = new ArrayList<>();
        // iterate member
        
        ArrayList<String> polygonIDs = new ArrayList<>();
        
        try {
            int i = 0;
            while(i < memberRoles.size()) {
                OHDMElement way = members.get(i);
                
                // fill nodes
                this.intermediateDB.addNodes2OHDMWay((OHDMWay)way);
                
                // now we have a complete way
                
                
                if(memberRoles.get(i).equalsIgnoreCase(OHDMRelation.INNER_ROLE)) {
                    innerWays.add((OHDMWay)way);
                } else {
                    outerWays.add((OHDMWay)way);
                }
            }
            
            // create wkt
            StringBuilder sb = new StringBuilder();
            sb.append("POLYGON ( (");
            
            this.addPoints(outerWays, sb);
            
            Iterator<OHDMWay> innerWaysIter = innerWays.iterator();
            while(innerWaysIter.hasNext()) {
                sb.append(","); // separate between ring(s)
                OHDMWay innerWay = innerWaysIter.next();
                if(innerWay.isPolygon()) {
                    sb.append(innerWay.getWKTPointsOnly());
                } else { // should not happen.. remove ','
                    System.err.println("inner ring is no polygon, osmid: " + innerWay.getOSMIDString());
                    sb.deleteCharAt(sb.length() - 1);
                }
            }
            
            sb.append(")"); // close polygon

            this.wkt = sb.toString();
            
//            System.err.println(this.wkt);
            
        } catch (SQLException ex) {
            // 
        }
        
        return this.wkt;
    }
    
    private void addPoints(ArrayList<OHDMWay> wayList, StringBuilder sb) {
        Iterator<OHDMWay> wayIter = wayList.iterator();
        boolean first = true;
        while(wayIter.hasNext()) {
            OHDMWay way = wayIter.next();
            if(way.isPolygon()) {
                if(first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append(way.getWKTPointsOnly());
            } else { // should not happen.. remove ','
                System.err.println("ring is no polygon, osmid: " + way.getOSMIDString());
                sb.deleteCharAt(sb.length() - 1);
            }
        }
    }

    @Override
    GeometryType getGeometryType() {
        return GeometryType.POLYGON;
    }
    
    void addMember(OHDMElement element, String roleName) {
        if (this.members == null) {
            this.members = new ArrayList<>();
            
            // setup position list
            this.memberIDList = this.setupIDList(this.memberIDs);
            this.memberRoles = new ArrayList<>();
        }

        int position = this.addMember(element, this.members, this.memberIDList);
        
        if(position > this.memberRoles.size() -1) {
            this.memberRoles.add(roleName);
        } else {
            this.memberRoles.add(position, roleName);
        }
    }
    
    public static final String INNER_ROLE = "inner";
    public static final String OUTER_ROLE = "outer";
    
    private boolean polygonChecked = false;
    
    @Override
    boolean isPolygon() {
        if(this.polygonChecked) return this.isPolygon;
        
        this.isPolygon = true;
        
        // check roles
        for (String roleName : this.memberRoles) {
            if( !roleName.equalsIgnoreCase(OHDMRelation.INNER_ROLE)
                    && !roleName.equalsIgnoreCase(OHDMRelation.OUTER_ROLE)
            ) {
                this.isPolygon = false;
            }
        }
        
        this.polygonChecked = true;
        return this.isPolygon;
    }
}
