package inter2ohdm;

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
        super(intermediateDB, osmIDString, classCodeString, null, sTags, ohdmObjectIDString, ohdmGeomIDString, false, valid);
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
    
    boolean fillRelatedGeometries(ArrayList<String> polygonIDs, ArrayList<String> polygonWKT) {
        // now... we are going to construct a wkt out of OSM multipolygon... good luck :/
        
        // create a polygon with hole
        // POLYGON ((10 10, 110 10, 110 110, 10 110), (20 20, 20 30, 30 30, 30 20), (40 20, 40 30, 50 30, 50 20))

//        if(this.getOSMIDString().equalsIgnoreCase("3323433")) {
//            int debuggingStop = 42;
//        }
        
        try {
            StringBuilder wktBuilder = null;
            OHDMWay way = null;
            OHDMWay next = (OHDMWay) this.members.get(0);
            this.intermediateDB.addNodes2OHDMWay(next); // fill way with nodes
            
            int i = 0;
            boolean wayOutside;
            boolean nextOutside = true;
            OHDMNode firstNode = null;
            
            boolean lastLoop = false;
            
            while(!lastLoop) {
                // shift
                way = next; // shift
                wayOutside = nextOutside;

                // luck ahead if possible
                if(++i < memberRoles.size()) {
                    next = (OHDMWay) members.get(i);
                    this.intermediateDB.addNodes2OHDMWay(next);
                    nextOutside = this.memberRoles.get(i).equalsIgnoreCase(OHDMRelation.OUTER_ROLE);
                } else {
                    // no more elements in queue - process final one
                    lastLoop = true;
                    nextOutside = true;
                }
                
                if(wayOutside) {
                    // just a sequence of a previous non polygon way?
                    if(wktBuilder != null) {
                        // there a previous points
                        wktBuilder.append(", ");
                        if(this.addWayToPolygon(firstNode, wktBuilder, way)) {
                            /* added and polygon is finished
                             is there a hole?
                            */
                            firstNode = null; // remember.. polygon is closed
                            
                            if(nextOutside) {
                                // no hole
                                polygonIDs.add("-1");
                                polygonWKT.add(wktBuilder.toString());
                                wktBuilder = null;
                            } else {
                                // there is a hole.. start hole wkt
                                wktBuilder.append(", (");
                            }
                            if(lastLoop) break; // done here
                        }
                    } else {
                        // no previous ways in wkt
                        if(way.isPolygon()) {
                            // is a polygon by itself.. has it a hole ?
                            if(nextOutside || lastLoop) { 
                                /* we are done here. It is a polygon followed 
                                by another one outside with no previous polygons
                                */
                                polygonIDs.add(way.getOHDMObjectID());
                                polygonWKT.add("");
                            } else {
                                /* one or more holes are following
                                create a new wkt and outside shape into wkt
                                */
                                wktBuilder = new StringBuilder();
                                wktBuilder.append("(");
                                firstNode = null; // mark that polygon is already closed as it comes
                                this.addWayToPolygon(firstNode, wktBuilder, way);
                                wktBuilder.append(") "); 
                                // following inner polygons are added here
                            }
                        } else {
                            /* no polygon but first way outside
                               start new polygon
                            */
                            if(lastLoop) {
                                // unfinished polygon
                                this.failed(wktBuilder, "2");
                            }
                            
                            wktBuilder = new StringBuilder();
                            wktBuilder.append("(");
                            // remember first node of this not yet closed polygon
                            firstNode = way.getNodeIter().next();
                            this.addWayToPolygon(firstNode, wktBuilder, way);
                        }
                    }
                } else {
                    // inner polygons
                    // in any case.. a wkt string builder exist
                    if(firstNode != null) {
                        // this polygon is a sequel.. add it
                        wktBuilder.append(", ");
                        if(this.addWayToPolygon(firstNode, wktBuilder, way)) {
                            // polygon closed
                            firstNode = null;
                            
                            if(nextOutside || lastLoop) {
                                // whole polygon done
                                polygonIDs.add("-1");
                                polygonWKT.add(wktBuilder.toString());
                                wktBuilder = null;
                            } // else // start next hole.. nothing todo
                        }
                    } else {
                        // first node is null.. this way at least starts another hole
                        if(way.isPolygon()) {
                            wktBuilder.append(", ( ");
                            this.addWayToPolygon(null, wktBuilder, way);
                            wktBuilder.append(") ");
                        } else {
                            if(lastLoop) { // cannot open a new polygon
                                this.failed(wktBuilder, "3");
                            }
                            // start polygon inside
                            wktBuilder.append(", ( ");
                            firstNode = way.getNodeIter().next();
                            this.addWayToPolygon(firstNode, wktBuilder, way);
                        }
                    }
                }
            }
            
            if(firstNode != null) {
                this.failed(wktBuilder, "polygon not closed");
            }
            
            if(wktBuilder != null && wktBuilder.length() > 0) {
                // save final polygon.. it is a polygon with hole
                polygonIDs.add("-1");
                polygonWKT.add("POLYGON(" + wktBuilder.toString() + ")");
            }
        } catch (SQLException ex) {
            return false;
        }
        
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("osmid: " + this.getOSMIDString());
        for(int i=0; i<polygonIDs.size();i++) {
            System.out.println("id: " + polygonIDs.get(i));
            System.out.println("wktstring:\n" + polygonWKT.get(i));
        }
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        
        return true;
    }
    
    private void failed(StringBuilder wktBuilder, String s) throws SQLException {
        String w = "";
        if(wktBuilder != null) {
            w = wktBuilder.toString();
        }
        String s2 = "malformed polygon ( " + s + " ): " + this.getOSMIDString() + "\n" + w;
        System.err.println(s2);
        throw new SQLException(s2);
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
        
        if(!this.memberRoles.get(0).equalsIgnoreCase(OHDMRelation.OUTER_ROLE)) {
            System.err.println("multipolygon starts not with outside role");
            return null; // must start outside
        }
        
        return null;
        
    }
    
    private boolean addWayToPolygon(OHDMNode firstNode, StringBuilder wkt, OHDMWay way) throws SQLException {
        // append all way points to wkt
        wkt.append(way.getWKTPointsOnly());
        
        // get last point
        OHDMNode lastWayNode = way.getLastPoint();
        
        if(lastWayNode != null && lastWayNode.identical(firstNode)) {
            // polygon finished
            wkt.append(") ");
            return true;
        }
        
        // not closed
        return false;
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
            // setup position list
            this.memberIDList = this.setupIDList(this.memberIDs);
            
            // setup other lists with same size
            this.memberRoles = new ArrayList<>(this.memberIDList.size());
            this.members = new ArrayList<>(this.memberIDList.size());
            
            // dummys must be added..
            for(int i = 0; i < this.memberIDList.size(); i++) {
                this.members.add(null);
                this.memberRoles.add(null);
            }
        }

        int position = this.addMember(element, this.members, this.memberIDList, false);

        // remember role of this member
        this.memberRoles.set(position, roleName);
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
                break;
            }
        }
        
        this.polygonChecked = true;
        return this.isPolygon;
    }
    
    private boolean isMultipolygon = false;
    private boolean isMultipolygonChecked = false;
    
    boolean isMultipolygon() {
        if(!this.isMultipolygonChecked) {
            this.isMultipolygonChecked = true;
            
            if(!this.isPolygon()) { // no polygon at all .. false
                this.isMultipolygon = false;
            }
            
            String relationType = this.getType();
            if(relationType != null && relationType.equalsIgnoreCase("multipolygon")) {
                this.isMultipolygon = true;
            }
        }
        
        return this.isMultipolygon;
    }

    /** 
        if a multipolygon relation has only two member, inner and outer,
        bring them into right order.
    */
    boolean checkMultipolygonMemberOrder() {
        if(!this.isMultipolygon) return false;
        
        // only two members?
        if(this.memberRoles.size() != 2) return true; // hope the best
        
        // one is inner, one is outer
        
        // if first is outer.. ok, next can be outer or inner both ok
        if(this.memberRoles.get(0).equalsIgnoreCase("outer")) return true;
        
        // first is inner.. second?
        if(this.memberRoles.get(1).equalsIgnoreCase("inner")) {
            // after an inner member comes another one.. that not an osm polygon
            this.isMultipolygon = false;
            this.isMultipolygonChecked = true;
            return false;
        } 
        
        // if only two member.. I can fix it
        if(this.memberRoles.size() != 2) { // more than two
            System.err.println("malformed multipolygon: starts with inner member and has more than two member at all: " + this.getOSMIDString());
            return false;
        } 
        
        // fix it by switching member positions
        String firstRole = this.memberRoles.get(0);
        this.memberRoles.set(0, this.memberRoles.get(1));
        this.memberRoles.set(1, firstRole);
        
        OHDMElement firstMember = this.members.get(0);
        this.members.set(0, this.members.get(1));
        this.members.set(1, firstMember);
        
        return true;
    }
}
