package inter2ohdm;

import java.io.PrintStream;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import util.OHDM_DB;

/**
 *
 * @author thsc
 */
public class OSMRelation extends OSMElement {
    private final String memberIDs;
    private ArrayList<OSMElement> members;
    private ArrayList<String> memberRoles;
    private ArrayList<String> memberIDList;
    private ArrayList<String> roleMemberIDList;
    
    OSMRelation(IntermediateDB intermediateDB, String osmIDString, 
            String classCodeString, String otherClassCodes, String sTags, String memberIDs, 
            String ohdmObjectIDString, String ohdmGeomIDString, 
            boolean valid,
            boolean isNew, boolean changed, boolean deleted,
            boolean has_name, Date tstampDate) {
        
        super(intermediateDB, osmIDString, classCodeString, otherClassCodes, sTags, 
                ohdmObjectIDString, ohdmGeomIDString, valid,
                isNew, changed, deleted, has_name, tstampDate);
        this.memberIDs = memberIDs;
    }
    
    OSMElement getMember(int i) {
        return this.members.get(i);
    }
    
    String getRoleName(int i) {
        return this.memberRoles.get(i);
    }
    
    int getMemberSize() {
        return this.members.size();
    }
    
    boolean fillRelatedGeometries(ArrayList<String> polygonIDs, 
            ArrayList<String> polygonWKT,
            ArrayList<OSMElement> waysWithIdentity,
            ArrayList<OSMElement> nodesWithIdentity) {
        // now... we are going to construct a wkt out of OSM multipolygon... good luck :/
        
        // create a polygon with hole or multiple polygons
        // POLYGON ((10 10, 110 10, 110 110, 10 110), (20 20, 20 30, 30 30, 30 20), (40 20, 40 30, 50 30, 50 20))

        if(this.getOSMIDString().equalsIgnoreCase("6780946")) {
            int debuggingStop = 42;
        }
        
        try {
            StringBuilder wktBuilder = null;
            OSMWay way = null;
            OSMWay next = (OSMWay) this.members.get(0);
            this.intermediateDB.addNodes2OHDMWay(next); // fill way with nodes
            
            int i = 0;
            boolean wayOutside;
            boolean nextOutside = true;
            OSMNode firstNode = null;
            
            boolean lastLoop = false;
            
            while(!lastLoop) {
                // shift
                way = next; // shift
                wayOutside = nextOutside;
                
                /*
                for update we must remember source of geometries.
                In most cases that relation is already stored in intermediate db
                But... But sometimes, e.g. ways have their own identity but also
                add their geometry to e.g. a relation like here.
                
                In those cases, intermediate tables only contain their relations
                to their (let's say first) identity but not to subsequent usages.
                
                Therefore we are going to collect all ways which already have
                an identity.
                */
                if(way.hasOHDMObjectID()) {
                    waysWithIdentity.add(way);
                }
                
                /*
                same goes for nodes of course. In rare cases, ways contain 
                nodes with their own identity. Remember that relation
                 */
                List<OSMElement> iNodesList = way.getNodesWithIdentity();
                if(iNodesList != null && !iNodesList.isEmpty()) {
                    for(OSMElement n : iNodesList) {
                        nodesWithIdentity.add(n);
                    }
                }
                
                // after all this pre-processing start loop now.
                
                // look ahead if possible
                if(++i < memberRoles.size()) {
                    next = (OSMWay) members.get(i);
                    this.intermediateDB.addNodes2OHDMWay(next);
                    nextOutside = this.memberRoles.get(i).equalsIgnoreCase(OSMRelation.OUTER_ROLE);
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
                            // added point and and polygon is finished
                            firstNode = null; // remember.. polygon is closed
                            
                            if(nextOutside) {
                                // no hole. close this polygon
                                polygonIDs.add("-1");
                                polygonWKT.add("SRID=4326;POLYGON(" + wktBuilder.toString() + ")");
                                wktBuilder = null;
                            } else {
                                // there is a hole.. start hole wkt
//                                wktBuilder.append(", (");
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
                                
                                // options: this way can have its geometry already in ohdm
                                String ohdmGeomID = way.getOHDMGeomID();
                                if(ohdmGeomID != null && !ohdmGeomID.equalsIgnoreCase("-1")) {
                                    polygonIDs.add(ohdmGeomID);
                                    polygonWKT.add("");
                                } else {
                                    // geometry not yet stored.. extract wkt for further processing
                                    polygonIDs.add("-1");
                                    polygonWKT.add(way.getWKTGeometry());
                                }
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
                                this.failed(wktBuilder, "unclosed outer polygone in last loop - cannot fix that");
                            }
                            
                            wktBuilder = new StringBuilder();
                            wktBuilder.append("(");
                            // remember first node of this not yet closed polygon
                            firstNode = way.getNodeIter().next();
                            this.addWayToPolygon(firstNode, wktBuilder, way);
                        }
                    }
                ///////////////////////////////////////////////    
                //     processing inner polygon.. a hole     //    
                ///////////////////////////////////////////////    
                    
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
                                polygonWKT.add("SRID=4326;POLYGON(" + wktBuilder.toString() + ")");
                                wktBuilder = null;
                            } // else // start next hole.. nothing todo
                        }
                    } else {
                        // first node is null.. this way at least starts another hole
                        wktBuilder.append(", ( ");
                        if(way.isPolygon()) {
                            this.addWayToPolygon(null, wktBuilder, way);
                            // end complete inner polygon
                            wktBuilder.append(") ");
                            
                            if(nextOutside || lastLoop) {
                                // whole polygon done
                                polygonIDs.add("-1");
                                polygonWKT.add("SRID=4326;POLYGON(" + wktBuilder.toString() + ")");
                                wktBuilder = null;
                                firstNode = null;
                            }
                            
                        } else {
                            // no polygon by itself
                            if(lastLoop) { // cannot open a new polygon
                                this.failed(wktBuilder, "last: loop and new (and unclosed polygon opened - that failure is not fixed here");
                            }
                            // start polygon inside
                            firstNode = way.getNodeIter().next();
                            this.addWayToPolygon(firstNode, wktBuilder, way);
                        }
                    }
                }
                
                /* processed current way, about entering next loop
                
                wktBuilder == null means: whole polygon (with hole) is
                written. A new polygon is to be started.. next must be
                outerWay.
                
                firstNode == null means: A simple polygon (outer boundary
                or inner whole) is finished. 
                
                It not a rare situation that simple polygons are not closed
                because OSM users did not chose identical but similar point.
                That situation can be detected when a switch occurs between
                inner and outer or vice versa.
                
                a) outer -> inner requires firstNode == null
                b) inner -> outer requires wktBuilder == null && firstNode == null
                
                We assume correctness of OSM data and fix that situation be
                adding the missing first node and closing the (simple) polygon.
                */
                
                // case a)
                if(wayOutside && !nextOutside && firstNode != null) {
                    // outer polygon is not yet closed.. fix it
                    wktBuilder.append(",");
                    wktBuilder.append(firstNode.getWKTPointsOnly());
                    wktBuilder.append(")");
                    firstNode = null;
                    // inner polygon can no follow
                    System.err.println("------------------------------------");
                    System.err.print("fixed unclosed outer boundary in multipolygon ");
                    System.err.print("relation osm_id: ");
                    System.err.println(this.getOSMIDString());
                    System.err.println("------------------------------------");
                    
                } 
                // case b)
                else if(!wayOutside && nextOutside && wktBuilder != null) {
                    // close inner polygon
                    wktBuilder.append(",");
                    wktBuilder.append(firstNode.getWKTPointsOnly());
                    wktBuilder.append(")");

                    // finish whole polygon
                    polygonIDs.add("-1");
                    polygonWKT.add("SRID=4326;POLYGON(" + wktBuilder.toString() + ")");
                    wktBuilder = null;
                    firstNode = null;
                    
                    // next polygon can start here
                    System.err.println("------------------------------------");
                    System.err.print("fixed unclosed inner hole in multipolygon ");
                    System.err.print("relation osm_id: ");
                    System.err.println(this.getOSMIDString());
                    System.err.println("------------------------------------");
                }
            }
            
//            // added points but polygon is not yet close
//            if(!nextOutside) {
//                // failure.. outside not close but followed by a hole
//                this.fixUnclosedPolygon(wktBuilder);
//                polygonIDs.add("-1");
//                polygonWKT.add("SRID=4326;POLYGON(" + wktBuilder.toString() + ")");
//                wktBuilder = null;
//            }
            
            
            if(firstNode != null) {
                // fix it
                wktBuilder.append(",");
                wktBuilder.append(firstNode.getWKTPointsOnly());
                wktBuilder.append(")");

                // finish whole polygon
                polygonIDs.add("-1");
                polygonWKT.add("SRID=4326;POLYGON(" + wktBuilder.toString() + ")");
                wktBuilder = null;
                firstNode = null;

                // next polygon can start here
                System.err.println("------------------------------------");
                System.err.print("fixed unclosed multipolygon ");
                System.err.print("relation osm_id: ");
                System.err.println(this.getOSMIDString());
                System.err.println("------------------------------------");
                
//                this.failed(wktBuilder, "polygon not closed");
            }
            
            if(wktBuilder != null && wktBuilder.length() > 0) {
                // save last polygon.. it is a polygon with hole
                polygonIDs.add("-1");
                polygonWKT.add("SRID=4326;POLYGON(" + wktBuilder.toString() + ")");
            }
        } catch (SQLException ex) {
            return false;
        }
        
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

    /**
     * See also http://wiki.openstreetmap.org/wiki/Relation:multipolygon
     * type : multipolygon
     * @return 
     */
    @Override
    protected void produceWKTGeometry() {
        this.wktStringProduced = true;
    }
    
    private boolean addWayToPolygon(OSMNode firstNode, StringBuilder wkt, OSMWay way) throws SQLException {
        // append all way points to wkt
        wkt.append(way.getWKTPointsOnly());
        
        // get last point
        OSMNode lastWayNode = way.getLastPoint();
        
        if(lastWayNode != null && lastWayNode.identical(firstNode)) {
            // polygon finished
            wkt.append(") ");
            return true;
        }
        
        // not closed
        return false;
    }
    
    private void addPoints(ArrayList<OSMWay> wayList, StringBuilder sb) {
        Iterator<OSMWay> wayIter = wayList.iterator();
        boolean first = true;
        while(wayIter.hasNext()) {
            OSMWay way = wayIter.next();
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
    int getGeometryType() {
        return OHDM_DB.POLYGON;
    }
    
    @Override
    boolean isConsistent(PrintStream p) {
        // all member added?
        if(this.memberIDList == null || this.memberIDList.isEmpty() ||
            this.members == null || this.members.isEmpty() ||
            this.memberRoles == null || this.memberRoles.isEmpty()) {
            
            // relation must have member and roles
            p.print("relation #");
            p.print(this.getOSMIDString());
            p.println(" isConsistent: relation must have member and roles");
            p.print("memberIDList null:" + Boolean.toString(memberIDList == null));
            p.print(" / members null:" + Boolean.toString(members == null));
            p.println(" / memberRoles null:" + Boolean.toString(memberRoles == null));
            return false;
        }
        
        // all lists must have same size
        if( this.members.size() != this.memberIDList.size() ||
                this.members.size() != this.memberRoles.size() ||
                this.memberRoles.size() != this.memberIDList.size()) {
            p.print("relation #");
            p.print(this.getOSMIDString());
            p.println(" isConsistent: all lists must have same size");
            p.print("members size:" + members.size());
            p.println(" / memberRoles size:" + memberRoles.size());
            return false;
        }
        
        return super.isConsistent(p);
    }
    
    void addMember(OSMElement element, String roleName) {
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
            if( !roleName.equalsIgnoreCase(OSMRelation.INNER_ROLE)
                    && !roleName.equalsIgnoreCase(OSMRelation.OUTER_ROLE)
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
        
        /**
         * The following scenarios are handled here:
         * a) There is only one outer way but no on top bring it up
         * b) .. I don't know :/
         */
        
        ////////////////////////////////////////////////////////////////
        //                         Scenario a)                        //
        ////////////////////////////////////////////////////////////////
        
        // does it start with an inner role?
        if(this.memberRoles.get(0).equalsIgnoreCase("inner")) {
            // maybe scenario a
            
            int outerRoleIndex = 0;
            // start with 1 .. 0 is already inner.
            for(int roleIndex = 1; roleIndex < this.memberRoles.size(); roleIndex++) {
                if(this.memberRoles.get(roleIndex).equalsIgnoreCase("outer")) {
                    // got one .. is it already the second one?
                    if(outerRoleIndex != 0) {
                        System.err.println("malformed multipolygon: starts with inner role but has more that one outer role.. cannot fix that");
                        return false;
                    } else {
                        outerRoleIndex = roleIndex; // remember index
                    }
                }
            }
            
            // survived that loop - we are in scenario a and can fix it
            
            // move outer role on the top
            // all inner polygons must not overlap in that case. Can just swap

            // remember outMember object
            OSMElement outerMember = this.members.get(outerRoleIndex);
            
            // switch first inner role to outer role slot
            this.members.set(outerRoleIndex, this.members.get(0));
            
            // set outer role on the top
            this.members.set(0, outerMember);
            
            // change roles
            this.memberRoles.set(0, "outer"); // first becomes outer
            this.memberRoles.set(outerRoleIndex, "inner");
            
            // fixed
            return true;
        }
        
        ///////////////////////////////////////////////////////////////
        //                               more??                      //
        ///////////////////////////////////////////////////////////////
        
        // that scenario is just a special case of a - isn't it TODO
        
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
        
        OSMElement firstMember = this.members.get(0);
        this.members.set(0, this.members.get(1));
        this.members.set(1, firstMember);
        
        return true;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        
        sb.append("\n");
        sb.append("members.size()");
        if(this.members != null) {
            sb.append(this.members.size());
        } else {
            sb.append("null");
        }
        sb.append("\t");
        
        
        sb.append("memberIDList.size()");
        if(this.members != null) {
            sb.append(this.memberIDList.size());
        } else {
            sb.append("null");
        }
        sb.append("\t");
        
        sb.append("memberRoles.size()");
        if(this.members != null) {
            sb.append(this.memberRoles.size());
        } else {
            sb.append("null");
        }
        
        return sb.toString();
    }

    private void fixUnclosedPolygon(StringBuilder wktBuilder) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}