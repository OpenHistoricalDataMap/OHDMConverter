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
    
    void fillRelatedGeometries(ArrayList<String> polygonIDs, ArrayList<String> polygonWKT) {
        // now... we are going to construct a wkt out of OSM multipolygon... good luck :/
        
        // create a polygon with hole
        // POLYGON ((10 10, 110 10, 110 110, 10 110), (20 20, 20 30, 30 30, 30 20), (40 20, 40 30, 50 30, 50 20))

        if(this.getOSMIDString().equalsIgnoreCase("3323434")) {
            int debuggingStop = 42;
        }
        
        try {
            StringBuilder wktBuilder = null;
            OHDMWay way;
            OHDMWay next = (OHDMWay) this.members.get(0);
            this.intermediateDB.addNodes2OHDMWay(next); // fill way with nodes
            
            int i = 1;
            boolean wayOutside;
            boolean nextOutside = true;
            String firstNode = null;
            
            while(i < memberRoles.size()) {
                way = next; // shift
                wayOutside = nextOutside;
                
                next = (OHDMWay) members.get(i++);
                this.intermediateDB.addNodes2OHDMWay(next);
                
                nextOutside = this.memberRoles.get(i).equalsIgnoreCase(OHDMRelation.OUTER_ROLE);
                
                if(wayOutside) {
                    // just a sequence of a previous non polygon way?
                    if(wktBuilder != null) {
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
                        }
                    } else {
                        // no previous ways in wkt
                        if(way.isPolygon()) {
                            // is a polygon by itself.. has it a hole ?
                            if(nextOutside) { 
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
                               start new polygone
                            */
                            wktBuilder = new StringBuilder();
                            wktBuilder.append("(");
                            // remember first node of this not yet closed polygon
                            firstNode = way.getNodeIter().next().getWKTGeometry();
                            this.addWayToPolygon(firstNode, wktBuilder, way);
                        }
                    }
                } else {
                    // inner polygons
                    // in any case.. a wkt string builder exist
                    if(firstNode != null) {
                        // this polygon is a sequel.. add it
                        if(this.addWayToPolygon(firstNode, wktBuilder, way)) {
                            // polygon closed
                            firstNode = null;
                            
                            if(nextOutside) {
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
                            // start polygon inside
                            wktBuilder.append(", ( ");
                            firstNode = way.getNodeIter().next().getWKTGeometry();
                            this.addWayToPolygon(firstNode, wktBuilder, way);
                        }
                    }
                }
            }
            
            if(firstNode != null) {
                // failure... still a polygon open..
                throw new SQLException("malformed polygon");
            }
            
            if(wktBuilder != null && wktBuilder.length() > 0) {
                // save final polygon.. it is a polygon with hole
                polygonIDs.add("-1");
                polygonWKT.add(wktBuilder.toString());
            }
        } catch (SQLException ex) {
            System.err.println("failure during constructing polygons: " + ex.getMessage());
        }
        
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("osmid: " + this.getOSMIDString());
        for(int i=0; i<polygonIDs.size();i++) {
            System.out.println("id: " + polygonIDs.get(i));
            System.out.println("wktstring:\n" + polygonWKT.get(i));
        }
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        
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
    
    private boolean addWayToPolygon(String firstNode, StringBuilder wkt, OHDMWay way) throws SQLException {
        // firstNode == null .. just add long / lat
        Iterator<OHDMNode> nodeIter = way.getNodeIter();
        while(nodeIter.hasNext()) {
            OHDMNode node = nodeIter.next();
            wkt.append(node.getLongitude());
            wkt.append(" ");
            wkt.append(node.getLatitude());
            
            if(firstNode != null && firstNode.equalsIgnoreCase(node.getWKTGeometry())) {
                // finished
                wkt.append(") ");
                return true;
            }
        }
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
                break;
            }
        }
        
        this.polygonChecked = true;
        return this.isPolygon;
    }

}
