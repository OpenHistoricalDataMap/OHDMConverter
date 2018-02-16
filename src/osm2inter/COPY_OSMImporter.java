package osm2inter;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.helpers.DefaultHandler;
import util.CopyConnector;
import util.ManagedStringBuilder.ManagedStringBuilder;
import util.UtilCopyImport;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import osm.OSMClassification;

/**
 * Klasse COPY_OSMImporter<br>
 * ist die Handlerklasse für den XmlSaxParser<br>
 * extends DefaultHandler<br>
 * <br>
 * Handler benutzt momentan folgende SQL-Tabellenstruktur<br>
 * <br>
 * NODE<br>
 * osm_id|tstamp|classcode|otherclasscodes|serTags|lon|lat|has_name|valid<br>
 * <br>
 * WAY<br>
 * osm_id|tstamp|classcode|otherclasscodes|serTags|memberIDs|has_name|valid<br>
 * <br>
 * RELATION<br>
 * osm_id|tstamp|classcode|otherclasscodes|serTags|memberIDs|has_name|valid<br>
 * <br>
 * WAYMEMBER<br>
 * way_id|node_id<br>
 * <br>
 * RELATIONMEMBER<br>
 * rel_id|member_node_id|member_way_id|member_rel_id|role<br>
 *
 * @author Jan-Derk Wendt
 * @author FlorianSauer
 */
@SuppressWarnings("Duplicates")
public class COPY_OSMImporter extends DefaultHandler {
    private Locator xmlFileLocator;
    private long parsedElements;
    private long gcIndex;

    /**
     * this method is used for escaping special chars, so the given string matches the csv format
     */
    public String serializedTagsToCsv(String serializedTags) {
        return "\"" + serializedTags.replace("\r", "\\r").replace("\n", "\\n") + "\"";
//		"\""+.replace(this.delimiterNode, "\\"+this.delimiterNode)+"\"" + this.delimiterNode +

    }

    public void setDocumentLocator(Locator locator) {
        this.xmlFileLocator = locator;
    }

    // Organisation
    private HashMap<String, CopyConnector> conns;
    public static final String[] connsNames = {"nodes", "relationmember", "relations", "waynodes", "ways"};
    private String delimiterNode, delimiterWay, delimiterRel, delimiterWayMem, delimiterRelMem;

    private int adminLevel, status;

    // Status codes, used for signaling where the parser is currently at; which element is he currently processing
    // finishing processing a entitity
    private static final int STATUS_OUTSIDE = 0;
    // currently processing a Node
    private static final int STATUS_NODE = 1;
    // currently processing a Way
    private static final int STATUS_WAY = 2;
    // currently processing a Relation
    private static final int STATUS_RELATION = 3;

    // Logging
    private long nodes, ways, rels;

    // Elements of FinalValues
    private String curMainElemID, timeStamp;
    private int classCode;
    private List<Integer> otherClassCodes;
    private int serTagsSize;
//    remplacing string builder with a ManagedStringBuilder that only operates with one array
//    private StringBuilder serTags;
    private ManagedStringBuilder serTags;

    private String lon, lat, memberIDs;
    private boolean hasName;

    /**
     * Konstruktor der Klasse<br>
     *
     * @param connectors ist die Hashmap mit Objekten von CopyConnector
     */
    public COPY_OSMImporter(HashMap<String, CopyConnector> connectors, 
            int serTagsSize) {
        
        this.conns = connectors;
        this.delimiterNode = this.conns.get(connsNames[0]).getDelimiter();
        this.delimiterWay = this.conns.get(connsNames[4]).getDelimiter();
        this.delimiterRel = this.conns.get(connsNames[2]).getDelimiter();
        this.delimiterWayMem = this.conns.get(connsNames[3]).getDelimiter();
        this.delimiterRelMem = this.conns.get(connsNames[1]).getDelimiter();
        this.adminLevel = this.status = classCode = 0;
        this.nodes = 0;
        this.ways = 0;
        this.rels = 0;
        this.curMainElemID = "";
        this.timeStamp = "";
        this.lon = "";
        this.lat = "";
        this.memberIDs = "";
        this.otherClassCodes = new ArrayList<>();
        
//        this.serTagsSize = 2000000000;
        this.serTagsSize = 200000;
        this.serTagsSize = serTagsSize;
        
        this.serTags = new ManagedStringBuilder(new char[this.serTagsSize]);
        this.hasName = false;
    }

    /* (non-Javadoc)
     * @see org.xml.sax.helpers.DefaultHandler#startDocument()
     */
    @Override
    public void startDocument() {
        System.out.println("...start...");
        status = STATUS_OUTSIDE;
    }

    /* (non-Javadoc)
     * @see org.xml.sax.helpers.DefaultHandler#endDocument()
     */
    @Override
    public void endDocument() {
        System.out.println("...end...");
        System.out.println("Nodes: " + this.nodes + " | Ways: " + this.ways + " | Relations: " + this.rels + "\n");
    }

    /* (non-Javadoc)
     * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
     */
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
//        if (qName != "changeset") {
//            System.out.println("uri" + uri);
//            System.out.println("localName" + localName);
//            System.out.println("qName" + qName);
//        }
        switch (qName) {
            case "node": {
                if (this.status != this.STATUS_OUTSIDE) {
                    System.out.println("node found but not outside, currently pseudo @ " + this.xmlFileLocator.getLineNumber());
                    System.out.println("this means a node element is declared inside a node element");
                    System.exit(1);
                }
                startMainElement(attributes, qName);
            }
            break;
            case "way": {
                if (this.status != this.STATUS_OUTSIDE) {
                    System.out.println("way found but not outside, currently pseudo @ " + this.xmlFileLocator.getLineNumber());
                    System.out.println("this means a way element is declared inside a way element");
                    System.exit(1);
                }
                startMainElement(attributes, qName);
            }
            break;
            case "relation": {
                if (this.status != this.STATUS_OUTSIDE) {
                    System.out.println("relation found but not outside, currently pseudo @ " + this.xmlFileLocator.getLineNumber());
                    System.out.println("this means a relation element is declared inside a relation element");
                    System.exit(1);
                }
                startMainElement(attributes, qName);
            }
            break;

            case "tag": {
                if (this.status != this.STATUS_OUTSIDE) {
                    startInnerElement(attributes, qName);
                } else {
//                    System.out.println("tag found but outside, currently pseudo @ " + this.xmlFileLocator.getLineNumber());
//                    System.out.println("this means a tag element is declared outside");
//                    System.exit(1);

                }
            }
            break;
            case "nd": {
                if (this.status != this.STATUS_OUTSIDE) {
                    startInnerElement(attributes, qName);
                } else {
//                    System.out.println("nd found but outside, currently pseudo @ " + this.xmlFileLocator.getLineNumber());
//                    System.out.println("this means a nd element is declared outside");
//                    System.exit(1);
                }
            }
            break;
            case "member": {
                if (this.status != this.STATUS_OUTSIDE) {
                    startInnerElement(attributes, qName);
                } else {
//                    System.out.println("member found but outside, currently pseudo @ " + this.xmlFileLocator.getLineNumber());
//                    System.out.println("this means a member element is declared outside");
//                    System.exit(1);
                }

//                if (this.status == this)
//				if (this.status != this.STATUS_OUTSIDE) {
//				} else {
//					System.out.println("XML-Error: Opening InnerElement at Line " + this.xmlFileLocator.getLineNumber() + " is outside of a MainElement.");
//				}
            }
            break;
            default:
        }
        this.gcIndex++;
        if (this.gcIndex >= 1000000) { // trigger every 1000000 parsed elements the garbage collector to perform a full collection
            System.gc();
            this.gcIndex = 0;
            System.out.println("reached mark 1.000.000 in COPY_OSMImporter @ time " + System.currentTimeMillis()+" @ pseudoline "+this.xmlFileLocator.getLineNumber());
        }
    }

    /* (non-Javadoc)
     * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void endElement(String uri, String localName, String qName) {
        switch (qName) {
            case "node":
                endMainElement(qName);
                break;
            case "way":
                endMainElement(qName);
                break;
            case "relation":
                endMainElement(qName);

//				if (this.status != this.STATUS_OUTSIDE) {
//				} else {
//					System.out.println("XML-Error: Single closing MainElement at Line " + this.xmlFileLocator.getLineNumber());
//				}
                break;

            case "tag":
                break;
            case "nd":
                break;
            case "member":
                break;

//				if (this.status == this.STATUS_OUTSIDE) {
//					System.out.println("XML-Error: Closing InnerElement at Line " + this.xmlFileLocator.getLineNumber() + " is outside of a MainElement.");
//				}

        }
    }

    /**
     * Methode startMainElement()<br>
     * wird aufgerufen wenn in der XML-Datei ein öffnendes Tag eines der Hauptelemente vorkommt<br>
     *
     * @param attr sind die Attribute des Elementes
     * @param name ist der Name des Elementes
     */
    private void startMainElement(Attributes attr, String name) {
        this.adminLevel = 0;
        this.classCode = 0;
        this.curMainElemID = "";
        this.timeStamp = "";
        this.lon = "";
        this.lat = "";
        this.memberIDs = "";
        this.otherClassCodes = new ArrayList<>();
        if (this.serTags == null){
            this.serTags = new ManagedStringBuilder(new char[this.serTagsSize]);
        } else {
            this.serTags.empty();
        }
        this.hasName = false;

        if (attr.getValue("id") != null) {
            this.curMainElemID = attr.getValue("id");
            switch (name) {
                case "node":
                    this.status = this.STATUS_NODE;
                    if (attr.getValue("lon") != null && attr.getValue("lat") != null) {
                        this.lon = attr.getValue("lon");
                        this.lat = attr.getValue("lat");
                    } else {
                        System.out.println("XML-Error: MainElement at Line " + this.xmlFileLocator.getLineNumber() + " has no lon and/or lat value.");
                    }
                    break;

                case "way":
                    this.status = this.STATUS_WAY;
                    break;

                case "relation":
                    this.status = this.STATUS_RELATION;
                    break;

            }
            if (attr.getValue("timestamp") != null) {
                this.timeStamp = attr.getValue("timestamp");
                UtilCopyImport.serializeTags(this.serTags, "uid", attr.getValue("uid"));
                UtilCopyImport.serializeTags(this.serTags, "user", attr.getValue("user"));
            } else {
                System.out.println("XML-Error: MainElement at Line " + this.xmlFileLocator.getLineNumber() + " has no timestamp value.");
            }
        } else {
            System.out.println("XML-Error: MainElement at Line " + this.xmlFileLocator.getLineNumber() + " has no id value.");
        }
    }

    /**
     * Methode endMainElement()<br>
     * wird aufgerufen wenn in der XML-Datei ein schließendes Tag eines der Hauptelemente vorkommt<br>
     *
     * @param name ist der Name des Elementes
     */
    private void endMainElement(String name) {
        if (this.classCode > 0) {
            if (this.classCode == OSMClassification.getOSMClassification().getOHDMClassID("boundary", "administrative")) {
                if (this.adminLevel > 0) {
                    this.classCode = OSMClassification.getOSMClassification().getOHDMClassID("ohdm_boundary", "adminlevel_" + this.adminLevel);
                }
            }
        }
        switch (name) {
            case "node":
                this.nodes++;
                try {
                    // NULL|osm_id|tstamp|classcode|otherclasscodes|serTags|lon|lat|NULL|NULL|NULL|NULL|NULL|has_name|valid
                    this.conns.get(this.connsNames[0]).write(new String[]{
                            this.curMainElemID,
                            this.timeStamp,
                            "" + this.classCode,
                            UtilCopyImport.getString(this.otherClassCodes),
                            this.serTags.toString(),
                            this.lon,
                            this.lat,
                            Boolean.toString(this.hasName),
                            "true"
                    });
                } catch (SQLException e) {
                    System.out.println("SQL-Error: Couldn't write final String to Node-Table.");
                    System.out.println("MainElements: " + (this.nodes + this.ways + this.rels));
                    e.printStackTrace();
                    System.exit(1); // probeweise
                }
                break;

            case "way":
                this.ways++;
                try {
                    // NULL|osm_id|tstamp|classcode|otherclasscodes|serTags|NULL|NULL|memberIDs|NULL|NULL|NULL|has_name|valid
                    this.conns.get(this.connsNames[4]).write(new String[]{
                            this.curMainElemID,
                            this.timeStamp,
                            "" + this.classCode,
                            UtilCopyImport.getString(this.otherClassCodes),
                            this.serTags.toString(),
                            this.memberIDs,
                            Boolean.toString(this.hasName),
                            "true"});
                } catch (SQLException e) {
                    System.out.println("SQL-Error: Couldn't write final String to Way-Table.");
                    System.out.println("MainElements: " + (this.nodes + this.ways + this.rels));
                    e.printStackTrace();
                    System.exit(1); // probeweise
                }
                break;

            case "relation":
                this.rels++;
                try {
                    // NULL|osm_id|tstamp|classcode|otherclasscodes|serTags|NULL|NULL|memberIDs|NULL|NULL|NULL|has_name|valid
                    this.conns.get(this.connsNames[2]).write(new String[]{
                            this.curMainElemID,
                            this.timeStamp,
                            "" + this.classCode,
                            UtilCopyImport.getString(this.otherClassCodes),
                            this.serTags.toString(),
                            this.memberIDs,
                            Boolean.toString(this.hasName),
                            "true"});
                } catch (SQLException e) {
                    System.out.println("SQL-Error: Couldn't write final String to Rel-Table.");
                    System.out.println("MainElements: " + (this.nodes + this.ways + this.rels));
                    e.printStackTrace();
                    System.exit(1); // probeweise
                }
                break;

        }
        this.status = this.STATUS_OUTSIDE;
        this.curMainElemID = null;
        this.timeStamp = null;
        this.lon = null;
        this.lat = null;
        this.memberIDs = null;
        this.otherClassCodes = null;

//        instead of deleting string builder and making a new one, we empty it
//        this.serTags = null;
        this.serTags.empty();
//        System.out.println("breakpoint1 "+this.serTags.length());
//        this.serTags.setLength(0);
//        this.serTags.setLength(this.serTagsSize);

        this.parsedElements++;

    }

    /**
     * Methode startInnerElement()<br>
     * wird aufgerufen wenn in der XML-Datei ein öffnendes Tag eines der inneren Elemente vorkommt<br>
     *
     * @param attr sind die Attribute des Elementes
     * @param name ist der Name des Elementes
     */
    private void startInnerElement(Attributes attr, String name) {
        switch (name) {
            case "tag":
                // key and value --> size 2
                if (attr.getLength() == 2) {
                    if (attr.getValue(0) != null && attr.getValue(1) != null) {
                        if (attr.getValue(1).equalsIgnoreCase("yes") || attr.getValue(1).equalsIgnoreCase("no")) {
                            // this values describe if sth is present / given or not
                            // at first in "if" before selecting the osm_classes
                            // because of pairs like "building-yes" would trigger
                            // the osm-main-class "building" with the default value
                            // "undefined" for a subclass
                            UtilCopyImport.serializeTags(this.serTags, attr.getValue(0), attr.getValue(1));
                        } else if (OSMClassification.getOSMClassification().classExists(attr.getValue(0))) {
                            if (this.classCode == 0) {
                                this.classCode = OSMClassification.getOSMClassification().getOHDMClassID(attr.getValue(0), attr.getValue(1));
                            } else {
                                this.otherClassCodes.add(OSMClassification.getOSMClassification().getOHDMClassID(attr.getValue(0), attr.getValue(1)));
                            }
                        } else if (attr.getValue(0).equalsIgnoreCase("admin_level")) {
                            try {
                                this.adminLevel = Integer.parseInt(attr.getValue(1));
                            } catch (NumberFormatException e) {
                                System.out.println("XML-Error: InnerElement 'tag' at Line " + this.xmlFileLocator.getLineNumber() + " does contain a not parsable Integer value >"+attr.getValue(1)+"<.");
                                this.adminLevel = 0;
                                e.printStackTrace();
                            }
                        } else {
                            UtilCopyImport.serializeTags(this.serTags, attr.getValue(0), attr.getValue(1));
                            if (attr.getValue(0).equalsIgnoreCase("name")) {
                                this.hasName = true;
                            }
                        }
                    } else {
                        System.out.println("XML-Error: InnerElement 'tag' at Line " + this.xmlFileLocator.getLineNumber() + " has one or two null-values.");
                    }
                } else {
                    System.out.println("XML-Error: InnerElement 'tag' at Line " + this.xmlFileLocator.getLineNumber() + " has more/less than 2 attributes.");
                }
                break;

            case "nd":
                if (this.status == this.STATUS_WAY) {
                    if (attr.getValue("ref") != null) {
                        if (this.memberIDs.isEmpty()) {
                            this.memberIDs = attr.getValue("ref");
                        } else {
                            this.memberIDs = this.memberIDs + "," + attr.getValue("ref");
                        }
                        try {
                            // NULL|way_id|node_id
                            this.conns.get(this.connsNames[3]).write(
                                    new String[]{
                                            this.curMainElemID, attr.getValue("ref")});
                        } catch (SQLException e) {
                            System.out.println("SQL-Error: Couldn't write final String to WayMem-Table.");
                            System.out.println("MainElements: " + (this.nodes + this.ways + this.rels));
                            e.printStackTrace();
                            System.exit(1); // probeweise
                        }
                    } else {
                        System.out.println("XML-Error: InnerElement 'nd' at Line " + this.xmlFileLocator.getLineNumber() + " has a null-value at 'ref'.");
                    }
                } else {
                    System.out.println("XML-Error: InnerElement 'nd' at Line " + this.xmlFileLocator.getLineNumber() + "is not inside a way.");
                }
                break;

            case "member":
                if (this.status == this.STATUS_RELATION) {
                    if (attr.getValue("ref") != null) {
                        if (this.memberIDs.isEmpty()) {
                            this.memberIDs = attr.getValue("ref");
                        } else {
                            this.memberIDs = this.memberIDs + "," + attr.getValue("ref");
                        }
                        if (attr.getValue("type") != null) {
                            // empty skeleton
                            String[] relIDs;
                            switch (attr.getValue("type").toLowerCase()) {
                                case "node": // 1st place
                                    relIDs = new String[]{attr.getValue("ref"), "NULL", "NULL"};
                                    break;
                                case "way": // 2nd place
                                    relIDs = new String[]{"NULL", attr.getValue("ref"), "NULL"};
                                    break;
                                case "relation": // 3rd place
                                    relIDs = new String[]{"NULL", "NULL", attr.getValue("ref")};
                                    break;
                                default:
                                    relIDs = new String[]{"NULL", "NULL", "NULL"};
                                    System.out.println("XML-Error: InnerElement 'member' at Line " + this.xmlFileLocator.getLineNumber() + " has no correct value at 'type'.");
                                    break;
                            }
                            if (attr.getValue("role") != null) {
                                try {
                                    // NULL|rel_id|member_node_id|member_way_id|member_rel_id|role
                                    // -> relation_id|node_id|way_id|member_rel_id|role
                                    this.conns.get(this.connsNames[1]).write(new String[]{
                                            this.curMainElemID, relIDs[0], relIDs[1], relIDs[2], UtilCopyImport.escapeSpecialChar(attr.getValue("role"))}
                                    );
                                } catch (SQLException e) {
                                    System.out.println("SQL-Error: Couldn't write final String to RelMem-Table.");
                                    System.out.println("MainElements: " + (this.nodes + this.ways + this.rels));
                                    e.printStackTrace();
                                    System.exit(1); // probeweise
                                }
                            } else {
                                System.out.println("XML-Error: InnerElement 'member' at Line " + this.xmlFileLocator.getLineNumber() + " has a null-value at 'role'.");
                            }
                        } else {
                            System.out.println("XML-Error: InnerElement 'member' at Line " + this.xmlFileLocator.getLineNumber() + " has a null-value at 'type'.");
                        }
                    } else {
                        System.out.println("XML-Error: InnerElement 'member' at Line " + this.xmlFileLocator.getLineNumber() + " has a null-value at 'ref'.");
                    }
                } else {
                    System.out.println("XML-Error: InnerElement 'member' at Line " + this.xmlFileLocator.getLineNumber() + "is not inside a relation.");
                }
                break;

        }
    }
}
