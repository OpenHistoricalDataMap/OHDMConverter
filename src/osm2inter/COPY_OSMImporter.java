package osm2inter;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.helpers.DefaultHandler;
import osm.OSMClassificationCopyImport;
import util.DBCopyConnector;
import util.UtilCopyImport;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
 */
@SuppressWarnings("Duplicates")
public class COPY_OSMImporter extends DefaultHandler {
	private Locator xmlFileLocator;
	private long parsedElements;
	private long gcIndex;

	/* (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#setDocumentLocator(org.xml.sax.Locator)
	 */
	public void setDocumentLocator(Locator locator) {
		this.xmlFileLocator = locator;
	}

	// Organisation
	private HashMap<String, DBCopyConnector> conns;
	public static final String[] connsNames = { "nodes", "relationmember", "relations", "waynodes", "ways" };
	private String delimiterNode, delimiterWay, delimiterRel, delimiterWayMem, delimiterRelMem;

	private int adminLevel, status;

	// Status Stats Log
	private static final int STATUS_OUTSIDE = 0;
	private static final int STATUS_NODE = 1;
	private static final int STATUS_WAY = 2;
	private static final int STATUS_RELATION = 3;

	// Logging
	private long nodes, ways, rels;

	// Elements of FinalValues
	private String curMainElemID, timeStamp;
	private int classCode;
	private List<Integer> otherClassCodes;
	private StringBuilder serTags;
	private String lon, lat, memberIDs;
	private boolean hasName;

	/**
	 * Konstruktor der Klasse<br>
	 * @param connectors ist die Hashmap mit Objekten von DBCopyConnector
	 */
	public COPY_OSMImporter(HashMap<String, DBCopyConnector> connectors) {
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
		this.serTags = new StringBuilder();
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
		switch (qName) {
			case "node":
			case "way":
			case "relation": {
				if (this.status == this.STATUS_OUTSIDE) {
					startMainElement(attributes, qName);
				} else {
					System.out.println("XML-Error: Opening MainElement at Line " + this.xmlFileLocator.getLineNumber() + " is inside another MainElement.");
				}
				break;
			}
			case "tag":
			case "nd":
			case "member": {
				if (this.status != this.STATUS_OUTSIDE) {
					startInnerElement(attributes, qName);
				} else {
					System.out.println("XML-Error: Opening InnerElement at Line " + this.xmlFileLocator.getLineNumber() + " is outside of a MainElement.");
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void endElement(String uri, String localName, String qName) {
		switch (qName) {
			case "node":
			case "way":
			case "relation": {
				if (this.status != this.STATUS_OUTSIDE) {
					endMainElement(qName);
				} else {
					System.out.println("XML-Error: Single closing MainElement at Line " + this.xmlFileLocator.getLineNumber());
				}
				break;
			}
			case "tag":
			case "nd":
			case "member": {
				if (this.status == this.STATUS_OUTSIDE) {
					System.out.println("XML-Error: Closing InnerElement at Line " + this.xmlFileLocator.getLineNumber() + " is outside of a MainElement.");
				}
			}
		}
	}

	/**
	 * Methode startMainElement()<br>
	 * wird aufgerufen wenn in der XML-Datei ein öffnendes Tag eines der Hauptelemente vorkommt<br>
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
		this.serTags = new StringBuilder();
		this.hasName = false;

		if (attr.getValue("id") != null) {
			this.curMainElemID = attr.getValue("id");
			switch (name) {
				case "node": {
					this.status = this.STATUS_NODE;
					if (attr.getValue("lon") != null && attr.getValue("lat") != null) {
						this.lon = attr.getValue("lon");
						this.lat = attr.getValue("lat");
					} else {
						System.out.println("XML-Error: MainElement at Line " + this.xmlFileLocator.getLineNumber() + " has no lon and/or lat value.");
					}
					break;
				}
				case "way": {
					this.status = this.STATUS_WAY;
					break;
				}
				case "relation": {
					this.status = this.STATUS_RELATION;
					break;
				}
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
	 * @param name ist der Name des Elementes
	 */
	private void endMainElement(String name) {
		if (this.classCode > 0) {
			if (this.classCode == OSMClassificationCopyImport.getOHDMClassCode("boundary", "administrative")) {
				if (this.adminLevel > 0) {
					this.classCode = OSMClassificationCopyImport.getOHDMClassCode("ohdm_boundary", "adminlevel_" + this.adminLevel);
				}
			}
		}
		switch (name) {
			case "node": {
				this.nodes++;
				try {
					// NULL|osm_id|tstamp|classcode|otherclasscodes|serTags|lon|lat|NULL|NULL|NULL|NULL|NULL|has_name|valid
					this.conns.get(this.connsNames[0]).write(
							this.curMainElemID + this.delimiterNode +
									this.timeStamp + this.delimiterNode +
									this.classCode + this.delimiterNode +
									UtilCopyImport.getString(this.otherClassCodes) + this.delimiterNode +
									this.serTags.toString() + this.delimiterNode +
									this.lon + this.delimiterNode +
									this.lat + this.delimiterNode +
									Boolean.toString(this.hasName) + this.delimiterNode +
									"true");
				} catch (SQLException e) {
					System.out.println("SQL-Error: Couldn't write final String to Node-Table.");
					System.out.println("MainElements: " + (this.nodes + this.ways + this.rels));
					e.printStackTrace();
					System.exit(1); // probeweise
				}
				break;
			}
			case "way": {
				this.ways++;
				try {
					// NULL|osm_id|tstamp|classcode|otherclasscodes|serTags|NULL|NULL|memberIDs|NULL|NULL|NULL|has_name|valid
					this.conns.get(this.connsNames[4]).write(
							this.curMainElemID + this.delimiterWay +
									this.timeStamp + this.delimiterWay +
									this.classCode + this.delimiterWay +
									UtilCopyImport.getString(this.otherClassCodes) + this.delimiterWay +
									this.serTags.toString() + this.delimiterWay +
									this.memberIDs + this.delimiterWay +
									Boolean.toString(this.hasName) + this.delimiterWay +
									"true");
				} catch (SQLException e) {
					System.out.println("SQL-Error: Couldn't write final String to Way-Table.");
					System.out.println("MainElements: " + (this.nodes + this.ways + this.rels));
					e.printStackTrace();
					System.exit(1); // probeweise
				}
				break;
			}
			case "relation": {
				this.rels++;
				try {
					// NULL|osm_id|tstamp|classcode|otherclasscodes|serTags|NULL|NULL|memberIDs|NULL|NULL|NULL|has_name|valid
					this.conns.get(this.connsNames[2]).write(
							this.curMainElemID + this.delimiterRel +
									this.timeStamp + this.delimiterRel +
									this.classCode + this.delimiterRel +
									UtilCopyImport.getString(this.otherClassCodes) + this.delimiterRel +
									this.serTags.toString() + this.delimiterRel +
									this.memberIDs + this.delimiterRel +
									Boolean.toString(this.hasName) + this.delimiterRel +
									"true");
				} catch (SQLException e) {
					System.out.println("SQL-Error: Couldn't write final String to Rel-Table.");
					System.out.println("MainElements: " + (this.nodes + this.ways + this.rels));
					e.printStackTrace();
					System.exit(1); // probeweise
				}
				break;
			}
		}
		this.status = this.STATUS_OUTSIDE;
		this.curMainElemID = null;
		this.timeStamp = null;
		this.lon = null;
		this.lat = null;
		this.memberIDs = null;
		this.otherClassCodes = null;
		this.serTags = null;
		this.parsedElements++;
		this.gcIndex++;
		if (this.gcIndex >= 1000000){ // trigger every 1 Million parsed elements the garbage collector to perform a full collection
			System.gc();
			this.gcIndex = 0;
		}
	}

	/**
	 * Methode startInnerElement()<br>
	 * wird aufgerufen wenn in der XML-Datei ein öffnendes Tag eines der inneren Elemente vorkommt<br>
	 * @param attr sind die Attribute des Elementes
	 * @param name ist der Name des Elementes
	 */
	private void startInnerElement(Attributes attr, String name) {
		switch (name) {
			case "tag": {
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
						} else if (OSMClassificationCopyImport.containsValue(attr.getValue(0))) {
							if (this.classCode == 0) {
								this.classCode = OSMClassificationCopyImport.getOHDMClassCode(attr.getValue(0), attr.getValue(1));
							} else {
								this.otherClassCodes.add(OSMClassificationCopyImport.getOHDMClassCode(attr.getValue(0), attr.getValue(1)));
							}
						} else if (attr.getValue(0).equalsIgnoreCase("admin_level")) {
							try {
								this.adminLevel = Integer.parseInt(attr.getValue(1));
							} catch (NumberFormatException e) {
								System.out.println("XML-Error: InnerElement 'tag' at Line " + this.xmlFileLocator.getLineNumber() + " does contain a not parsable Integer value.");
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
			}
			case "nd": {
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
									this.curMainElemID + this.delimiterWayMem +
											attr.getValue("ref"));
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
			}
			case "member": {
				if (this.status == this.STATUS_RELATION) {
					if (attr.getValue("ref") != null) {
						if (this.memberIDs.isEmpty()) {
							this.memberIDs = attr.getValue("ref");
						} else {
							this.memberIDs = this.memberIDs + "," + attr.getValue("ref");
						}
						if (attr.getValue("type") != null) {
							// empty skeleton
							String relIDs = "NULL" + this.delimiterRelMem +
									"NULL" + this.delimiterRelMem +
									"NULL" + this.delimiterRelMem;
							switch (attr.getValue("type").toLowerCase()) {
								case "node": // 1st place
									relIDs = attr.getValue("ref") + this.delimiterRelMem +
											"NULL" + this.delimiterRelMem +
											"NULL" + this.delimiterRelMem;
									break;
								case "way": // 2nd place
									relIDs = "NULL" + this.delimiterRelMem +
											attr.getValue("ref") + this.delimiterRelMem +
											"NULL" + this.delimiterRelMem;
									break;
								case "relation": // 3rd place
									relIDs = "NULL" + this.delimiterRelMem +
											"NULL" + this.delimiterRelMem +
											attr.getValue("ref") + this.delimiterRelMem;
									break;
								default:
									System.out.println("XML-Error: InnerElement 'member' at Line " + this.xmlFileLocator.getLineNumber() + " has no correct value at 'type'.");
									break;
							}
							if (attr.getValue("role") != null) {
								try {
									// NULL|rel_id|member_node_id|member_way_id|member_rel_id|role
									this.conns.get(this.connsNames[1]).write(
											this.curMainElemID + this.delimiterRelMem +
													relIDs +
													UtilCopyImport.escapeSpecialChar(attr.getValue("role")));
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
}
