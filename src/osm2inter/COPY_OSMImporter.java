package osm2inter;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.sql.SQLException;
import util.DBCopyConnector;
import util.UtilCopyImport;
import osm.OSMClassificationCopyImport;

/**
 * Klasse COPY_OSMImporter<br>
 * ist die Handlerklasse für den XmlSaxParser<br>
 * extends DefaultHandler<br>
 * <br>
 * Handler benutzt momentan folgende SQL-Tabellenstruktur<br>
 * <br>
 * NODE<br>
 * psql_table_id|osm_id|tstamp|classcode|otherclasscodes|serializedTags|lon|lat|LEER|LEER|LEER|LEER|LEER|has_name|valid<br>
 * <br>
 * WAY<br>
 * psql_table_id|osm_id|tstamp|classcode|otherclasscodes|serializedTags|LEER|LEER|memberIDS(Nodes)|LEER|LEER|LEER|has_name|valid<br>
 * <br>
 * RELATION<br>
 * psql_table_id|osm_id|tstamp|classcode|otherclasscodes|serializedTags|LEER|LEER|memberIDS(all)|LEER|LEER|LEER|has_name|valid<br>
 * <br>
 * WAYMEMBER<br>
 * psql_table_id|way_id|node_id<br>
 * <br>
 * RELATIONMEMBER<br>
 * psql_table_id|actual_relation_id|member_node_id|member_way_id|member_rel_id|role<br>
 */
public class COPY_OSMImporter extends DefaultHandler {
	private Locator xmlFileLocator;

	/* (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#setDocumentLocator(org.xml.sax.Locator)
	 */
	public void setDocumentLocator(Locator locator) {
		xmlFileLocator = locator;
	}

	// Organisation
	private HashMap<String, DBCopyConnector> conns;
	public static final String[] connsNames = { "nodes", "relationmember", "relations", "waynodes", "ways" };
	private String delNode, delWay, delRel, delWayMem, delRelMem;

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
		conns = connectors;
		delNode = conns.get(connsNames[0]).getDelimiter();
		delWay = conns.get(connsNames[4]).getDelimiter();
		delRel = conns.get(connsNames[2]).getDelimiter();
		delWayMem = conns.get(connsNames[3]).getDelimiter();
		delRelMem = conns.get(connsNames[1]).getDelimiter();
		adminLevel = status = classCode = 0;
		nodes = ways = rels = 0;
		curMainElemID = timeStamp = lon = lat = memberIDs = "";
		otherClassCodes = new ArrayList<>();
		serTags = new StringBuilder();
		hasName = false;
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
		System.out.println("Nodes: " + nodes + " | Ways: " + ways + " | Relations: " + rels + "\n");
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
				if (status == STATUS_OUTSIDE) {
					startMainElement(attributes, qName);
				} else {
					System.out.println("XML-Error: Opening MainElement at Line " + xmlFileLocator.getLineNumber() + " is inside another MainElement.");
				}
				break;
			}
			case "tag":
			case "nd":
			case "member": {
				if (status != STATUS_OUTSIDE) {
					startInnerElement(attributes, qName);
				} else {
					System.out.println("XML-Error: Opening InnerElement at Line " + xmlFileLocator.getLineNumber() + " is outside of a MainElement.");
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
				if (status != STATUS_OUTSIDE) {
					endMainElement(qName);
				} else {
					System.out.println("XML-Error: Single closing MainElement at Line " + xmlFileLocator.getLineNumber());
				}
				break;
			}
			case "tag":
			case "nd":
			case "member": {
				if (status == STATUS_OUTSIDE) {
					System.out.println("XML-Error: Closing InnerElement at Line " + xmlFileLocator.getLineNumber() + " is outside of a MainElement.");
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
		// reset the placeholders, vars, organisation-container
		adminLevel = classCode = 0;
		curMainElemID = timeStamp = lon = lat = memberIDs = "";
		otherClassCodes = new ArrayList<>();
		serTags = new StringBuilder();
		hasName = false;

		if (attr.getValue("id") != null) {
			curMainElemID = attr.getValue("id");
			switch (name) {
				case "node": {
					status = STATUS_NODE;
					if (attr.getValue("lon") != null && attr.getValue("lat") != null) {
						lon = attr.getValue("lon");
						lat = attr.getValue("lat");
					} else {
						System.out.println("XML-Error: MainElement at Line " + xmlFileLocator.getLineNumber() + " has no lon and/or lat value.");
					}
					break;
				}
				case "way": {
					status = STATUS_WAY;
					break;
				}
				case "relation": {
					status = STATUS_RELATION;
					break;
				}
			}
			if (attr.getValue("timestamp") != null) {
				timeStamp = attr.getValue("timestamp");
				UtilCopyImport.serializeTags(serTags, "uid", attr.getValue("uid"));
				UtilCopyImport.serializeTags(serTags, "user", attr.getValue("user"));
			} else {
				System.out.println("XML-Error: MainElement at Line " + xmlFileLocator.getLineNumber() + " has no timestamp value.");
			}
		} else {
			System.out.println("XML-Error: MainElement at Line " + xmlFileLocator.getLineNumber() + " has no id value.");
		}
	}

	/**
	 * Methode endMainElement()<br>
	 * wird aufgerufen wenn in der XML-Datei ein schließendes Tag eines der Hauptelemente vorkommt<br>
	 * @param name ist der Name des Elementes
	 */
	private void endMainElement(String name) {
		// adjust classcode
		if (classCode > 0) {
			if (classCode == OSMClassificationCopyImport.getOHDMClassCode("boundary", "administrative")) {
				if (adminLevel > 0) {
					classCode = OSMClassificationCopyImport.getOHDMClassCode("ohdm_boundary", "adminlevel_" + adminLevel);
				}
			}
		}
		switch (name) {
			// update stats and write to SQL-DB
			case "node": {
				nodes++;
				try {
					// psql_table_id|osm_id|tstamp|classcode|otherclasscodes|serializedTags|lon|lat|LEER|LEER|LEER|LEER|LEER|has_name|valid
//				conns.get(connsNames[0]).write("" + delNode +
					conns.get(connsNames[0]).write("null" + delNode +
							curMainElemID + delNode +
							timeStamp + delNode +
							classCode + delNode +
							UtilCopyImport.getString(otherClassCodes) + delNode +
							serTags.toString() + delNode +
							lon + delNode +
							lat + delNode +
							"" + delNode +
							"" + delNode +
							"" + delNode +
							"" + delNode +
							"" + delNode +
							Boolean.toString(hasName) + delNode +
							"true");
				} catch (SQLException e) {
					System.out.println("SQL-Error: Couldn't write final String to Node-Table.");
					System.out.println("MainElements: " + (nodes + ways + rels));
					e.printStackTrace();
					System.exit(1); // probeweise
				}
				break;
			}
			case "way": {
				ways++;
				try {
					// psql_table_id|osm_id|tstamp|classcode|otherclasscodes|serializedTags|LEER|LEER|memberIDS(Nodes)|LEER|LEER|LEER|has_name|valid
//				conns.get(connsNames[4]).write("" + delWay +
					conns.get(connsNames[4]).write("null" + delWay +
							curMainElemID + delWay +
							timeStamp + delWay +
							classCode + delWay +
							UtilCopyImport.getString(otherClassCodes) + delWay +
							serTags.toString() + delWay +
							"" + delWay +
							"" + delWay +
							memberIDs + delWay +
							"" + delWay +
							"" + delWay +
							"" + delWay +
							Boolean.toString(hasName) + delWay +
							"true");
				} catch (SQLException e) {
					System.out.println("SQL-Error: Couldn't write final String to Way-Table.");
					System.out.println("MainElements: " + (nodes + ways + rels));
					e.printStackTrace();
					System.exit(1); // probeweise
				}
				break;
			}
			case "relation": {
				rels++;
				try {
					// psql_table_id|osm_id|tstamp|classcode|otherclasscodes|serializedTags|LEER|LEER|memberIDS(all)|LEER|LEER|LEER|has_name|valid
//				conns.get(connsNames[2]).write("" + delRel +
					conns.get(connsNames[2]).write("null" + delRel +
							curMainElemID + delRel +
							timeStamp + delRel +
							classCode + delRel +
							UtilCopyImport.getString(otherClassCodes) + delRel +
							serTags.toString() + delRel +
							"" + delRel +
							"" + delRel +
							memberIDs + delRel +
							"" + delRel +
							"" + delRel +
							"" + delRel +
							Boolean.toString(hasName) + delRel +
							"true");
				} catch (SQLException e) {
					System.out.println("SQL-Error: Couldn't write final String to Rel-Table.");
					System.out.println("MainElements: " + (nodes + ways + rels));
					e.printStackTrace();
					System.exit(1); // probeweise
				}
				break;
			}
		}
		status = STATUS_OUTSIDE;
		// set placeholders, vars and organisation-container to null
		// to force the garbage-collector
		curMainElemID = timeStamp = lon = lat = memberIDs = null;
		otherClassCodes = null;
		serTags = null;
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
							UtilCopyImport.serializeTags(serTags, attr.getValue(0), attr.getValue(1));
						} else if (OSMClassificationCopyImport.containsValue(attr.getValue(0))) {
							if (classCode == 0) {
								classCode = OSMClassificationCopyImport.getOHDMClassCode(attr.getValue(0), attr.getValue(1));
							} else {
								otherClassCodes.add(OSMClassificationCopyImport.getOHDMClassCode(attr.getValue(0), attr.getValue(1)));
							}
						} else if (attr.getValue(0).equalsIgnoreCase("admin_level")) {
							try {
								adminLevel = Integer.parseInt(attr.getValue(1));
							} catch (NumberFormatException e) {
								System.out.println("XML-Error: InnerElement 'tag' at Line " + xmlFileLocator.getLineNumber() + " does contain a not parsable Integer value.");
								adminLevel = 0;
								e.printStackTrace();
							}
						} else {
							UtilCopyImport.serializeTags(serTags, attr.getValue(0), attr.getValue(1));
							if (attr.getValue(0).equalsIgnoreCase("name")) {
								hasName = true;
							}
						}
					} else {
						System.out.println("XML-Error: InnerElement 'tag' at Line " + xmlFileLocator.getLineNumber() + " has one or two null-values.");
					}
				} else {
					System.out.println("XML-Error: InnerElement 'tag' at Line " + xmlFileLocator.getLineNumber() + " has more/less than 2 attributes.");
				}
				break;
			}
			case "nd": {
				if (status == STATUS_WAY) {
					if (attr.getValue("ref") != null) {
						if (memberIDs.isEmpty()) {
							memberIDs = attr.getValue("ref");
						} else {
							memberIDs = memberIDs + "," + attr.getValue("ref");
						}
						// write to SQL-DB
						try {
							// psql_table_id|way_id|node_id
//						conns.get(connsNames[3]).write("" + delWayMem +
							conns.get(connsNames[3]).write("null" + delWayMem +
									curMainElemID + delWayMem +
									attr.getValue("ref"));
						} catch (SQLException e) {
							System.out.println("SQL-Error: Couldn't write final String to WayMem-Table.");
							System.out.println("MainElements: " + (nodes + ways + rels));
							e.printStackTrace();
							System.exit(1); // probeweise
						}
					} else {
						System.out.println("XML-Error: InnerElement 'nd' at Line " + xmlFileLocator.getLineNumber() + " has a null-value at 'ref'.");
					}
				} else {
					System.out.println("XML-Error: InnerElement 'nd' at Line " + xmlFileLocator.getLineNumber() + "is not inside a way.");
				}
				break;
			}
			case "member": {
				if (status == STATUS_RELATION) {
					if (attr.getValue("ref") != null) {
						if (memberIDs.isEmpty()) {
							memberIDs = attr.getValue("ref");
						} else {
							memberIDs = memberIDs + "," + attr.getValue("ref");
						}
						if (attr.getValue("type") != null) {
							// empty skeleton
							String relIDs = "" + delRelMem +
									"" + delRelMem +
									"" + delRelMem;
							switch (attr.getValue("type").toLowerCase()) {
								case "node": // 1st place
									relIDs = attr.getValue("ref") + delRelMem +
											"" + delRelMem +
											"" + delRelMem;
									break;
								case "way": // 2nd place
									relIDs = "" + delRelMem +
											attr.getValue("ref") + delRelMem +
											"" + delRelMem;
									break;
								case "relation": // 3rd place
									relIDs = "" + delRelMem +
											"" + delRelMem +
											attr.getValue("ref") + delRelMem;
									break;
								default:
									System.out.println("XML-Error: InnerElement 'member' at Line " + xmlFileLocator.getLineNumber() + " has no correct value at 'type'.");
									break;
							}
							if (attr.getValue("role") != null) {
								// write to SQL-DB
								try {
									// psql_table_id|actual_relation_id|member_node_id|member_way_id|member_rel_id|role
//								conns.get(connsNames[1]).write("" + delRelMem +
									conns.get(connsNames[1]).write("null" + delRelMem +
											curMainElemID + delRelMem +
											relIDs +
											UtilCopyImport.escapeSpecialChar(attr.getValue("role")));
								} catch (SQLException e) {
									System.out.println("SQL-Error: Couldn't write final String to RelMem-Table.");
									System.out.println("MainElements: " + (nodes + ways + rels));
									e.printStackTrace();
									System.exit(1); // probeweise
								}
							} else {
								System.out.println("XML-Error: InnerElement 'member' at Line " + xmlFileLocator.getLineNumber() + " has a null-value at 'role'.");
							}
						} else {
							System.out.println("XML-Error: InnerElement 'member' at Line " + xmlFileLocator.getLineNumber() + " has a null-value at 'type'.");
						}
					} else {
						System.out.println("XML-Error: InnerElement 'member' at Line " + xmlFileLocator.getLineNumber() + " has a null-value at 'ref'.");
					}
				} else {
					System.out.println("XML-Error: InnerElement 'member' at Line " + xmlFileLocator.getLineNumber() + "is not inside a relation.");
				}
				break;
			}
		}
	}
}
