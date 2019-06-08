package util;

import util.ManagedStringBuilder.ManagedStringBuilder;

import java.util.List;

/**
 * Klasse UtilCopyImport<br>
 * beinhaltet diverse Hilfsmethoden für die Klasse COPY_OSMImporter<br>
 *
 * @author thsc
 * @author FlorianSauer
 */
@SuppressWarnings("Duplicates")
public class UtilCopyImport {
	/**
	 * Methode serializeTags()<br>
	 * serialisiert key und value in target<br>
	 * @param target Zielort der Serialiserung
	 * @param key Wert1 von tag-Elementen
	 * @param value Wert2 von tag-Elementen
	 */
	public static void serializeTags(StringBuilder target, String key, String value) {
		try {

            if (target != null) {
                if (key != null && key.length() != 0) { // key okay
                    if (value != null && value.length() != 0) { // key and value okay - best case
                        append(target, key);
                        append(target, value);
                    } else { // value is null / empty - but key isnt - perhaps null value have to be stored - continue
                        append(target, key);
                        target.append("0000");
                    }
                } else { // key is null / empty - value doesnt matter - will never be found again
                    target.append("00000000");
                }
            }
		} catch (OutOfMemoryError E){
            System.out.println("Out of memory, failed building the serialized tag and exiting");
            System.out.println("StringBuilder capacity was "+target.capacity());
            System.out.println("StringBuilder length was "+target.length());
            System.out.println("StringBuilder key was "+key);
            System.out.println("StringBuilder value was "+value);
            System.exit(1);
        }
	}
	public static void serializeTags(ManagedStringBuilder target, String key, String value) {
		try {

            if (target != null) {
                if (key != null && key.length() != 0) { // key okay
                    if (value != null && value.length() != 0) { // key and value okay - best case
                        append(target, key);
                        append(target, value);
                    } else { // value is null / empty - but key isnt - perhaps null value have to be stored - continue
                        append(target, key);
                        target.append("0000");
                    }
                } else { // key is null / empty - value doesnt matter - will never be found again
                    target.append("00000000");
                }
            }
		} catch (OutOfMemoryError E){
            System.out.println("Out of memory, failed building the serialized tag and exiting");
            System.out.println("StringBuilder capacity was "+target.capacity());
            System.out.println("StringBuilder length was "+target.length());
            System.out.println("StringBuilder key was "+key);
            System.out.println("StringBuilder value was "+value);
            System.exit(1);
        }
	}

	/**
	 * Methode append()<br>
	 * fügt serialisierten String s dem target hinzu<br>
	 * @param target Ziel der Serialisierung
	 * @param s soll serialisiert / hinzugefügt werden
	 */
	private static void append(StringBuilder target, String s) {
		s = removeStrips(s);
		// if we see it realistic the length of key and value cannot be over 999
		// so we take size of 3
		if (s.length() < 1000) {
			for (int i = 100; i >= 1; i = i / 10) {
				if (s.length() < i) {
					target.append("0");
				}
			}
			target.append(s.length());
			target.append(s);
		} else {
			target.append("0000");
		}
	}
	private static void append(ManagedStringBuilder target, String s) {
		s = removeStrips(s);
		// if we see it realistic the length of key and value cannot be over 999
		// so we take size of 3
		if (s.length() < 1000) {
			for (int i = 100; i >= 1; i = i / 10) {
				if (s.length() < i) {
					target.append("0");
				}
			}
			target.append(s.length());
			target.append(s);
		} else {
			target.append("0000");
		}
	}

	/**
	 * Methode removeStrips()<br>
	 * entfernt '-Zeichen von String str<br>
	 * @param str Inhalt welcher überprüft wird
	 * @return String str welcher überprüft wurde
	 */
	private static String removeStrips(String str) {
		String tmp = "";
		for (int i = str.indexOf("'"); i != -1; i = str.indexOf("'")) {
			tmp = tmp + str.substring(0, i);
			if (i < str.length() - 1) {
				tmp = tmp + str.substring(i + 1);
			}
			str = tmp;
			tmp = "";
		}
		tmp = null;
		return str;
	}

	/**
	 * Methode escapeSpecialChar4SQL()<br>
	 * überprüft ob '-Zeichen in String str vorkommt<br>
	 * maskiert dieses Zeichen falls es vorkommt<br>
	 * @param str ist der String welcher überprüft wird
	 * @return str String welcher überprüft wurde
	 */
	public static String escapeSpecialChar(String str) {
		boolean wasQuoted = false;
		if (str.startsWith("'")) {
			str = str.substring(1, str.length() - 1);
			wasQuoted = true;
		}
		str = str.replace("'", "''");

		if (wasQuoted) {
			str = "'" + str + "'";
		}
		return str;
	}

	/**
	 * Methode getString()<br>
	 * erstellt String aus Elementen der Integerliste<br>
	 * @param elems ist die Integerliste
	 * @return String mit Elementen der Integerliste
	 */
	public static String getString(List<Integer> elems) {
		String sReturn = "";
		if (elems != null) {
			if (elems.size() > 0) {
				for (int i = 0; i < elems.size() - 1; i++) {
					sReturn = sReturn + elems.get(i) + ",";
				}
				sReturn = sReturn + elems.get(elems.size() - 1);
			}
		}
		return sReturn;
	}
}
