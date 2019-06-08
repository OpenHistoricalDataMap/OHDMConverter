package util;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 *
 * @author thsc
 */
public class Util {
    private static final int MAX_DECIMAL_PLACES = 3;
    private static final int HIGHEST_NUMBER_PLUS_ONE = (int) Math.pow(10, MAX_DECIMAL_PLACES);
    
    private static  void appendStringWithLength(StringBuilder target, String s) {
        if(s == null || s.length() == 0) {
            target.append("0000");
            return;
        }
        
        int length = s.length();
        if(length >= HIGHEST_NUMBER_PLUS_ONE) {
            target.append("0000");
            return;
        }

        int hugeNumber = HIGHEST_NUMBER_PLUS_ONE / 10;
        
        while(hugeNumber > 1) {
            if(length >= hugeNumber) {
                break;
            } else {
                target.append("0");
            }
            
            hugeNumber /= 10;
        }
        
        target.append(Integer.toString(length));

        target.append(s);
    }
    
  
    public static void serializeAttributes(StringBuilder target, String key, String value) {
        if(target == null) return;
        
        /* strip forbidden signs (without additional function call and string copy operation!
        accept code duplicates. in most cases, key and value are not copied
        */
        int i = key.indexOf("'");
        while(i != -1) {
            // just throw it away
            StringBuilder sb = new StringBuilder();
            sb.append(key.substring(0, i));
            if(i < key.length()-1) {
                sb.append(key.substring(i+1));
            }
            key = sb.toString();
            i = key.indexOf("'");
        }

        if(value == null) { i = -1; }
        else { i = value.indexOf("'");}
        
        while(i != -1) {
            // just throw it away
            StringBuilder sb = new StringBuilder();
            sb.append(value.substring(0, i));
            if(i < value.length()-1) {
                sb.append(value.substring(i+1));
            }
            value = sb.toString();
            i = value.indexOf("'");
        }
        
        appendStringWithLength(target, key);
        appendStringWithLength(target, value);
    }
    
    public static String getElapsedTime(long since) {
        long now = System.currentTimeMillis();
        long sec = (now - since) / 1000;

        return Util.getTimeString(sec);
    }
    
    public static String getTimeString(long sec) {
        int days = 0;
        int hours = 0;
        int min = 0;
        
        if(sec >= 60) {
            min = (int) (sec / 60);
            sec = sec % 60;
        }
        if(min >= 60) {
            hours = min / 60;
            min = min % 60;
        }
        if(hours >= 24) {
            days = hours / 24;
            hours = hours % 24;
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append(days).append(" : ");
        sb.append(hours).append(" : ");
        sb.append(min).append(" : ");
        sb.append(sec);
        
        return sb.toString();
    }
    
    
    public static void printExceptionMessage(Throwable e, SQLStatementQueue sql, String additionalMessage, boolean goahead) {
        Util.printExceptionMessage(System.err, e, sql, additionalMessage, goahead);
    }

    public static String getNowAsFormatedDateString() {
        StringBuilder sb = new StringBuilder();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss.SSS");
        sb.append("time: ");
        sb.append(df.format(new Date()));
        sb.append("\n");

        return sb.toString();
    }
    
    public static void printExceptionMessage(PrintStream err, Throwable e, SQLStatementQueue sql, String additionalMessage, boolean goahead) {
        StringBuilder sb = new StringBuilder();

        sb.append("****************************************************************************************\n");
        sb.append(Util.getNowAsFormatedDateString());

        if(additionalMessage != null ) {
            sb.append(additionalMessage);
            sb.append("\n");
        }
        
        Util.printExceptionMessage(err, e, sql);
        
        if(!goahead) {
            sb.append("FATAL.. stop executing\n");
            err.print(sb.toString());
            System.exit(1);
        } else {
            sb.append("non-fatal.. go ahead\n");
        }

        sb.append("****************************************************************************************\n");
        err.print(sb.toString());
    }
    
    public static void printExceptionMessage(PrintStream err, Throwable e, SQLStatementQueue sql) {
        if(err == null) {
            err = System.err;
        }
        
        err.println("........................................................................................");
        if(e != null ) err.println(e.getMessage());
        if(sql != null ) err.println(sql.toString());
        if(e != null ) e.printStackTrace(err);
        err.println("........................................................................................");
    }
    
    public static void printExceptionMessage(Throwable e, SQLStatementQueue sql, String additionalMessage) {
        Util.printExceptionMessage(System.err, e, sql, additionalMessage, true);
    }

    public static String escapeXML(String s) {
        if(!s.contains("& ")) return s;

        s = s.replace("& ", "&amp; ");
        return Util.escapeXML(s);
/*
        int offset = 0;
        int index = s.indexOf("&");
        if(index < 0) return s;

        StringBuilder sb = new StringBuilder();

        while (index > -1) {
            if (index > offset) {
                // head
                sb.append(s.substring(offset, index));
            }
            // replace
            sb.append("&amp");

            offset = index+1;
            index = s.indexOf("&", offset);

        }
        // tail
        sb.append(s.substring(offset));

        return sb.toString();
        */
    }

    public static String escapeSpecialChar4SQL(String t) {
        if(t == null) return "";
        boolean wasQuoted = false;
        if(t.startsWith("'")) {
            t = t.substring(1, t.length()-1);
            wasQuoted = true;
        }
        t = t.replace("'", "''");
        t = t.replace("\"", "\\\"");
        t = t.replace("\\\\\"", "\\\"");

        if(wasQuoted) {
            t = "'" + t + "'";
        }
        return t;
    }
    
    static String getThreeDigitString(long value) {
        if(value >= 100) return String.valueOf(value);
        
        StringBuilder s = new StringBuilder();
        s.append("0"); // leading 0
        if(value < 10) {
            s.append("0"); // maybe another one
        }
        
        s.append(value);
        
        return s.toString();
    }
    
    public static String setDotsInStringValue(String value) {
        if(value.length() <= 3) return value;
        
        StringBuilder s = new StringBuilder();
        
        int firstlength = value.length() % 3;
        if(firstlength != 0) {
            s.append(value.subSequence(0, firstlength));
        }
        
        int from = firstlength;
        while(from < value.length()) {
            if(from != 0) {
                s.append(".");
            }
            s.append(value.subSequence(from, from+3));
            from += 3;
        };
        
        return s.toString();
    }
    
    public static String getValueWithDots(long value) {
        if(value < 1000) {
            return String.valueOf(value);
        }
        
        long rest = value % 1000;
        
        String result = Util.getThreeDigitString(rest);
        value /= 1000;
        
        while(value >= 1000) {
            result = Util.getThreeDigitString(value % 1000) + "." + result;
            value /= 1000;
        } 
        
        // finally
        result = value + "." + result;
        return result;
    }

    public static Process feedPSQL(Parameter parameter, String sqlFileName, 
            boolean parallel, boolean removeFile) throws IOException {
        
        // create command line
        StringBuilder sb = new StringBuilder();
        sb.append(parameter.getFullPSQLPath());
        sb.append(" -d ");
        sb.append(parameter.getdbName());
        sb.append(" -f ");
        sb.append(sqlFileName);
        sb.append(" -h ");
        sb.append(parameter.getServerName());
        sb.append(" -p ");
        sb.append(parameter.getPortNumber());
        sb.append(" -U ");
        sb.append(parameter.getUserName());
        sb.append(" -q "); // quiet
        
        Runtime runtime = Runtime.getRuntime();
        
        System.out.println("performing: " + sb.toString());
        Process psqlProcess = runtime.exec(sb.toString());
        
        if(!parallel) {
            try {
                int retCode = psqlProcess.waitFor();
//                System.out.println("..process finished with " + retCode);
            } catch (InterruptedException ex) {
                // won't happen.. no plans to send interrupt 
//                System.out.println("..process produced exception: " + ex.getMessage());
            }
        }
        
        if(removeFile) {
            File f = new File(sqlFileName);
//            if(!f.delete()) {
                f.deleteOnExit();
//            }
        }
        
        return psqlProcess;
    }

    public static HashMap<String, String> parametersToMap(String args[], boolean valueRequired, String helpMessage) {
        if(valueRequired && args.length % 2 != 0) {
            System.err.println("malformed parameter list: each parameter needs a value. ");
            System.err.println(helpMessage);
            return null;
        }

        HashMap<String, String> argumentMap = new HashMap<>();

        int i = 0;
        while(i < args.length) {
            // key is followed by value. Key starts with -
            if(!args[i].startsWith("-")) {
                /* found parameter that does not start with '-' 
                maybe shell parameters. Leave it alone. We are done here
                */
                return argumentMap;
            }

            // value can be empty
            if(args.length > i+1 && !args[i+1].startsWith("-")) {
                // it is a value
                argumentMap.put(args[i], args[i+1]);
                i += 2;
            } else {
                // no value - next parameter
                argumentMap.put(args[i], null);
                i += 1;
            }
        }

        return argumentMap;
    }

    public static Map<String, String> jsonText2Map(String jsonString) {

/*
"addr:city"=>"Berlin", "addr:street"=>"Edisonstraße", "addr:suburb"=>"Oberschöneweide", "addr:country"=>"DE", "addr:postcode"=>"12459", "addr:housenumber"=>"54"
 */
        Map<String, String> map = new HashMap<>();

        if(jsonString == null || jsonString.length() == 0) return map;


        int first = jsonString.indexOf("\"");
        int last = jsonString.indexOf("\"", first+1);

        int offset = 0;
        boolean isKey = true;
        String keyString = null;
        String valueString;

        while(first > -1 && last > -1) {
            if(isKey) {
                keyString = jsonString.substring(first+1, last);
                offset = jsonString.indexOf("=>", first);
            } else {
                valueString = jsonString.substring(first+1, last);
                if(valueString.length() == 0) valueString = null;
                map.put(Util.escapeXML(keyString), Util.escapeXML(valueString));
                offset = last+1;
            }

            isKey = !isKey;
            first = jsonString.indexOf("\"", offset);
            last = jsonString.indexOf("\"", first+1);
        }

        return map;
    }
}
