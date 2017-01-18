package util;

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
        int days = 0;
        int hours = 0;
        int min = 0;
        long now = System.currentTimeMillis();
        long sec = (now - since) / 1000;

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
        System.err.println("****************************************************************************************");

        if(additionalMessage != null ) System.err.println(additionalMessage);
        if(e != null ) System.err.println(e.getMessage());
        if(sql != null ) System.err.println(sql.toString());
        if(e != null ) e.printStackTrace(System.err);
        
        if(!goahead) {
            System.err.println("FATAL.. stop executing");
            System.exit(1);
        } else {
            System.err.println("non-fatal.. go ahead");
        }
        
        System.err.println("****************************************************************************************");
    }
    
    public static void printExceptionMessage(Throwable e, SQLStatementQueue sql, String additionalMessage) {
        Util.printExceptionMessage(e, sql, additionalMessage, true);
    }
    
    static String getThreeDigitString(int value) {
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
    
    public static String getIntWithDots(int value) {
        if(value < 1000) {
            return Util.getThreeDigitString(value);
        }
        
        int rest = value % 1000;
        
        String result = Util.getThreeDigitString(rest);
        value /= 1000;
        
        while(value > 1000) {
            result = Util.getThreeDigitString(value % 1000) + "." + result;
            value /= 1000;
        } 
        
        // finally
        result = value + "." + result;
        return result;
    }
}
