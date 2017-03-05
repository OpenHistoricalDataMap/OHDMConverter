package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/**
 *
 * @author thsc
 */
public class FTest {
    
    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        File f = new File("testfile.txt");
        
        PrintStream ps = new PrintStream(f, "UTF-8");
        // StandardCharsets.UTF_8
        ps.print("Ä");
    }
    
}
