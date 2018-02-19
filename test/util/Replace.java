package util;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author thsc
 */
public class Replace {
    
    public Replace() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
     @Test
     public void hello() {
         String testIn = "'Addenbrooke's'";
         String testOut = "'Addenbrooke''s'";
         
         
         String o = Util.escapeSpecialChar(testIn);
         
         Assert.assertEquals(testOut, o);
         
         testIn = "'a'b'c''d'";
         testOut = "'a''b''c''''d'";
         
         
         o = Util.escapeSpecialChar(testIn);
         
         Assert.assertEquals(testOut, o);
         
     }
}
