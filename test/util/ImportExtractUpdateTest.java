package util;

import java.sql.SQLException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import osm2inter.OSMImport;

/**
 *
 * @author thsc
 */
public class ImportExtractUpdateTest {
    
    public ImportExtractUpdateTest() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void importExtractUpdate() throws SQLException {
        
        // fill intermediate with 'old' data
        String dbFileName = "db_local_sample_intermediate.txt";
        String osmFileName = "sample_intermediate.osm";
        
        OSMImport.main(new String[] {osmFileName, dbFileName});
    
    }
}
