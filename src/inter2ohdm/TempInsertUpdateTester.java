package inter2ohdm;

import osm2inter.OSMImport;

/**
 * just a test class -- sorry, will be rewritten
 * as junit test.. just a temporary test class.
 *
 * thsc
 */
public class TempInsertUpdateTester {
    public static void main(String[] args) {
        // fill intermediate with 'old' data
        String dbFileName = "db_initialImport.txt";
        String osmFileName = "initialImport.osm";

        String[] importArgs = new String[] {osmFileName, dbFileName};

        OSMImport.main(importArgs);


    }

}
