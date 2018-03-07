package util;

import inter2ohdm.OHDMImporter;
import inter2ohdm.OHDMUpdateInter;
import osm2inter.OSMImport;

import java.io.IOException;
import java.sql.SQLException;

/**
 * just a test class -- sorry, will be rewritten
 * as junit test.. just a temporary test class.
 *
 * thsc
 */
public class TempInsertUpdateTester {
    public static void main(String[] args) throws SQLException, IOException {
        // initial import
        String dbInitialImportConfigFile = "db_initialImport.txt";
        String osmInitialImportFileName = "initialImport.osm";
        String dbOHDMConfigFile = "db_ohdm.txt";

        String[] importArgs = new String[] {osmInitialImportFileName, dbInitialImportConfigFile};
        // import to intermediate
        OSMImport.main(importArgs);

        // initial ohdm import
        OHDMImporter.main(new String[]{dbInitialImportConfigFile, dbOHDMConfigFile});

        // import update intermediate
        String dbUpdateIntermediateConfigFile = "db_updateImport.txt";
        String osmUpdateFileName = "updateImport.osm";

        String[] importUpdateArgs = new String[] {osmUpdateFileName, dbUpdateIntermediateConfigFile};
        OSMImport.main(importUpdateArgs);

        // make that tagging in intermediate and drop update db in that process

        String[] updateArgs = new String[] {dbInitialImportConfigFile, dbUpdateIntermediateConfigFile,
                dbOHDMConfigFile, "2018-03-01"};

        OHDMUpdateInter.main(updateArgs);
    }
}
