package ohdm2geoserverrendering;

import ohdm2geoserverrendering.IsRunningChecker;
import util.DB;
import util.Parameter;
import util.SQLStatementQueue;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class OHDM2Geoserverrendering {

    public List<String> defaultSQLStatementList = new ArrayList<>();
    public List<String> actualSQLStatementList = new ArrayList<>();

    String admin_labels_sql = "";
    String amenities_sql = "";
    String boundaries_sql = "";
    String buildings_sql = "";
    String housenumbers_sql = "";
    String landusages_sql = "";
    String places_sql = "";
    String roads_sql = "";
    String transport_areas_sql = "";
    String transport_points_sql = "";
    String waterarea_sql = "";
    String waterways_sql = "";

    //EPSG Codes
    final static String PSEUDO_MERCATOR_EPSG = "3857";
    final static String WGS_EPSG = "4326";

    //Table names
    final static String MY_ADMIN_LABELS = "my_admin_labels";
    final static String MY_AMENITIES = "my_amenities";
    final static String MY_BOUNDARIES = "my_boundaries";
    final static String MY_BUILDINGS = "my_buildings";
    final static String MY_LANDUSAGES = "my_landusages";
    final static String MY_HOUSENUMBERS = "my_housenumbers";
    final static String MY_PLACES = "my_places";
    final static String MY_ROADS = "my_roads";
    final static String MY_TRANSPORT_AREAS = "my_transport_areas";
    final static String MY_TRANSPORT_POINTS = "my_transport_points";
    final static String MY_WATERAREA = "my_waterarea";
    final static String MY_WATERWAYS = "my_waterways";

    //for IsRunningChecker notificator
    private static Parameter targetParameterChecker;

    public static void main(String[] args) throws SQLException, IOException {
        String sourceParameterFileName = "db_ohdm.txt";
        String targetParameterFileName = "db_rendering.txt";


        if(args.length > 0) {
            sourceParameterFileName = args[0];
        }

        if(args.length > 1) {
            targetParameterFileName = args[1];
        }

//            Connection sourceConnection = Importer.createLocalTestSourceConnection();
//            Connection targetConnection = Importer.createLocalTestTargetConnection();

        Parameter sourceParameter = new Parameter(sourceParameterFileName);
        Parameter targetParameter = new Parameter(targetParameterFileName);
        setTargetParameterChecker(targetParameterFileName);



        Connection connection = DB.createConnection(targetParameter);

        String targetSchema = targetParameter.getSchema();

        String sourceSchema = sourceParameter.getSchema();

        SQLStatementQueue sql = new SQLStatementQueue(connection);
        OHDM2Geoserverrendering renderer = new OHDM2Geoserverrendering();

        renderer.loadSQLFiles();
        renderer.changeDefaultParametersToActual(targetSchema, sourceSchema);

        renderer.executeSQLStatements(sql);

        System.out.println("Render tables creation for Geoserver finished");

        System.out.println("Start copying symbols into user-dir..");
        renderer.loadSymbolsFromResources();
        System.out.println("Start copying css-files into user-dir..");
        renderer.loadCssFromResourecs();

        if(!renderer.checkFiles()){
            System.out.println("CSS and/or symbolfiles couldnt be created successfully.\n" +
                    "Please download these files manually from: https://github.com/teceP/OSMImportUpdateGeoserverResources");
        }else{
            System.out.println("CSS and symbolfiles has been created successfully.");
        }

        System.out.println("Start transforming epsg-system..");
        renderer.transformEpsg(sql, targetSchema, PSEUDO_MERCATOR_EPSG);

       System.out.println("Start creating spatial indexes..");
       renderer.createSpatialIndex(sql,targetSchema);

    }

    static void setTargetParameterChecker(String targetParameterCheckerString){
        try {
            targetParameterChecker= new Parameter(targetParameterCheckerString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void loadSQLFiles(){
        System.out.println("Load SQL-Files...");

        admin_labels_sql = loadSqlFromResources("resources/sqls/admin_labels.sql");
        amenities_sql = loadSqlFromResources("resources/sqls/amenities.sql");
        boundaries_sql = loadSqlFromResources("resources/sqls/boundaries.sql");
        buildings_sql = loadSqlFromResources("resources/sqls/buildings.sql");
        housenumbers_sql = loadSqlFromResources("resources/sqls/housenumbers.sql");
        landusages_sql = loadSqlFromResources("resources/sqls/landusages.sql");
        places_sql = loadSqlFromResources("resources/sqls/places.sql");
        roads_sql  = loadSqlFromResources("resources/sqls/roads.sql");
        transport_areas_sql = loadSqlFromResources("resources/sqls/transport_areas.sql");
        transport_points_sql = loadSqlFromResources("resources/sqls/transport_points.sql");
        waterarea_sql = loadSqlFromResources("resources/sqls/waterarea.sql");
        waterways_sql = loadSqlFromResources("resources/sqls/waterways.sql");

        defaultSQLStatementList.add(admin_labels_sql);
        defaultSQLStatementList.add(amenities_sql);
        defaultSQLStatementList.add(boundaries_sql);
        defaultSQLStatementList.add(buildings_sql);
        defaultSQLStatementList.add(housenumbers_sql);
        defaultSQLStatementList.add(landusages_sql);
        defaultSQLStatementList.add(places_sql);
        defaultSQLStatementList.add(roads_sql);
        defaultSQLStatementList.add(transport_areas_sql);
        defaultSQLStatementList.add(transport_points_sql);
        defaultSQLStatementList.add(waterarea_sql);
        defaultSQLStatementList.add(waterways_sql);
    }

    void changeDefaultParametersToActual(String targetSchema, String sourceSchema){
        for (String statement : defaultSQLStatementList) {

            String actualStatement = statement.replaceAll("my_test_schema", targetSchema);
            actualStatement = actualStatement.replaceAll("ohdm", sourceSchema);

            actualSQLStatementList.add(actualStatement);
        }
    }

    void executeSQLStatements(SQLStatementQueue sql){

        System.out.println(actualSQLStatementList.size() + " statements in queue. Start execute SQL files... ");

        float eachPercentage = 100/actualSQLStatementList.size();
        float currentPercentage = 0;

        for(String statement: actualSQLStatementList){
            sql.append(statement);

            IsRunningChecker runningChecker= new IsRunningChecker(targetParameterChecker.getServerName(),targetParameterChecker.getPortNumber(),targetParameterChecker.getUserName(),targetParameterChecker.getPWD(),targetParameterChecker.getdbName(),targetParameterChecker.getSchema(),"my_");
            runningChecker.start();

            try {
                sql.forceExecute();

            } catch (SQLException e) {
                e.printStackTrace();
            }
            runningChecker.update();
            runningChecker.interrupt();
            sql.resetStatement();
            System.out.print("Statement finished at:   ");
            System.out.println(LocalDateTime.now());
            currentPercentage = currentPercentage + eachPercentage;
            System.out.println(currentPercentage + " % finished."+"\n");

        }


        System.out.println("100 % finished.");

    }

    public void deleteOldDatas(File dir){

        if(dir.exists()){
            //clean up
            if(dir.isDirectory()){
                String[] files = dir.list();
                for(String file : files){
                    File currentFile = new File(file);
                    currentFile.delete();
                }
            }
        }

    }

    public String loadSqlFromResources(String path){
        InputStream in = getClass().getResourceAsStream(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String content = "";
        try {
            content = readFile(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("File loaded from: " + path);
        return content;
    }


    public String readFile(BufferedReader reader) throws IOException {
        String content = "";
        String line = reader.readLine();

        while(line != null){
            content = content + " " + line + System.lineSeparator();
            line = reader.readLine();
        }

        return content;
    }

    /**
     * CSS
     */

    void loadCssFromResourecs(){
        File targetdir = new File("css");

        this.deleteOldDatas(targetdir);

        targetdir.mkdir();

        JarFile jar = null;
        String path = "resources/css/";
        try {
            jar = new JarFile("OSMImportUpdate.jar");
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();

                if (entry.getName().contains(path) && !entry.isDirectory()) {
                    System.out.println("Load symbol: " + entry.getName());
                    createCssFromResources("/" + entry.getName());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void createCssFromResources(String filename){

        try {
            InputStream in = getClass().getResourceAsStream(filename);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String content = readFile(reader);

            int pos = filename.length()-1;

            while(filename.charAt(pos) != '/'){
                pos--;
            }

            filename = filename.substring(pos);

            File file = new File("css" + filename);
            file.createNewFile();

            BufferedWriter writer = new BufferedWriter(new FileWriter(file)); //write file (content)
            writer.write(content);
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Symbols
     */

    public void loadSymbolsFromResources(){

        System.out.println("Symbol files will be copied to working dir..(" + System.getProperty("user.dir") + "/symbols/ )");

        File targetdir = new File("symbols");

        this.deleteOldDatas(targetdir);

        targetdir.mkdir();

        JarFile jar = null;
        String path = "resources/symbols/";
        try {
            jar = new JarFile("OSMImportUpdate.jar");
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();

                if (entry.getName().contains(path) && !entry.isDirectory()) {
                    System.out.println("Load symbol: " + entry.getName());
                    createSymbolsFromResources("/" + entry.getName());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createSymbolsFromResources(String filename){

        //There are some problems with writing and reading the SVG files..
        //Methode shouldnt be used till its working.. Download symbols from git repo (link in main-method)

       /* try {
            BufferedImage bimage = ImageIO.read(getClass().getResource(filename));

            int pos = filename.length()-1;

            while(filename.charAt(pos) != '/'){
                pos--;
            }

            filename = filename.substring(pos);

            File file = new File("symbols" + filename);
            file.createNewFile();
            ImageIO.write(bimage, "png", file);

        } catch (IOException e) {
           // e.printStackTrace();
        }*/
    }

    public boolean checkFiles(){
        File symbols = new File("symbols");
        boolean symbolFiles = false;

        if(symbols.isDirectory()){
            if(symbols.listFiles().length > 0){
                symbolFiles = true;
            }
        }

        File css = new File("css");
        boolean cssFiles = false;

        if(css.isDirectory()){
            if(css.listFiles().length > 0){
                cssFiles = true;
            }
        }

        return symbolFiles && cssFiles;
    }

    /**
     * Transforms the geometrys to the given epsg-code format
     * @param sql
     * @param targetSchema
     * @param epsgCode Target epsg code
     */
    public void transformEpsg(SQLStatementQueue sql, String targetSchema, String epsgCode){
        System.out.println("Transforming EPSG System (4326 -> " + epsgCode +  ") ...");

        String fullName = "";
        String geometryName = "";

        int errorCounter = 0;

        List<String> tables = new ArrayList<>();
        tables.add(MY_HOUSENUMBERS);
        tables.add(MY_ADMIN_LABELS);
        tables.add(MY_BOUNDARIES);
        tables.add(MY_BUILDINGS);
        tables.add(MY_AMENITIES);
        tables.add(MY_LANDUSAGES);
        tables.add(MY_PLACES);
        tables.add(MY_ROADS);
        tables.add(MY_TRANSPORT_AREAS);
        tables.add(MY_TRANSPORT_POINTS);
        tables.add(MY_WATERAREA);
        tables.add(MY_WATERWAYS);

        for(String table : tables){
            try {

                fullName = targetSchema + "." + table;

                System.out.println("Do transform: " + fullName);

                sql.append("UPDATE ");
                sql.append(fullName);
                sql.append(" SET ");
                sql.append("geometry");
                sql.append(" = ST_TRANSFORM(");
                sql.append("geometry");
                sql.append(", 3857);");

                sql.forceExecute();
                sql.resetStatement();

            } catch (SQLException e) {
                System.err.println("Could not transform table '" + fullName + "' with geometrytype '" + geometryName + "'.");
                System.err.println("= >Exception: " + e.getClass().getSimpleName());
                e.printStackTrace();
                errorCounter++;
            }
        }
        System.out.println("Transforming finished with " + errorCounter + " errors.");
    }


    public void createSpatialIndex(SQLStatementQueue sql, String targetSchema){

        String fullName = "";
        int errorCounter = 0;

        List<String> tables = new ArrayList<>();
        tables.add(MY_HOUSENUMBERS);
        tables.add(MY_ADMIN_LABELS);
        tables.add(MY_BOUNDARIES);
        tables.add(MY_BUILDINGS);
        tables.add(MY_AMENITIES);
        tables.add(MY_LANDUSAGES);
        tables.add(MY_PLACES);
        tables.add(MY_ROADS);
        tables.add(MY_TRANSPORT_AREAS);
        tables.add(MY_TRANSPORT_POINTS);
        tables.add(MY_WATERAREA);
        tables.add(MY_WATERWAYS);

        for(String table : tables){
            try {

                fullName = targetParameterChecker.getdbName()+"."+ targetParameterChecker.getSchema() +"."+  table;

                System.out.println("Create Spatial Index: " + fullName);


                sql.append("CREATE INDEX " +table);
                sql.append("_geometry_idx ON ");
                sql.append(fullName);
                sql.append(" USING GIST (geometry);" );


                sql.forceExecute();
                sql.resetStatement();


            } catch (SQLException e) {
                System.err.println("Could not create spatial index for '" + fullName + "'.");
                System.err.println("= >Exception: " + e.getClass().getSimpleName());
                e.printStackTrace();
                errorCounter++;
            }
        }
        System.out.println("Spatial indexing finished with " + errorCounter + " errors.");
    }

}
