package ohdm2geoserverrendering;

import java.sql.*;
import java.time.LocalDateTime;

public class IsRunningChecker extends Thread {
    private Boolean flag=null;
    private String servername;
    private String portnumber;
    private String username;
    private String pwd;
    private String dbname;
    private String schema;
    private String statementIdentifierString;

    public IsRunningChecker(String servername, String portnumber, String username, String pwd, String dbname, String schema, String statementIdentifierString){
       //statementIdentifierString to identify the right Query in pg_stat_activities. Should be a unique Word within that query
        this.servername=servername;
        this.portnumber=portnumber;
        this.username=username;
        this.pwd=pwd;
        this.dbname=dbname;
        this.schema=schema;
        this.statementIdentifierString=statementIdentifierString;

        flag = true;
    }

    public void run(){
        System.out.println();
        System.out.println("running Statement for scheme: "+schema);
            while(flag==true){

                String url = "jdbc:postgresql://"+servername+":"+portnumber+"/"+dbname;
                String user = username;
                String password = pwd;

                try (Connection con = DriverManager.getConnection(url, user, password);
                     Statement st = con.createStatement();
                     ResultSet rs = st.executeQuery("SELECT state_change,waiting,state,datname, pid, state, query, age(clock_timestamp(), query_start) AS age \n" +
                             "FROM pg_stat_activity\n" +
                             "WHERE state <> 'idle' \n" +
                             " AND datname='"+dbname+"'  AND query NOT LIKE '% FROM pg_stat_activity %' AND query LIKE '%"+statementIdentifierString+"%' \n" +
                             "ORDER BY age;")) {
                    if (rs.next()) {
                        System.out.println("\n | last checked state change: "+rs.getString(1)+" at: "+LocalDateTime.now());
                        System.out.println(" | waiting: "+rs.getString(2));
                        System.out.println(" | PID: "+rs.getString(5));
                    }
                } catch (SQLException ex) {
                    try (Connection con = DriverManager.getConnection(url, user, password);
                         Statement st = con.createStatement();
                         ResultSet rs = st.executeQuery("SELECT state_change,wait_event,wait_event_type,state,datname, pid, state, query, age(clock_timestamp(), query_start) AS age \n" +
                                 "FROM pg_stat_activity\n" +
                                 "WHERE state <> 'idle' \n" +
                                 " AND datname='"+dbname+"'  AND query NOT LIKE '% FROM pg_stat_activity %' AND query LIKE '%"+statementIdentifierString+"%' \n" +
                                 "ORDER BY age;")) {
                        if (rs.next()) {
                            System.out.println(" \n | last checked state change: "+rs.getString(1)+" at: "+LocalDateTime.now());
                            System.out.println(" | wait event: "+rs.getString(2));
                            System.out.println(" | wait event type: "+rs.getString(3));
                            System.out.println(" | PID: "+rs.getString(6));
                        }
                    } catch (SQLException exe) {
                        System.out.println(ex.getMessage());
                        ex.printStackTrace();
                        System.out.println(exe.getMessage());
                        exe.printStackTrace();
                    }
                }
                try{
                    sleep(300000);
                }
                catch(Exception e){
                }
            }
        }


    public void update(){
        flag=false;
    }
}
