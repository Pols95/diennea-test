package diennea.test.db_benchmarks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQLManager {
    
    private static String BENCHMARKS_DB_NAME = "diennea_test_db";

    private Connection conn = null;

    private String dbmsUrl;    
    private String dbmsUser;
    private String userPw;

    
    public PostgreSQLManager(String dbmsUrl, String dbmsUser, String userPw) throws ClassNotFoundException {        
        this.dbmsUrl = dbmsUrl;        
        this.dbmsUser = dbmsUser;
        this.userPw = userPw;
        Class.forName("org.postgresql.Driver");
    }

    
    public void executesBenchmarks() {
        try {            
            createBenchmarksDB();
            executesInsertBenchmarks();
            executesSelectBenchmarks();            
        } catch (SQLException e) {            
            e.printStackTrace();
        } catch (IOException e) { 
            e.printStackTrace();
        } finally {
//            try {
//                dropDB(BENCHMARKS_DB_NAME);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
        }
    }
    
    private void createBenchmarksDB() throws SQLException, IOException {        
        try {
            // DBMS connection and benchmarks DB creation.
            createDB(BENCHMARKS_DB_NAME);        
            
            // Benchmarks DB connection and table creation.
            connectToDB(BENCHMARKS_DB_NAME);
            
            String tableDDL = String.join("",
                    Files.readAllLines(Paths.get(Main.class.getResource("/benchmarks_db_table.sql").getPath())));             

            createTable(tableDDL);            
        } catch (SQLException | IOException e) {
            System.err.println("Benckmarks DB table creation failed.");
            throw e;
        }        
    }
    
    private void executesInsertBenchmarks() {
        // TODO Auto-generated method stub
        
    }

    private void executesSelectBenchmarks() {
        // TODO Auto-generated method stub
        
    }

    
    
    public void createDB(String dbName) throws SQLException { 
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://" + dbmsUrl + "/", dbmsUser, userPw)) {               
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE DATABASE " + dbName);
                System.out.println("Database " + dbName + " created.");
            } catch (SQLException e) {
                System.err.println("Database " + dbName + " creation failed.");
                throw e;
            }
        } catch (SQLException e) {            
            throw e;
        }  
    }    

    public void dropDB(String dbName) throws SQLException {       
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://" + dbmsUrl + "/", dbmsUser, userPw)) {
            disconnectFromDB();
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP DATABASE " + dbName);
                System.out.println("Database " + dbName + " dropped.");
            } catch (SQLException e) {
                System.err.println("Database " + dbName + " dropping failed.");
                throw e;
            }                     
        } catch (SQLException e) {            
            throw e;    
        }
    }
    
    public void connectToDB(String dbName) throws SQLException {                        
        try {
            disconnectFromDB();
            
            conn = DriverManager.getConnection("jdbc:postgresql://" + dbmsUrl + "/" + dbName, dbmsUser, userPw);
        } catch (SQLException e) {
            System.err.println("Errors occured on connection opening.");
            throw e;
        }    
    }
    
    public void disconnectFromDB() throws SQLException {
        if (conn == null) return;
        try {
            conn.close();
        } catch (SQLException e) {
            System.err.println("Errors occured on connection closing.");
            throw e;
        } finally {
            conn = null;
        }        
    }
    
    public void createTable(String tableDDL) throws SQLException {
        if (conn == null) throw new SQLException("Cannot create a table without an established connection.");
        
        final String tableName = tableDDL.split(" ")[2];
        
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(tableDDL);
            System.out.println("Table " + tableName + " created.");
        } catch (SQLException e) {
            System.err.println("Table " + tableName + " creation failed.");
            throw e;
        }      
    }

}
