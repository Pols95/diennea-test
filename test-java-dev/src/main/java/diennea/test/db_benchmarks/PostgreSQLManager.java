package diennea.test.db_benchmarks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Simple class for PostgreSQL dbms instance (server) management.
 * 
 * @author paolo
 *
 */
public class PostgreSQLManager {
    
    private static String BENCHMARKS_DB_NAME = "diennea_test_db";

    private Connection conn = null;
    private String connectedDB = null;

    private String dbmsUrl;    
    private String dbmsUser;
    private String userPw;

    /**
     * 
     * @param dbmsUrl: url of the PostgreSQL dbms instance (server) to manage (i.e. host[:port])
     * @param dbmsUser: user (name) for the authentication
     * @param userPw: password for the authentication
     * 
     * @throws ClassNotFoundException: raised whether the PostgreSQL Driver class cannot be found.
     */
    public PostgreSQLManager(String dbmsUrl, String dbmsUser, String userPw) throws ClassNotFoundException {        
        this.dbmsUrl = dbmsUrl;        
        this.dbmsUser = dbmsUser;
        this.userPw = userPw;
        Class.forName("org.postgresql.Driver");
    }

    /**
     * Executes a benchmark of noStatementsPerTransaction statements per noTransactions transactions according to 
     * "insert" and "select" operation on a temporary database created on the PostgreSQL instance (server) managed.
     * 
     * @param noTransactions: number of transactions to execute during benchmark.
     * @param noStatementsPerTransaction: number of statements each transaction has to execute during benchmark.
     */
    public void executesBenchmarks(int noTransactions, int noStatementsPerTransaction) {
        try {            
            createBenchmarksDB();
            executesInsertBenchmarks(noTransactions, noStatementsPerTransaction);
            executesSelectBenchmarks(noTransactions, noStatementsPerTransaction);            
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
    
    private void executesInsertBenchmarks(int noTransactions, int noStatementsPerTransaction) {
        // TODO Auto-generated method stub
        
    }

    private void executesSelectBenchmarks(int noTransactions, int noStatementsPerTransaction) {
        // TODO Auto-generated method stub
        
    }

    
    /**
     * Creates a DB with the given name.
     * @param dbName: name of the database to create.
     * @throws SQLException: whether PostgreSQL instance (server) access or database creation fail.
     */
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

    /**
     * Drops the DB with the given name.
     * @param dbName: name of the database to drop.
     * @throws SQLException: whether PostgreSQL instance (server) access or database dropping fail.
     */
    public void dropDB(String dbName) throws SQLException {       
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://" + dbmsUrl + "/", dbmsUser, userPw)) {            
            if (dbName.equals(connectedDB)) disconnectFromDB();            
            
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
    
    /**
     * Connects to DB with the given name.
     * One connection to one single DB is managed at time (existing connections close).
     * @param dbName:name of the database to connect to.
     * @throws SQLException: whether new DB connection opening (to given DB name) or existing DB
     *                       connection closing (on managed PostgreSQL instance-server) fail.
     */
    public void connectToDB(String dbName) throws SQLException {                        
        try {
            disconnectFromDB();
            
            conn = DriverManager.getConnection("jdbc:postgresql://" + dbmsUrl + "/" + dbName, dbmsUser, userPw);
            connectedDB = dbName;
        } catch (SQLException e) {
            System.err.println("Errors occured on connection opening.");
            throw e;
        }    
    }
    
    /**
     * Disconnects from current connected DB.
     * @throws SQLException: whether existing DB connection closing (on managed PostgreSQL instance-server) fail.
     */
    public void disconnectFromDB() throws SQLException {
        if (conn == null) return;
        try {
            conn.close();
        } catch (SQLException e) {
            System.err.println("Errors occured on connection closing.");
            throw e;
        } finally {
            conn = null;
            connectedDB = null;
        }        
    }
    
    /**
     * Creates a table in the current connected DB.
     * @param tableDDL: Data Definition Language statement for table definition.
     * @throws SQLException: whether none connection to a DB exists or the table creation fails.
     */
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
