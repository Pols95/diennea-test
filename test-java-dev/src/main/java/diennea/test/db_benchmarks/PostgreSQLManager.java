package diennea.test.db_benchmarks;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * Simple class for a PostgreSQL server management.
 * 
 * @author paolo
 *
 */
public class PostgreSQLManager {

    private static final String BENCHMARKS_DB_NAME = "diennea_benchmarks_test_db";
    private static final boolean DROP_BENCHMARKS_DB_AFTER_TEST = true;

    private Connection conn = null;
    private String connectedDB = null;

    private String dbmsUrl;    
    private String dbmsUser;
    private String userPw;

    /**
     * Class constructor.
     * 
     * @param dbmsUrl: url of the PostgreSQL server to manage (i.e. host[:port]).
     * @param dbmsUser: user (name) for server authentication.
     * @param userPw: password for server authentication.
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
     * Executes an INSERT benchmark of noStatementsPerTransaction statements per noTransactions transactions and 
     * a SELECT benchmark of noSelectStatements statements on a temporary database created on the PostgreSQL server managed.
     * 
     * @param noTransactions: number of transactions to execute during the benchmark.
     * @param noStatementsPerTransaction: number of INSERT statements each transaction has to execute during the benchmark.
     * @param noSelectStatements: number of SELECT statements to execute during the benchmark.
     */
    public void executeBenchmarks(int noTransactions, int noStatementsPerTransaction, int noSelectStatements) {
        System.out.println("Benchmarks initializing...");
        try {
            createBenchmarksDB();            
            executeInsertBenchmark(noTransactions, noStatementsPerTransaction);
            executeSelectBenchmark(noSelectStatements);         
        } catch (SQLException | IOException | URISyntaxException e) {            
            e.printStackTrace();
        } finally {
            if (!DROP_BENCHMARKS_DB_AFTER_TEST) return;
            try {
                dropDB(BENCHMARKS_DB_NAME);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }   

    private void createBenchmarksDB() throws SQLException, IOException, URISyntaxException {        
        try {            

            // DBMS connection and benchmarks DB creation.
            createDB(BENCHMARKS_DB_NAME);        

            // Benchmarks DB connection and table creation.
            connectToDB(BENCHMARKS_DB_NAME);

            String tableDDL = String.join (
                    "",
                    Files.readAllLines(Paths.get(Main.class.getResource("/benchmarks_db_table.sql").toURI()))
            );             

            createTable(tableDDL);            
        } catch (SQLException | IOException | URISyntaxException e) {
            System.err.println("Benckmarks DB table creation failed.");
            throw e;
        }
    }

    private void executeInsertBenchmark(int noTransactions, int noStatementsPerTransaction) throws SQLException {
        float avg = 0, max = -1, min = -1;
        int statementsMade = 0;
        int transactionsFailed = 0;        
        System.out.println("\n# Benchmark with " + noTransactions + " transactions and " + 
                noStatementsPerTransaction + " INSERT statements per transaction:");
        
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO Student VALUES (?, ?, ?, ?)")) {
            conn.setAutoCommit(false);
            for (int i = 0; i < noTransactions; i++) {
                // Single transaction
                try {
                    for (int j = 0; j < noStatementsPerTransaction; j++) {
                        final int userIndex = i * noStatementsPerTransaction + j;
                        stmt.setInt(1, userIndex);
                        stmt.setString(2, "student_" + userIndex + "_firstname");
                        stmt.setString(3, "student_" + userIndex + "_lastname");
                        stmt.setDate(4, new Date(System.currentTimeMillis()));

                        // Benchmark
                        long start = System.nanoTime();
                        stmt.executeUpdate();
                        long end = System.nanoTime();
                        float amount = (end - start) / 1000F;                        

                        statementsMade++;
                        avg += amount;
                        max = amount > max ? amount : max;
                        min = amount < min || min < 0 ? amount : min;
                    }
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    transactionsFailed++;
                }                
            }

            avg /= statementsMade;           
            System.out.println("- transactions failed: " + transactionsFailed + "/" + noTransactions);
            System.out.println("- total statements benchmarked (committed or not): " + statementsMade + "/" + noTransactions*noStatementsPerTransaction);
            System.out.println("- AVG time: " + avg + " μs");
            System.out.println("- Worst time: " + max + " μs");
            System.out.println("- Best time: " + min + " μs");

        } catch (SQLException e) {
            System.err.println("INSERT statements benchmarking failed.");
            throw e;
        } finally {
            conn.setAutoCommit(true);            
        }
    }

    // Version with SELECT transactions
    private void executesSelectBenchmarks(int noTransactions, int noStatementsPerTransaction) throws SQLException {
        float avg = 0, max = -1, min = -1;
        int statementsMade = 0;
        int transactionsFailed = 0;  
        final Random rng = new Random();
        
        System.out.println("\n# Benchmark with " + noTransactions + " transactions and " + 
                noStatementsPerTransaction + " INSERT statements per transaction:");

        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM Student WHERE ID = ?")) {
            conn.setAutoCommit(false);
            for (int i = 0; i < noTransactions; i++) {
                // Single transaction
                try {
                    for (int j = 0; j < noStatementsPerTransaction; j++) {
                        final int userIndex = rng.nextInt(noStatementsPerTransaction * noTransactions);
                        stmt.setInt(1, userIndex);                      

                        long start = System.nanoTime();
                        stmt.executeQuery();
                        long end = System.nanoTime();
                        float amount = (end - start) / 1000F;                       

                        statementsMade++;
                        avg += amount;
                        max = amount > max ? amount : max;
                        min = amount < min || min < 0 ? amount : min;
                    }
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    transactionsFailed++;
                }                
            }

            avg /= statementsMade;            
            System.out.println("- transactions failed: " + transactionsFailed + "/" + noTransactions);
            System.out.println("- total statements benchmarked (committed or not): " + statementsMade + "/" + noTransactions*noStatementsPerTransaction);
            System.out.println("- AVG time: " + avg + " μs");
            System.out.println("- Worst time: " + max + " μs");
            System.out.println("- Best time: " + min + " μs");

        } catch (SQLException e) {
            System.err.println("SELECT statements benchmarking failed.");
            throw e;
        } finally {
            conn.setAutoCommit(true);    
        }
    }

    private void executeSelectBenchmark(int noSelectStatements) throws SQLException {
        float avg = 0, max = -1, min = -1; 
        int statementsMade = 0, maxID = 0;
        Random rng = new Random();
        
        // Max ID value inserted fetching
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT MAX(ID) FROM Student");
            rs.next();                
            maxID = rs.getInt(1);
            rs.close();
        } catch (SQLException e) {
            System.err.println("Unable to fetch MAX ID value from Student table in " +
                                BENCHMARKS_DB_NAME + " DB during SELECT benchmarking.");
            throw e;
        }
        
        // Benchmarking.
        System.out.println("\n# Benchmark with " + noSelectStatements + " total SELECT statements:");
        
        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM Student WHERE ID = ?")) {    
            for (int i = 0; i < noSelectStatements; i++) {
                try {
                    final int userIndex = rng.nextInt(maxID);
                    stmt.setInt(1, userIndex);                      

                    long start = System.nanoTime();
                    stmt.executeQuery();
                    long end = System.nanoTime();
                    float amount = (end - start) / 1000F;
                    
                    statementsMade++;
                    avg += amount;
                    max = amount > max ? amount : max;
                    min = amount < min || min < 0 ? amount : min;
                } catch (SQLException e) {}
            }

            avg /= statementsMade;            
            System.out.println("- total statements benchmarked: " + statementsMade + "/" + noSelectStatements);
            System.out.println("- AVG time: " + avg + " μs");
            System.out.println("- Worst time: " + max + " μs");
            System.out.println("- Best time: " + min + " μs");

        } catch (SQLException e) {
            System.err.println("SELECT statements benchmarking failed.");
            throw e;
        }
    }   


    /**
     * Creates a DB with the given name on the PostgreSQL server managed.
     * @param dbName: name of the database to create.
     * @throws SQLException: whether PostgreSQL server access or database creation fail.
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
     * Drops the DB with the given name on the PostgreSQL server managed.
     * @param dbName: name of the database to drop.
     * @throws SQLException: whether PostgreSQL server access or database dropping fail.
     */
    public void dropDB(String dbName) throws SQLException {       
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://" + dbmsUrl + "/", dbmsUser, userPw)) {            
            if (dbName.equals(connectedDB)) disconnectFromDB();            

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP DATABASE " + dbName);
                System.out.println("\nDatabase " + dbName + " dropped.");
            } catch (SQLException e) {
                System.err.println("\nDatabase " + dbName + " dropping failed.");
                throw e;
            }                     
        } catch (SQLException e) {            
            throw e;    
        }
    }

    /**
     * Connects to the DB with the given name on the PostgreSQL server managed.
     * One connection to one single DB is managed at time (existing connections close).
     * @param dbName: name of the database to connect to.
     * @throws SQLException: whether the new DB connection opening (to given DB name) or existing DB
     *                       connection closing (on managed PostgreSQL server) fail.
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
     * @throws SQLException: whether existing DB connection closing (on managed PostgreSQL server) fail.
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
