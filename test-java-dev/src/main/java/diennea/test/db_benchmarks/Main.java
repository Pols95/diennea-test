package diennea.test.db_benchmarks;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {

    // PostgreSQL server access config
    private static String dbmsUrl;
    private static String dbmsUser;
    private static String userPw;
    
    // Benchmarks config
    private static int noTransactions;
    private static int noStatementsPerTransaction;
    private static int noSelectStatements;

    
    public static void main(String[] args) throws ClassNotFoundException {
        loadConfig();        
        new PostgreSQLManager(dbmsUrl, dbmsUser, userPw).executesBenchmarks(noTransactions, noStatementsPerTransaction, noSelectStatements);        
    }

    private static void loadConfig() {
        final Properties prop = new Properties();		

        try (InputStream input = Main.class.getResourceAsStream("/configuration.properties")) {
            
            prop.load(input);
            System.out.println("Configuration loaded:");           

            // PostgreSQL server access config
            dbmsUrl = prop.getProperty("dbms_url");
            System.out.println("- dbms_url: " + dbmsUrl);

            dbmsUser = prop.getProperty("dbms_user");
            System.out.println("- dbms_user: " + dbmsUser);

            userPw = prop.getProperty("user_pw");    
            System.out.println("- user_pw: " + userPw);
            
            
            // Benchmarks config
            noTransactions = Integer.parseInt(prop.getProperty("no_transactions"));
            System.out.println("- no_transactions: " + noTransactions);

            noStatementsPerTransaction = Integer.parseInt(prop.getProperty("no_statements_per_transaction"));    
            System.out.println("- no_statements_per_transaction: " + noStatementsPerTransaction);
            
            noSelectStatements = Integer.parseInt(prop.getProperty("no_select_statements"));    
            System.out.println("- no_select_statements: " + noSelectStatements + "\n");
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
