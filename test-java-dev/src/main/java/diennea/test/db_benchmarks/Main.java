package diennea.test.db_benchmarks;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {

    private static String dbmsUrl;
    private static String dbmsUser;
    private static String userPw;

    public static void main(String[] args) throws ClassNotFoundException {

        loadConfig();        
        new PostgreSQLManager(dbmsUrl, dbmsUser, userPw).executesBenchmarks();
        
    }

    private static void loadConfig() {
        Properties prop = new Properties();		

        try (InputStream input = Main.class.getResourceAsStream("/configuration.properties")) {

            System.out.println("Configuration loading...");
            // load a properties file
            prop.load(input);		    

            // get the property value and print it out
            dbmsUrl = prop.getProperty("dbms_url");
            System.out.println("dbms_url: " + dbmsUrl);

            dbmsUser = prop.getProperty("dbms_user");
            System.out.println("dbms_user: " + dbmsUser);

            userPw = prop.getProperty("user_pw");    
            System.out.println("user_pw: " + userPw + "\n");         


        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
