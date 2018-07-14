package diennea.test.db_benchmarks;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Main {

	private static String dbmsUrl;
	private static String dbmsUser;
	private static String userPw;

	private static String dbName = "dienneaTestDB";

	public static void main(String[] args) throws ClassNotFoundException {
		loadConfig();
		Class.forName("org.postgresql.Driver");
		try(Connection conn = DriverManager.getConnection("jdbc:postgresql://" + dbmsUrl + "/", dbmsUser, userPw)) {

			createTestDB(conn);

			dropTestDB(conn);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}

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

	private static void createTestDB(Connection conn) throws SQLException {				
		try (Statement stmt = conn.createStatement()) {						
			stmt.executeUpdate("CREATE DATABASE " + dbName);
			System.out.println("Database " + dbName + " created.");
		} catch (SQLException e) { 
			System.err.println("Database " + dbName + " creation failed.");
			throw e;
		}
	}

	private static void dropTestDB(Connection conn) {		 
		try (Statement stmt = conn.createStatement()) {					
			stmt.executeUpdate("DROP DATABASE " + dbName);
			System.out.println("Database " + dbName +" dropped.");
		} catch (SQLException e) {
			System.err.println("Database " + dbName + " dropping failed.");
			System.out.println(e);
		}
	}
}
