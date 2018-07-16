DB Benchmarks

This project leads to execute a bounch of benchmarks about DML request to a PostgreSQL (9) db via Java and related JDBC driver.

# Main requirements below:
	- You have to implement simple java application which executes benchmarks on a JDBC compliant database.
   	- The application MUST evaluate the min/max/avg time of INSERT statements.
   	- The application MUST evaluate the min/max/avg time of SELECT statements (using the PK of the COLUMN).
   	- The application MUST issue DML requests using the PreparedStatement API and issuing “commits” every X statements.
   	- The application will be run on a PostGRE or MS SQLServer database.
   	- The script to create the table MUST be included in the source code.


# Setup
The file "configuration.properties" contains all fields to setup before app execution:
	- 



TODO
The project MUST contain all files needed for configuration, usually it is better to have a README.txt file which explains the project and the configuration. Usually the configuration will reside in a src/main/resources/configuration.properties file (but this is not required)
