PostgreSQL DB Benchmarks


### Overview ###
This project aims to execute a baunch of benchmarks about INSERT and SELECT statement requests onto
a PostgreSQL temporary database using Java and JDBC.

All benchmarks are executed using a dedicated class (PostgreSQLManager) which manages a 
PostgreSQL server instance (specified by the dbms_url, a user and his password for the authentication
in the configuration.properties file).
The PostgreSQLManager, in addition to the db benckmarks, allows to create, drop and connect
to a database (onto the specified psql server) as well as create tables into a connected DB.
All the benchmarks are executed onto the specified psql server and to a temporary database create for
the purpose ("diennea_benchmarks_test_db"). Even the table used for the benchmarks, whose DDL script
is located in src/main/resources/benchmarks_db_table.sql, is automatically created by the app.


### Benchmarks Notes ###
Following types of benchmark are executed:
 - INSERT benchmark: consists of no_transactions* transactions with no_statements_per_transaction* INSERT
 	prepared-statements.
 	Whenever an INSERT statement fails the entire transaction is discarded and the
	next transaction is executed (successfully executed INSERT statements of a discarded transaction are
	still benchmarked).
 
 - SELECT benckmark: consists of no_select_statements* SELECT prepared-statements on the table PK.
 	Each failing SELECT statement is not benchmarked.


### Setup ###
The src/main/resources/configuration.properties file contains all the configuration properties for
the project execution:

# PostgreSQL server access config
- dbms_url = <psql_server_url[:port]>
- dbms_user = <psql_user_for_server_auth>
- user_pw = <psql_user_password_for_server_auth>

# Benchmarks config
- no_transactions = <number_of_transaction_to_execute_during_INSERT_benchmark>
- no_statements_per_transaction = <number_of_statements_per_transaction_to_execute_during_INSERT_benchmark>
- no_select_statements = <number_of_SELECT_statements_to_execute_during_SELECT_benchmark>