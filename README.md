Small Java utility project for various tasks like executing SQL from a file, download LOB field from database, copy one table from one database to another database.

These classes are very lightweight and do not have any dependency on JAR files other than JDBC driver. This is designed with the purpose of executing it in the server which is close to the database server so that network speed will not be an issue. Even though I have not tested it with other databases than Oracle, it should be able to work with multiple vendor databases.

### Execute SQL from a file ###
This can be used to execute SQL from a file. Currently two class files are available. One for executing SELECT statements and another one for executing INSERT/UPDATE/DELETE statements.

SQLSelectFromFile is useful to find out which SQL is taking time to execute. This class can read SQL from multiple files and execute them. After execution it prints 5 SQL which took most time to execute. Also it has a threshold configuration which specified the maximum time an SQL can take. On exceeding this threshold, it will print SQL in console.

SQLExecuteFromFile is to execute any SQL. This is particularly useful for executing INSERT/UPDATE/DELETE statements in bulk.

### Download LOB from a table ###
DownloadLobAsFile is for downloading one LOB record(CLOB/BLOB) from a table. This uses a query to decide which record to download. This is useful when files are stored in a table like PDF, images etc. It is even possible to configure different database vendors like MS SQL Server and Oracle as source and target databases.

### Copy table data from one table to other ###
CopyTableData is to copy data from one table in one database to another database. This cannot copy LOB columns. The table should be same name in both databases(TODO make table name configurable in target database).
