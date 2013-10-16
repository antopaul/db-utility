package org.dbutility.sqlfile;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.dbutility.SQLBase;

public class SQLExecuteFromFile extends SQLBase {

	private static String CONFIG_FILE = "config/SQLExecuteFromFile.properties";
	
	protected static String CURRENT_SQL = null; 
	
	protected static int EXIT_STATUS = 1;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SQLExecuteFromFile cmd = new SQLExecuteFromFile();
		cmd.execute();
	}
	
	protected void execute() {
		try {
			loadConfig(CONFIG_FILE);
			addShutdownHook();
			createConnections();
			for(int i=0; i < sqlFileList.size(); i++) {
				readFile(sqlFileList.get(i));
			if(sqlList.size() == 0) {
					log("No SQL to execute for file " + sqlFile);
				return;
			}
			executeSQL();
			}
			
			EXIT_STATUS = 0;
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			try {
				rollback();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	protected void executeSQL() throws Exception {
		log("Executing sql from file " + sqlFile);

		int count = 0;
		long overall = 0;
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			for (String sqlStr : sqlList) {
				long start = System.currentTimeMillis();
				
				String sql = sqlStr;
				CURRENT_SQL = sql;
				if("true".equalsIgnoreCase(config.getProperty("printsql"))) {
					System.out.print(count + 1 + " - " + sql);
				}

				int c = stmt.executeUpdate(sql);
				count++;
				commitEvery(count);
				
				long end = System.currentTimeMillis();
				
				long time = end - start;
				overall += time;
				
				if("true".equalsIgnoreCase(config.getProperty("printsql"))) {
					System.out.println(", updated - " + c + " records, took " + time + "ms");
				}
				
			}
			commitOrRollback();
		} catch(SQLException e) {
			log("Error executing SQL - " + CURRENT_SQL);
			throw new RuntimeException(e);
		} finally {
			stmt.close();
		}
		log("Finished executing SQL in " + formatTime(overall));
	}
	
	protected void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

			public void run() {
				if(EXIT_STATUS == 1) {
					log("Current SQL " + CURRENT_SQL);
				}
				
			}}) );
	}

}
