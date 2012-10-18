package org.dbutility.sqlfile;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.dbutility.SQLBase;

public class SQLSelectFromFile extends SQLBase {

	private Map<Integer, Long> timing = new HashMap<Integer, Long>();
	
	private static String CONFIG_FILE = "config/SQLSelectFromFile.properties";
	
	protected static String CURRENT_SQL = null; 
	
	protected static int EXIT_STATUS = 1;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		SQLSelectFromFile cmd = new SQLSelectFromFile();
		cmd.execute();
	}
	
	protected void execute() {
		try {
			loadConfig(CONFIG_FILE);
			addShutdownHook();
			// TODO execute multiple SQL files
			readFile();
			createConnections();
			executeSQL();
			EXIT_STATUS = 0;
			printThresholdExceeded();
			timing = sortByValue(timing);
			printTop5();
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
	
	protected void printThresholdExceeded() {
		int threshold = Integer.parseInt(config.getProperty("threshold", "-1"));
		if(threshold == -1) {
			return;
		}
		
		for(Map.Entry<Integer, Long> entry : timing.entrySet()) {
			if(entry.getValue() >= threshold) {
				System.out.println("Threshold exceeded for index " + 
						(entry.getKey() + 1) + ", Time taken - " + entry.getValue() 
						+ "ms, SQL - " + sqlList.get(entry.getKey()));
			}
		}
	}
	
	protected void printTop5() {
		int printtopcount = Integer.parseInt(config.getProperty("printtopcount"));
		if(printtopcount == -1) {
			return;
		}
		System.out.println("Printing top " + printtopcount);
		int count = 0;
		for(Map.Entry<Integer, Long> entry : timing.entrySet()) {
			Integer index = entry.getKey();
			Long time = entry.getValue();
			String sql = sqlList.get(index);
			System.out.println("Index - " + index + ", Time - " + time + " ,SQL - " + sql);
			if(++count >= printtopcount) {
				break;
			}
		}
	}
	
	protected void executeSQL() {
		System.out.println("Executing SELECT sql from file " + sqlFile);

		int count = 0;
		long overall = 0;
		
		try {
			for (String sqlStr : sqlList) {
				String sql = sqlStr;
				CURRENT_SQL = sql;
				if("true".equalsIgnoreCase(config.getProperty("printsql"))) {
					System.out.print(count + 1 + " - " + sql);
				}
				Connection conn = getConnection();
				Statement stmt = conn.createStatement();
				long start = System.currentTimeMillis();
				ResultSet rs = stmt.executeQuery(sql);
				while(rs.next()) {
					rs.getString(1);
				}
				long end = System.currentTimeMillis();
				rs.close();
				stmt.close();
				
				long time = end - start;
				overall += time;
				timing.put(count, time);
				count++;
				
				if("true".equalsIgnoreCase(config.getProperty("printSQL"))) {
					System.out.println(" - took " + time + "ms");
				}
				
			}
			commitOrRollback();
		} catch(SQLException e) {
			System.out.println("Error executing SQL - " + CURRENT_SQL);
			throw new RuntimeException(e);
		}
		System.out.println("Finished executing SQL in " + formatTime(overall));
	}
	
	protected Map<Integer, Long> sortByValue(Map<Integer, Long> map) {
		Map<Integer, Long> sortedMap = new LinkedHashMap<Integer, Long>();
		
		SortedSet<Map.Entry<Integer, Long>> sSet = 
			new TreeSet<Map.Entry<Integer, Long>>(new Comparator<Map.Entry<Integer, Long>>() {

				public int compare(Map.Entry<Integer, Long> o1, Map.Entry<Integer, Long> o2) {
					// it is comparing in reverse.
					int  a = o2.getValue().compareTo(o1.getValue());
					if(a == 0) {
						a = 1;
					}
					return a;
				}});
		sSet.addAll(map.entrySet());

		for(Map.Entry<Integer, Long> e : sSet) {
			sortedMap.put(e.getKey(), e.getValue());
		}
		
		return sortedMap;
	}
	
	protected void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

			public void run() {
				if(EXIT_STATUS == 1) {
					System.out.println("Current SQL " + CURRENT_SQL);
				}
				
			}}) );
	}

}
