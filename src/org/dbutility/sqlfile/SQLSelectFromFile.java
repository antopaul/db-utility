/*
 * Copyright 2013 Anto Paul
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.List;
import java.util.ArrayList;

import org.dbutility.SQLBase;


/**
 * Class to execute multiple SELECT statements. Each statement should end with semicolon(;).
 * Multiple statements can be specified. Useful in testing timing of SQL. It has option to
 * print n number of slowest and fastest SQL. Also a threshold can be set in milliseconds,
 * SQL exceeding this threshold is printed to console. 
 */
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
			// Never commit
			config.setProperty("commitdbchanges","false");
			addShutdownHook();
			createConnections();
			for(int i=0; i < sqlFileList.size(); i++) {
				readFile(sqlFileList.get(i));
			if(sqlList.size() == 0) {
					System.out.println("No SQL to execute for file " + sqlFile);
				return;
			}
				
			executeSQL();
			}
			EXIT_STATUS = 0;
			printThresholdExceeded();
			timing = sortByValue(timing);
			printSlowest();
			printFastest();
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
	
	protected void printSlowest() {
		int printtopslowestcount = Integer.parseInt(config.getProperty("printtopslowestcount"));
		if(printtopslowestcount == -1) {
			return;
		}
		System.out.println("Printing slowest top " + printtopslowestcount);
		int count = 0;
		for(Map.Entry<Integer, Long> entry : timing.entrySet()) {
			Integer index = entry.getKey();
			Long time = entry.getValue();
			String sql = sqlList.get(index);
			System.out.println("Index - " + index + ", Time - " + time + " ,SQL - " + sql);
			if(++count >= printtopslowestcount) {
				break;
			}
		}
	}
	
	protected void printFastest() {
		int printtopfastestcount = Integer.parseInt(config.getProperty("printtopfastestcount"));
		
		if(printtopfastestcount == -1) {
			return;
		}
		System.out.println("Printing fastest top " + printtopfastestcount);
		int count = 0;
		List<Map.Entry<Integer, Long>> list = new ArrayList(timing.entrySet());
		for(int i = list.size() -1; i >= 0; i--) {
			Map.Entry<Integer, Long> entry = list.get(i);
			Integer index = entry.getKey();
			Long time = entry.getValue();
			String sql = sqlList.get(index);
			System.out.println("Index - " + index + ", Time - " + time + " ,SQL - " + sql);
			if(++count >= printtopfastestcount) {
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
				
				if("true".equalsIgnoreCase(config.getProperty("printsql"))) {
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
