package org.dbutility;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SQLBase {

	protected List<String> sqlList = new ArrayList<String>();
	private Map<String, Connection> connMap = new HashMap<String, Connection>();
	protected Properties config = null;
	protected String configFile = null;
	protected String sqlFile = null;

	protected void loadConfig(String configFile) {
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(configFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.configFile = configFile;
		config = props;
	}

	public String formatTime(long dt) {
		
		StringBuffer sb = new StringBuffer();
		
		if (dt < 60000) {
			sb.append(dt / 1000).append('.').append(dt % 1000).append("s");
		}
		else {
			dt /= 1000;
			
			int s = (int)dt % 60;
			dt /= 60;
			int m = (int)dt % 60;
			dt /= 60;
			int h = (int)dt % 60;
			
			if (h > 0) sb.append(h).append("h, ");
			if (h > 0 || m > 0) sb.append(m).append("m, ");
			sb.append(s).append("s");
		}
		
		return sb.toString();
	}

	protected void readFile() throws Exception {
		System.out.println("Reading from file " + config.getProperty("sqlfile"));
		sqlFile = config.getProperty("sqlfile");
		StringBuilder sb = readFile("sql/" + config.getProperty("sqlfile"));
		int pos = sb.indexOf(";");
		int count = 0;
		int start = 0;
		while(pos >= 0) {
			String sql = sb.substring(start, pos);
			sqlList.add(sql);
			start = pos + 1;
			pos = sb.indexOf(";", pos + 1);
			count++;
		}
		
		System.out.println("Loaded " + count + " SQL");
	}

	protected StringBuilder readFile(String fileName) {
		
		BufferedReader reader = null;
		StringBuilder sb = new StringBuilder();
		
		try {
			reader = new BufferedReader(new FileReader(fileName));
	
			String line = null;
			while((line = reader.readLine()) != null) {
				// skip comments
				if(line.trim().startsWith("--")) {
					continue;
				}
				if(line.indexOf("--") > 0) {
					line = line.substring(0, line.indexOf("--"));
				}
				sb.append(line).append(" ");
			}
		
			reader.close();
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		return sb;
	}
	
	public Connection getConnection() {
		return connMap.get("0");
	}

	protected void commit() throws SQLException {
		Iterator<Map.Entry<String, Connection>> it = connMap.entrySet()
				.iterator();
		while (it.hasNext()) {
			Connection conn = it.next().getValue();
			conn.commit();
		}
	}

	protected void rollback() throws SQLException {
		Iterator<Map.Entry<String, Connection>> it = connMap.entrySet()
				.iterator();
		while (it.hasNext()) {
			Connection conn = it.next().getValue();
			conn.rollback();
		}
	}
	
	public void commitOrRollback() throws SQLException {
		String commitdbchanges = null;
		boolean isCommit = false;
		if(commitdbchanges == null) {
			commitdbchanges = config.getProperty("commitdbchanges");
			if("true".equalsIgnoreCase(commitdbchanges)) {
				isCommit = true;
			}
			System.out.println("commitdbchanges is " + isCommit);
		}
		if(isCommit) {
			commit();
		} else {
			rollback();
		}
	}
	
	protected void createConnections() throws Exception {
		System.out.println("Adding connections to map");
		String schema1 = null;
		String schema2 = null;
		
		schema1 = config.getProperty("schema1");
		schema2 = config.getProperty("schema2");
		
		// if no schema specified, do not use prefix to look up connection string
		if(schema1 == null || schema2 == null) {
			Connection conn = createConnection(null);
			String schema = "" + 0;
			connMap.put(schema, conn);
		} else {
			Connection conn1 = createConnection(schema1);
			connMap.put(schema1, conn1);
			
			Connection conn2 = createConnection(schema2);
			connMap.put(schema2, conn2);
		}
	}

	protected Connection createConnection(String schema)  {
	    
		
		if(schema != null) {
			System.out.println("Creating connection for schema - " + schema);
		} else {
			System.out.println("Creating connection");
		}
        
		String url = null;
        String userid = null;
        String passwd = null;
        String driver = null;
        
		if(schema == null) {
	        url = config.getProperty("url");
	        userid = config.getProperty("userid");
	        passwd = config.getProperty("passwd");
	        driver = config.getProperty("driver");
		} else {
			url = config.getProperty(schema + ".url");
	        userid = config.getProperty(schema + ".userid");
	        passwd = config.getProperty(schema + ".passwd");
	        driver = config.getProperty(schema + ".driver");
		}
        
        System.out.println("url - " + url + ", user - " + userid);
        
        try {
        	Class.forName(driver);
        } catch(ClassNotFoundException e) {
        	System.out.println("Error loading database driver " + driver + ". " +
			"Make sure to put JDBC driver jar in classpath.");
        	e.printStackTrace();
        	throw new RuntimeException(e);
        }
        
        Connection conn = null;
        
		try {
			conn = DriverManager.getConnection(url, userid, passwd);
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			System.out.println("");
			e.printStackTrace();
		}

        return conn;
	}
}
