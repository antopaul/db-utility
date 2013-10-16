package org.dbutility;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SQLBase {

	protected List<String> sqlList = new ArrayList<String>();
	private Map<String, Connection> connMap = new HashMap<String, Connection>();
	protected Properties config = null;
	protected String configFile = null;
	protected List<String> sqlFileList = new LinkedList<String>();
	protected String sqlFile = null;
	protected static String DEFAULT_CONN = "0";
	protected static String SCHEMA1_CONN = "SCHEMA1";
	protected static String SCHEMA2_CONN = "SCHEMA2";
	protected static String FILE_SEPERATOR = ",";
	
	protected DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss SSS");

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
		loadSqlFileList();
	}
	
	protected void loadSqlFileList() {
		String filenames = config.getProperty("sqlfile");
		if(filenames == null) {
			log("No sqlfile specified.");
			return;
		}
		if(filenames.indexOf(FILE_SEPERATOR) == -1) {
			sqlFileList.add(filenames);
		} else {
			String[] names = filenames.split(FILE_SEPERATOR);
			sqlFileList.addAll(Arrays.asList(names));
		}
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

	protected void readFile(String file) throws Exception {
		log("Reading from file " + file);
		sqlFile = file;
		sqlList.clear();
		StringBuilder sb = loadFile("sql/" + sqlFile);
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
		if(sqlList.size() == 0) {
			log("No SQL loaded. Make sure that SQL is ended with ;");
		}
		log("Loaded " + count + " SQL");
	}

	protected StringBuilder loadFile(String fileName) {
		
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
		return connMap.get(DEFAULT_CONN);
	}
	
	public Connection getSchema1Connection() {
		return connMap.get("SCHEMA1_CONN");
	}
	
	public Connection getSchema2Connection() {
		return connMap.get("SCHEMA2_CONN");
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
			log("commitdbchanges is " + isCommit);
		}
		if(isCommit) {
			commit();
		} else {
			rollback();
		}
	}
	
	protected void createConnections() throws Exception {
		log("Adding connections to map");
		String schema1 = null;
		String schema2 = null;
		
		schema1 = config.getProperty("schema1");
		schema2 = config.getProperty("schema2");
		
		// if no schema specified, do not use prefix to look up connection string
		if(schema1 == null || schema2 == null) {
			Connection conn = createConnection(null);
			String schema = DEFAULT_CONN;
			connMap.put(schema, conn);
		} else {
			Connection conn1 = createConnection(schema1);
			connMap.put("SCHEMA1_CONN", conn1);
			
			Connection conn2 = createConnection(schema2);
			connMap.put("SCHEMA2_CONN", conn2);
		}
	}

	protected Connection createConnection(String schema)  {
	    
		boolean isOk = true;
		
		if(schema != null) {
			log("Creating connection for schema - " + schema);
		} else {
			log("Creating connection");
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
		
		if(url == null) {
			log("Please configure url property.");
			isOk = false;
		}
		if(userid == null) {
			log("Please configure userid property.");
			isOk = false;
		}
		if(passwd == null) {
			log("Please configure passwd property.");
			isOk = false;
		}
		if(driver == null) {
			log("Please configure driver property.");
			isOk = false;
		}
        
		if(!isOk) {
			System.exit(1);
		}
			
        log("url - " + url + ", user - " + userid);
        
        try {
        	Class.forName(driver);
        } catch(ClassNotFoundException e) {
        	log("Error loading database driver " + driver + ". " +
			"Make sure to put JDBC driver jar in classpath.");
        	e.printStackTrace();
        	throw new RuntimeException(e);
        }
        
        Connection conn = null;
        
		try {
			conn = DriverManager.getConnection(url, userid, passwd);
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			log("");
			e.printStackTrace();
		}

        return conn;
	}
	
	public void log(String message) {
		System.out.println(logDate() + " " + message);
	}
	
	public String logDate() {
		String dateStr = null;
		Date date = new Date();
		dateStr = dateFormat.format(date);
		return dateStr;
	}
}
