package org.dbutility.exportasinsert;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import org.dbutility.SQLBase;

public class ExportAsInsertsOracle extends SQLBase {

	private static String CONFIG_FILE = "config/ExportAsInserts.properties";
	
	protected static int EXIT_STATUS = 1;
	
	protected static String TABLE = "table";
	
	protected static String EXPORT_SQL = "sql";
	
	protected Connection sourceConn = null;
	
	protected Statement sourceStmt = null;
	
	ResultSet sourceRs = null;
	
	protected List<Integer> metaData = null;
	
	protected List<String> columnNames = null;
	
	SimpleDateFormat dateSdf = new SimpleDateFormat("dd-MMM-yyyy"); 
	SimpleDateFormat tsSdf = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		ExportAsInsertsOracle cmd = new ExportAsInsertsOracle();
		cmd.execute();
	}
	
	protected void execute() {
		try {
			loadConfig(CONFIG_FILE);
			createConnections();
			String tableName = config.getProperty(TABLE);
			String sql = config.getProperty(EXPORT_SQL);
			
			if(tableName != null && sql == null) {
				getTableData(tableName);
			} else if(tableName != null && sql != null) {
				getSqlData(sql);
			} else if(tableName == null && sql != null){
				log("When using sql, table name must be specified");
				return;
			} else if(tableName == null && sql == null) {
				log("Please specify table or table and sql");
				return;
			}
			
			metaData = loadMetadata(sourceRs);
			generateInserts(tableName.toUpperCase(), columnNames, sourceRs);
			EXIT_STATUS = 0;

		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			try {
				rollback();
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(sourceConn != null) {
				try { sourceConn.close(); } catch (SQLException e) { }
			}
		}
	}
	
	protected void generateInserts(String tableName, List<String> columnNames, ResultSet rs) throws SQLException {
		StringBuilder insertSql = new StringBuilder();
		
		insertSql.append("INSERT INTO ").append(tableName);
		insertSql.append("(");
		boolean isFirst = true;
		for(String column : columnNames) {
			if(!isFirst) {
				insertSql.append(",");
			}
			insertSql.append(column);
			isFirst = false;
		}
		
		insertSql.append(")").append(" VALUES("); //.append(values).append(")");
		try {
			while(rs.next()) {
				StringBuilder values = new StringBuilder();
				int index = 1;
				isFirst = true;
				for(Integer type : metaData) {
					if(!isFirst) {
						values.append(",");
					}
					switch (type) {
					case Types.DATE:
						Date d = rs.getDate(index);
						String dateStr = "";
						if(d != null) {
							String s = dateSdf.format(d);
							dateStr = "to_date('" + s + "','dd-MON-yyyy')";  
						} else {
							values.append(dateStr);
						}
						break;
					case Types.TIMESTAMP:
						Timestamp ts = rs.getTimestamp(index);
						String tsStr = "";
						if(ts != null) {
							String s = tsSdf.format(ts);
							tsStr = "to_timestamp('" + s + "','dd-MON-yyyy HH24:MI:SS.FF')";
							values.append(tsStr);
						} else {
							values.append("'").append(tsStr).append("'");
						}
						break;
					default:
						String s = rs.getString(index);
						if(s != null) {
							values.append("'").append(s).append("'");
						} else {
							values.append(s);
						}
					}
					index++;
					isFirst = false;
				}
				StringBuilder sql = new StringBuilder(insertSql);
				sql.append(values).append(");");
				System.out.println(sql);	
			}
		} finally {
			if(sourceRs != null) {
				sourceRs.close();
			}
			if(sourceStmt != null) {
				sourceStmt.close();
			}
		}
	}
	
	protected void getTableData(String tableName) {
		String sql = "SELECT * FROM " + tableName;
		getSqlData(sql);
	}
	
	protected void getSqlData(String sql) {

		try {
			sourceConn = getConnection();
			try {
				if(sourceConn instanceof oracle.jdbc.OracleConnection) {
					((oracle.jdbc.OracleConnection) sourceConn).setDefaultRowPrefetch(1000);
				}
			} catch(Exception e) {}
			sourceStmt = sourceConn.createStatement();
			sourceRs = sourceStmt.executeQuery(sql);
		} catch(SQLException sqle) {
			throw new RuntimeException(sqle);
		} 
	}

	
	protected List<Integer> loadMetadata(ResultSet rs) {
		metaData = new LinkedList<Integer>(); 
		columnNames = new LinkedList<String>();
		
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			
			for(int i=1; i <= columnCount; i++) {
				
				columnNames.add(rsmd.getColumnName(i));
				int type = rsmd.getColumnType(i);
				
				switch (type) {
				case Types.DATE:
					metaData.add(Types.DATE);
					break;
				case Types.TIMESTAMP:
					metaData.add(Types.TIMESTAMP);
					break;
				default:
					metaData.add(Types.VARCHAR);
				}
			}
 
		} catch(SQLException sqle) {
			throw new RuntimeException(sqle);
		}
		
		System.out.println("Columns are : " + columnNames);
		System.out.println("Types are : " + metaData);
		
		return metaData;
	}

}
