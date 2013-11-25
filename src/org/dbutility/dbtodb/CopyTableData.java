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

package org.dbutility.dbtodb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;

import org.dbutility.SQLBase;

public class CopyTableData extends SQLBase {
	
	private static String CONFIG_FILE = "config/CopyTableData.properties";
	
	protected static String CURRENT_SQL = null; 
	
	protected static int EXIT_STATUS = 1;
	
	protected static String DATE = "DATE";
	
	protected static String TIMESTAMP = "TIMESTAMP";
	
	protected static String STRING = "STRING";
	
	protected static String TABLE = "table";
	
	protected Connection schema1Conn = null;
	
	protected Connection schema2Conn = null;
	
	protected Statement schema1Stmt = null;
	
	ResultSet schema1Rs = null;
	
	protected List<Integer> metadata = null;
	
	protected List<String> columnNames = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CopyTableData cmd = new CopyTableData();
		cmd.execute();
	}
	
	protected void execute() {
		System.out.println("Executing CopyTableData...");
		try {
			loadConfig(CONFIG_FILE);
			createConnections();
			String tableName = config.getProperty(TABLE);
			getTableData(tableName);
			loadMetadata(schema1Rs);
			insertData(tableName);
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
	
	protected void insertData(String tableName) {
		StringBuilder sql = new StringBuilder();
		StringBuilder values = new StringBuilder();
		sql.append("INSERT INTO ").append(tableName);
		sql.append("(");
		boolean isFirst = true;
		for(String column : columnNames) {
			if(!isFirst) {
				sql.append(",");
				values.append(",");
				
			}
			sql.append(column);
			values.append("?");
			isFirst = false;
		}
		
		sql.append(")").append(" VALUES(").append(values).append(")");
		
		System.out.println("Insert SQL " + sql);
		
		Connection schema2Conn = getSchema2Connection();
		PreparedStatement pstmt = null; 
			
		try {
			pstmt = schema2Conn.prepareStatement(sql.toString());
			
			while(schema1Rs.next()) {
				int index = 1;
				for(Integer type : metadata) {
					switch (type) {
					case Types.DATE:
						pstmt.setDate(index, schema1Rs.getDate(index));
						break;
					case Types.TIMESTAMP:
						pstmt.setTimestamp(index, schema1Rs.getTimestamp(index));
						break;
					default:
						pstmt.setString(index, schema1Rs.getString(index));
					}
					index++;
				}
				
				pstmt.addBatch();
			}
			
			int[] updateCount = pstmt.executeBatch();
			
			System.out.println("Inserted " + updateCount.length + " records");
			
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				commitOrRollback();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

	}
	
	protected void getTableData(String tableName) {

		try {
			schema1Conn = getSchema1Connection();
			schema1Stmt = schema1Conn.createStatement();
			schema1Rs = schema1Stmt.executeQuery("SELECT * FROM " + tableName);
		} catch(SQLException sqle) {
			throw new RuntimeException(sqle);
		} 
	}
	
	protected List<Integer> loadMetadata(ResultSet rs) {
		metadata = new LinkedList<Integer>(); 
		columnNames = new LinkedList<String>();
		
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			
			for(int i=1; i <= columnCount; i++) {
				
				columnNames.add(rsmd.getColumnName(i));
				int type = rsmd.getColumnType(i);
				
				switch (type) {
				case Types.DATE:
					metadata.add(Types.DATE);
					break;
				case Types.TIMESTAMP:
					metadata.add(Types.TIMESTAMP);
					break;
				default:
					metadata.add(Types.VARCHAR);
				}
			}
 
		} catch(SQLException sqle) {
			throw new RuntimeException(sqle);
		}
		
		System.out.println("Columns are : " + columnNames);
		System.out.println("Types are : " + metadata);
		
		return metadata;
	}
}
