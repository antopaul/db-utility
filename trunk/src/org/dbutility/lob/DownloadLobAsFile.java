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

package org.dbutility.lob;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.dbutility.SQLBase;

/**
 * This class is to download a blob/clob from database and save it as a file
 * in specified location. Blob should be retrieved by the query.
 * 
 * @author Anto Paul 
 *
 */
public class DownloadLobAsFile extends SQLBase {
	
	private static String CONFIG_FILE = "config/DownloadLobAsFile.properties";
	
	protected String sql = null;
	protected String fileName = null;
	protected String location = null;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		DownloadLobAsFile cmd = new DownloadLobAsFile();
		cmd.execute();
	}
	
	protected void execute() {
		
		try {
			loadConfig(CONFIG_FILE);
			createConnections();
			readFile(sqlFileList.get(0));
			downloadAndSave();
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
	
	protected void downloadAndSave() {
		
		System.out.println("Downloading and saving file");
		
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Blob blob = null;
		
		sql = sqlList.get(0);
		
		try {
			conn = getConnection();
			stmt = conn.prepareStatement(sql);
			
			rs = stmt.executeQuery();
			
			if(rs.next()) {
				blob = rs.getBlob(1);
			} else {
				System.out.println("No blob found. Please check the query.");
			}
			
			if(blob != null && blob.length() > 0) {
				writeFile(blob.getBinaryStream());
			}
			
		} catch(SQLException sqle) {
			System.out.println("Error executing SQL " + sql + " - " + sqle.getMessage());
			throw new IllegalStateException(sqle);
		}
	}
	
	protected void writeFile(InputStream is) {
		
		System.out.println("Writing file");
				
		fileName = config.getProperty("filename");
		location = config.getProperty("location");
		
		String newfile = location + System.getProperty("file.separator") + fileName;
		
		File file = new File(newfile);
		
		try {
			FileOutputStream fos = new FileOutputStream(file);
			byte[] b = new byte[1024 * 1024];
			int count = -1;
			
			while((count = is.read(b)) != -1) {
				fos.write(b, 0, count);
			}
			
			fos.close();
			
			System.out.println("Downloaded file " + newfile);

		} catch(FileNotFoundException fne) {
			System.out.println("File not found - " + fne.getMessage());
			throw new IllegalStateException(fne);
		} catch(IOException ioe) {
			System.out.println("Error writing file " + ioe.getMessage());
			throw new IllegalStateException(ioe);
		}
	}

}
