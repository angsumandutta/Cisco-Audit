package com.databuck.processCiscojobs;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
//import java.util.Date;
import java.sql.Connection;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.springframework.beans.factory.annotation.Autowired;


public class ProcessCiscoJobs {
	
	private String postgrs_host = null;
	private String postgrs_port = null;
	private String postgrs_dbName =null;
	private String postgrs_userName = null;
	private String postgrs_password = null;
	private String postgrs_url = null;
	private String postgrs_url2 = null;
	private String postgrs_tableName = null;
	private String postgrs_dbSchema = null;
	
	private Process process = null;
	
	private ArrayList<Integer> postgrsAuditIDs = new ArrayList<>();
	
	@Autowired
	public Properties resultDBConnectionProperties;
	
	
	Properties prop= resultDBConnectionProperties;

	ProcessCiscoJobs(){
		
		FileReader reader = null;
		try {
			reader = new FileReader("ciscodb.properties");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
			 
		 Properties prop =new Properties();  
		 try {
			prop.load(reader);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		 
		postgrs_host = prop.getProperty("postgrs_host");
		postgrs_port = prop.getProperty("postgrs_port");
		postgrs_dbName = prop.getProperty("postgrs_dbName");
		postgrs_userName = prop.getProperty("postgrs_userName");
		postgrs_password = prop.getProperty("postgrs_password");
		postgrs_tableName = prop.getProperty("postgress_tablename");
		postgrs_dbSchema = prop.getProperty("postgrs_dbSchema");
		
		postgrs_url = "jdbc:postgresql://" + postgrs_host + ":" + postgrs_port + "/" + postgrs_dbName;
		postgrs_url2 = "jdbc:postgresql://" + postgrs_host + ":" + postgrs_port;
		
		System.out.println("Postgress URL = " + postgrs_url);
		
	}
	

public int updateAuditStatusInPostgresTable(Connection con, ArrayList<Integer> AuditIDs) {
	int numRecordsUpdated = 0;
	
	if (AuditIDs.size() > 0 ) {
		try {
			String strAuditIds = AuditIDs.toString().replace('[', ' ').replace(']',' ');
			String sqlQueryUpdate = "UPDATE " + postgrs_dbSchema + "." + postgrs_tableName + " SET audit_ready = 'N' where audit_pk_id in(" + strAuditIds + ")";
			System.out.println("Postgres update query = " + sqlQueryUpdate);
			Statement stmtUpdate = con.createStatement();
			
			numRecordsUpdated = stmtUpdate.executeUpdate(sqlQueryUpdate);
			if ( numRecordsUpdated > 0) {
				System.out.println("Postgres update completed. Number of records updated = " + numRecordsUpdated);
			}
		    
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	return numRecordsUpdated;
}

public boolean runCommand(String command, boolean waitForCompletion) throws Exception {
	
	process = null;
	boolean runStatus = false;
	
	try {
		System.out.println("Command to execute : " + command);
		process = Runtime.getRuntime().exec(command);
		System.out.println("");
		if(waitForCompletion) {
			process.waitFor(); //wait for script to execute 
		}
		runStatus = true;
	} catch (Exception e1) {
		System.out.println("\n====>Exception occurred when triggering script !!!");
		System.out.println("Command execution failed, command : " + command);
		runStatus = false;
		e1.printStackTrace();
	}
	return runStatus;
}


public int getPostTargetTableCount(String tableName) {
	
	Connection connection = null;
	String post_dbName = null;
	
	int recCount = 0;		
	String[] strNames;
	
	strNames = tableName.split("\\.");
	if(strNames != null) {
		if (strNames.length > 1) {
			post_dbName = strNames[0];
		} 
	}
	
	
	try {
		
		postgrs_url2 = postgrs_url2+ "/" + post_dbName +"?currentSchema="+postgrs_dbSchema+"&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
		System.out.println("Record count Postgres URL = " + postgrs_url);
		Class.forName("org.postgresql.Driver");
		connection = DriverManager.getConnection(postgrs_url, postgrs_userName, postgrs_password);
		
		Statement stmt = connection.createStatement();
		String sqlCountQuery = "select count(1) from " + tableName;
		System.out.println("Target table count SQL = " + sqlCountQuery);
		ResultSet result =stmt.executeQuery(sqlCountQuery); 
		
		while (result.next()) {
			recCount = result.getInt(1);
		}
		System.out.println("Records count in table " + tableName + " = " + recCount);
		connection.close();
		
	} catch(Exception e){ 
		System.out.println("ERROR : Failed to execute SQL for Matching Dashboard table");
		System.out.println(e);
	} 
	
	return recCount;
}

public int getTeradataTableRecCount(String schemaName, String tableName) {
	
	String teradata_user = "DA_EDW2B_RO";
	String teradata_password = "Cisco123$";
	String teradata_host = "TDTEST";
	int teredata_port = 1025;

	int recCount = 0;
	String SQLRecCound = null;
	Connection connection = null;
	
	String schemaName2 = schemaName.replace(".", "");
	System.out.println(" schemaName2 = " + schemaName2);
	System.out.println(" tableName = " + tableName);
	
	String connUrl="jdbc:teradata://" + teradata_host+ "/" + schemaName2;
	System.out.println("Connection URL = " + connUrl);

	try {
		Class.forName("com.teradata.jdbc.TeraDriver").newInstance();
		connection = DriverManager.getConnection(connUrl, teradata_user, teradata_password);
		System.out.println("Teradata connection successfull");
	} 
	catch(Exception e){ 
		System.out.println("ERROR : Teradata connection failed");
		System.out.println(e);
	} 
	
	try {
		
		SQLRecCound = "select count(1) from " + schemaName2+ "." + tableName;
		System.out.println("Teradata count query = " + SQLRecCound);
		Statement stmt= connection.createStatement();
		ResultSet result = stmt.executeQuery(SQLRecCound);
		System.out.println("Executing query = " + SQLRecCound);
		
		while (result.next()) {
			recCount = result.getInt(1);
		}
		System.out.println("Records count in table " + tableName + " = " + recCount);
		connection.close();
	} catch(Exception e){ 
		System.out.println("ERROR : Failed to execute SQL = " + SQLRecCound);
		System.out.println(e);
	} 
	
	return recCount;
}


public void getPostgressData() {
	
	Connection con = null;
	RunScript runScript = new RunScript();
	int audit_pk_id = 0;
	String source_table_name = null;
	String source_schema = null;
	String target_table_name = null;
	String audit_ready = null;
	String load_type = null;

	java.util.Date date = new java.util.Date();  
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");  
    String strDate= formatter.format(date);  
    System.out.println("java date = " + strDate);
    
	try {
			postgrs_url = postgrs_url+"?currentSchema="+postgrs_dbSchema+"&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
			System.out.println("Postgres URL = " + postgrs_url);
			Class.forName("org.postgresql.Driver");
			con = DriverManager.getConnection(postgrs_url, postgrs_userName, postgrs_password);
			Statement stmt = con.createStatement();
			String sqlQueryAppIds = "select audit_pk_id from " + postgrs_dbSchema + "." + postgrs_tableName + " where audit_ready = 'Y' OR audit_ready = 'y'";
			System.out.println("Postgres App IDs query  = " + sqlQueryAppIds);
			ResultSet result1 = stmt.executeQuery(sqlQueryAppIds);
			
			while(result1.next()) {
				audit_pk_id = result1.getInt("audit_pk_id");
				postgrsAuditIDs.add(audit_pk_id);
			}
			
			if(postgrsAuditIDs.size() > 0) {
				updateAuditStatusInPostgresTable(con, postgrsAuditIDs);
				System.out.println("Updated audit_ready status to 'N'....");
			} else {
				System.out.println("... No records found with audit_ready = 'Y', hence no records updated to 'N'");
			}
			
			if(postgrsAuditIDs.size() > 0) {
				String strAuditIds = postgrsAuditIDs.toString().replace('[', ' ').replace(']',' ');
				String sqlQueryTables = "select audit_pk_id, source_table_name, source_schema, target_table_name, audit_ready, load_type from " + postgrs_dbSchema + "." + postgrs_tableName + " where audit_pk_id in(" + strAuditIds + ")";
				System.out.println("Postgres main query  = " + sqlQueryTables);
				ResultSet result2 = stmt.executeQuery(sqlQueryTables);
				
				while(result2.next()) {
					audit_pk_id = result2.getInt("audit_pk_id");
					source_table_name = result2.getString("source_table_name");
					source_schema = result2.getString("source_schema");
					target_table_name = result2.getString("target_table_name");
					audit_ready = result2.getString("audit_ready");
					load_type = result2.getString("load_type");
					System.out.println(audit_pk_id + "  " + result2.getString("source_table_name") + "  " + source_table_name + "  "+  result2.getString("target_table_name") + "   " + target_table_name + "  " + audit_ready + "  "+ load_type);
					String targetTableName = null;
					String[] strNames;
					strNames = target_table_name.split("\\.");
					if(strNames != null) {
						if (strNames.length > 1) {
							targetTableName = strNames[1];
						} else {
							targetTableName = strNames[0];
							System.out.println("Destination table name don't have dabaseName.tableName formate, hence taking field as table name = " + targetTableName);
						}
					}
					
					runScript.runJob(source_table_name, targetTableName, load_type);
					

//					int postTargetTbleRecordsCount = getPostTargetTableCount(target_table_name);
//					System.out.println("Postgres target table records count = "+ postTargetTbleRecordsCount);
//					
//					int teredataSourceTableRecordsCount = getTeradataTableRecCount(source_schema, source_table_name);
//					System.out.println("Teradata source table records count = "+ teredataSourceTableRecordsCount);
//					
//					if(teredataSourceTableRecordsCount == 0 && postTargetTbleRecordsCount == 0) {
//						System.out.println("Both input tables have zero records, hence not processing the table = " + source_table_name);
//					} else {
//						System.out.println("Need to process audit Id = "  + audit_pk_id + " and table = " + source_table_name);
//						runScript.runJob(source_table_name, targetTableName, load_type);
//					}
				}
				
			} else {
				System.out.println("... No records found with audit_ready = 'Y', hence no processing");
			}
			
		con.close();
		   
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) {
		
		ProcessCiscoJobs process = new ProcessCiscoJobs();
		process.getPostgressData();
		
//		ExcuteJobs runScript = new ExcuteJobs();
//		String sourceTableName = null;
//		
//		if (args.length > 0 ) {
//			sourceTableName = args[0];
//			System.out.println("Table name you provided = " + sourceTableName);
//			runScript.runJob(sourceTableName);
//		} else {
//			System.out.println("Please provide table name as parameter");
//		}
		
		System.out.println("..... End......");
	}
}