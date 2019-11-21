package com.databuck.ExecuteCiscoJobs;

import java.awt.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import com.databuck.bean.AppConfig;
//import com.databuck.processCiscojobs.ProcessCiscoJobs;

@Service
public class ExcuteJobs {
	
	@Autowired
	public Properties appDBConnectionProperties;
	
	//private int RCCOUNTAPPID = 0;
	private ArrayList<Integer> RCCOUNTAPPID = new ArrayList<>();
	private ArrayList<String>  countCheckTableNames = new ArrayList<String>();
	
	private ArrayList<Integer> FIELDMATCHING = new ArrayList<>();
	private ArrayList<String>  fieldMachingTableNames = new ArrayList<String>();
	
	private ArrayList<Integer> NULLCHECKAPPID = new ArrayList<>();
	private ArrayList<Integer> DUPLICATECHECKAPPID = new ArrayList<>();
	private ArrayList<Integer> DEPENDENCYAPPID = new ArrayList<>();
	private ArrayList<Integer> DOLLARCHECKAPPID = new ArrayList<>();
	
	private String strTableName = null;
	private String cmd = null;
	private Process process = null;
	
	private String mysql_url = null;
	private String mysql_userName = null;
	private String mysql_password = null;
	
	Properties prop= appDBConnectionProperties;
	
	public int checkJobStatusInMatchingDashboard(int idApp) {
			int recCount = 0;
			AppConfig appConfig = new AppConfig();
			prop = appConfig.appDbConnectionProperties( "/propertiesFiles/resultsdb.properties", "db1.pwd");
			
			mysql_url = prop.getProperty("db1.url");
			mysql_userName = prop.getProperty("db1.user");
			mysql_password = prop.getProperty("db1.pwd");
			
			Connection conection = null;
			System.out.println("Checking job status in DATA_MATCHING_DASHBOARD table for IdApp = " + idApp);
			
			try{  
				Class.forName("com.mysql.jdbc.Driver");  
				System.out.println("Job check URL : "+ mysql_url);
				conection = DriverManager.getConnection(mysql_url, mysql_userName, mysql_password);
				Statement stmt = conection.createStatement();  
				//String query = "SELECT COUNT(1) FROM data_matching_dashboard WHERE idApp = " + idApp + " AND 'FAILED' IN(source1Status, source2Status, unMatchedStatus)";
				String query = "SELECT COUNT(1) FROM data_matching_dashboard WHERE idApp = " + idApp + " AND 'FAILED' IN(source1Status, source2Status, unMatchedStatus)";
				System.out.println("SQL = " + query);
				ResultSet result =stmt.executeQuery(query); 
				while (result.next()) {
					recCount = result.getInt(1);
				}
				System.out.println("Failed records count in table DATA_MATCHING_DASHBOARD = " + recCount);
				conection.close();
			}
			catch(Exception e){ 
				System.out.println("ERROR : Failed to execute SQL for Matching Dashboard table");
				System.out.println(e);
			} 
			return recCount;
	}
	
	
	public String getJobStatus(int idApp) {
		String status = null;
		String leftStatus = null;
		String rightStatus = null;
		String query =null;
		AppConfig appConfig = new AppConfig();
		prop = appConfig.appDbConnectionProperties( "/propertiesFiles/resultsdb.properties", "db1.pwd");
		
		mysql_url = prop.getProperty("db1.url");
		mysql_userName = prop.getProperty("db1.user");
		mysql_password = prop.getProperty("db1.pwd");
		
		Connection conection = null;
		System.out.println("Checking job status in DATA_MATCHING_DASHBOARD table for IdApp = " + idApp);
		
		String tableName = "DATA_MATCHING_" + idApp + "_SUMMARY";
		
		try{  
			Class.forName("com.mysql.jdbc.Driver");  
			System.out.println("Job check URL : "+ mysql_url);
			conection = DriverManager.getConnection(mysql_url, mysql_userName, mysql_password);
			Statement stmt = conection.createStatement();  
			
			query = "SELECT source1OnlyStatus, source2OnlyStatus FROM " + tableName + " where Run = " + 
					"(select max(Run) from " + tableName + " where Date = (" + 
					"select max(Date) from " + tableName + "))" + 
					" and Date = " + 
					"(select max(Date) from " + tableName + ")";
			
//					SqlRowSet rs1 = jdbcTemplate1.queryForRowSet(sql);
//					String leftStatus = "";
//					String rightStatus = "";
//
//					while (rs1.next()) {
//						leftStatus = rs1.getString(11);
//						rightStatus = rs1.getString(14);
//					}
			
			
			//query = "SELECT source1OnlyStatus, source2OnlyStatus FROM " + tableName + " WHERE idApp = " + idApp;
			System.out.println("SQL = " + query);
			ResultSet result = stmt.executeQuery(query); 
			while (result.next()) {
				leftStatus = result.getString("source1OnlyStatus");
				rightStatus = result.getString("source2OnlyStatus");
			}

			status = leftStatus + ":"	+ rightStatus;
			System.out.println("leftStatus & rightStatus = " + status);
			conection.close();
		}
		catch(Exception e){ 
			System.out.println("ERROR : Failed to execute getstatus query : " + query);
			System.out.println(e);
		} 
		return status;
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
	
	
	public int runJob(String sourceTableName, String targetTableName, String loadType) {
		
		AppConfig appConfig = new AppConfig();
		prop = appConfig.appDbConnectionProperties( "/propertiesFiles/appdb.properties", "db.pwd");
		
		mysql_url = prop.getProperty("db.url");
		mysql_userName = prop.getProperty("db.user");
		mysql_password = prop.getProperty("db.pwd");
		
		
		System.out.println("My SQL Executor URL = " + mysql_url);
		System.out.println("My SQL Executor password = " + mysql_password);
		System.out.println("My SQL Executor user = " + mysql_userName);
		
		try{
			
			Class.forName("com.mysql.jdbc.Driver");  
			Connection con = DriverManager.getConnection(mysql_url, mysql_userName, mysql_password);  
			
			System.out.println("Excutor URL = " + mysql_url);

			//String getIdAppSql = "select idApp, name from listApplications  where name like '" +  sourceTableName + "-%" + loadType + "' OR name like  '" + targetTableName + "-%" +loadType + "' and active = 'yes' ";
			String getIdAppSql = "select max(idApp), name from listApplications  where name like '" +  sourceTableName + "-%" + loadType + "' OR name like  '" + targetTableName + "-%" +loadType + "' and active = 'yes' group by name";
			
			System.out.println("getIdAppSql = " + getIdAppSql);
			
			Statement stmt1 = con.createStatement();  
			ResultSet rs1=stmt1.executeQuery(getIdAppSql);
			 
			while(rs1.next())  {
				 strTableName = rs1.getString(2).toLowerCase();
				 
				 if (strTableName.contains("-CountCheck-".toLowerCase())) {
					 RCCOUNTAPPID.add(rs1.getInt(1));
					 countCheckTableNames.add(strTableName);
					 System.out.println("App ID = " + rs1.getInt(1) + " CountCheck = " + strTableName);
					 
				 } else if(strTableName.contains("-FieldMatching-".toLowerCase())) {
					 FIELDMATCHING.add(rs1.getInt(1));
					 fieldMachingTableNames.add(strTableName);
					 System.out.println("App ID = " + rs1.getInt(1) + " FieldMatching = " + strTableName);
				 }
			}
			
			//String getIdAppSql2 = "select idApp, name from listApplications  where name like '%" +  sourceTableName + "-%' OR name like  '%" + targetTableName + "-%' and active = 'yes' ";
			String getIdAppSql2 = "select max(idApp), name from listApplications  where name like '" +  sourceTableName + "-%'"  + " OR name like  '" + targetTableName + "-%'"  + " and active = 'yes' group by name";
			System.out.println("Others SQL = " + getIdAppSql2);
			ResultSet rs2=stmt1.executeQuery(getIdAppSql2);
			
			while(rs2.next()) {
				
				strTableName = rs2.getString(2).toLowerCase();
				if(strTableName.contains("-NullCheck-".toLowerCase())) {
					 NULLCHECKAPPID.add(rs2.getInt(1));
					 System.out.println("App ID = " + rs2.getInt(1) + " NullCheck = " + strTableName);
					 
				 } else if(strTableName.contains("-DupsCheck-".toLowerCase())) {
					 DUPLICATECHECKAPPID.add(rs2.getInt(1));
					 System.out.println("App ID = " + rs2.getInt(1)  + " DupsCheck = " + strTableName);
					 
				 } else if(strTableName.contains("-DepeCheck-".toLowerCase())) {
					 DEPENDENCYAPPID.add(rs2.getInt(1));
					 System.out.println("App ID = " + rs2.getInt(1)  + " DepeCheck = " + strTableName);
					 
				 } else if(strTableName.contains("-DollarCheck-".toLowerCase())) {
					 DOLLARCHECKAPPID.add(rs2.getInt(1));
					 System.out.println("App ID = " + rs2.getInt(1)  + " DollarCheck = " + strTableName);
				 }

			}
			
			
			con.close();
			
			boolean recordCountRunStatus = false;
			String scriptLocation = "";
			String databuckHome = "/opt/dfadmrpm/databuck";
			boolean countCheckMaster = false;
			boolean countCheckIncremental = false;
			boolean countCheckUpdate = false;
			boolean countCheckMonthly = false;
			String runNextJobs = null;
			boolean singleTableRun = false;
			String tableName = null;
			String source1Status = null;
			String source2Status = null;
			
			if (System.getenv("DATABUCK_HOME") != null) {
				databuckHome = System.getenv("DATABUCK_HOME");
			} 
			
			System.out.println("DATABUCK_HOME path = " + System.getenv("DATABUCK_HOME") );
			scriptLocation = databuckHome + "/test.sh";

			//1.Run Record Count  ( test.sh RCCOUNTAPPID ) – wait. If Step 1 is successful – then proceed to Step 2
			 if(RCCOUNTAPPID.size() > 0) {
				 recordCountRunStatus = false; 
				for(int i = 0; i < RCCOUNTAPPID.size(); i++) {
					singleTableRun = false;
					String cmd = scriptLocation + "  " + RCCOUNTAPPID.get(i);
					System.out.println("Run Record Cout Check, Command = " + cmd);
					recordCountRunStatus = runCommand(cmd, true);
					
					String returnStatus[] = getJobStatus(RCCOUNTAPPID.get(i)).split(":");
					if(returnStatus != null & returnStatus.length > 1) {
						source1Status = returnStatus[0];
						source2Status = returnStatus[1];
					}
					
					if(source1Status.equalsIgnoreCase("ZeroCount") && source2Status.equalsIgnoreCase("ZeroCount")) {
						runNextJobs = "ZEROCOUNT";
					} else if(source1Status.equalsIgnoreCase("Passed") && source2Status.equalsIgnoreCase("Passed")) {
						runNextJobs = "PASSED";
					} else if(source1Status.equalsIgnoreCase("Failed") && source2Status.equalsIgnoreCase("Failed")) {
						runNextJobs = "FAILED";
					} else if(source1Status.equalsIgnoreCase("ZeroCount") || source2Status.equalsIgnoreCase("ZeroCount")) {
						runNextJobs = "FAILED";
					}
					
					System.out.println("runNextJobs = " + runNextJobs);
					
//					if(recordCountRunStatus) {
//						if(checkJobStatusInMatchingDashboard(RCCOUNTAPPID.get(i)) > 0) {
//							recordCountRunStatus = false;
//							System.out.println("Failed records found in DATA_MATCHING_DASHBOARD table for IdApp = " + RCCOUNTAPPID.get(i));
//						} else {
//							System.out.println("Job run successfully for IdApp = " + RCCOUNTAPPID.get(i) + " and table name = " + countCheckTableNames.get(i) );
//							runNextJobs = true;
//						}
//					} 
					
					if(!recordCountRunStatus) {
						System.out.println("Record Count job failed for IdApp = " + RCCOUNTAPPID.get(i) + " and table name = " + countCheckTableNames.get(i));
						System.out.println("***** Other jobs will not run, because Record Count failed");
					}
					
					if (recordCountRunStatus) {
						
						tableName = countCheckTableNames.get(i).toLowerCase();
						if(tableName.contains("-Master-".toLowerCase())) {
							countCheckMaster = true;
							System.out.println("Count check run for Master, table name = " + countCheckTableNames.get(i));
						} else if(tableName.contains("-Incremental-".toLowerCase())) {
							countCheckIncremental = true;
							System.out.println("Count check run for Incremental, table name = " + countCheckTableNames.get(i));
						} else if(tableName.contains("-Update-".toLowerCase())) {
							countCheckUpdate = true;
							System.out.println("Count check run for Update, table name = " + countCheckTableNames.get(i));
						} else if(tableName.contains("-Monthly-".toLowerCase())) {
							countCheckMonthly = true;
							System.out.println("Count check run for Monthly, table name = " + countCheckTableNames.get(i));
						}
					}
					
					
					if(!(countCheckMaster || countCheckIncremental || countCheckUpdate || countCheckMonthly)) {
						singleTableRun = true;
						System.out.println("....Count Check Single table run...");
					} else {
						System.out.println("....Count Check Multi  table run...");
					}
				}

			}
			 
			 
			// 4. Run duplicate check – do not wait 
			if(runNextJobs.equalsIgnoreCase("PASSED") || runNextJobs.equalsIgnoreCase("FAILED")) {
				for(int i = 0; i < DUPLICATECHECKAPPID.size(); i++) {
					cmd = scriptLocation + "  " + DUPLICATECHECKAPPID.get(i);  //Duplicate check
					System.out.println("Run dup check,  DUPLICATECHECKAPPID = " + DUPLICATECHECKAPPID.get(i));
					runCommand(cmd, true);
				}
			}

			
			if(runNextJobs.equalsIgnoreCase("PASSED")) {
				
				//3. Run NullCheck – do not wait
				for(int i = 0; i < NULLCHECKAPPID.size(); i++) {
					cmd = scriptLocation + "  " + NULLCHECKAPPID.get(i);  //Null check
					System.out.println("Run NullCheck,  NULLCHECKAPPID = " + NULLCHECKAPPID.get(i));
					runCommand(cmd, true);
				}

				// 5. Run all dependency check in parallel
				for(int i = 0; i < DEPENDENCYAPPID.size(); i++) {
					cmd = scriptLocation + "  " + DEPENDENCYAPPID.get(i);  //Dependency check
					System.out.println("Run dependency check,  DEPENDENCYAPPID = " + DEPENDENCYAPPID.get(i));
					runCommand(cmd, true);
				}
				
				// 6. Run dollar value checks one by one
				for(int i = 0; i < DOLLARCHECKAPPID.size(); i++) {
					cmd = scriptLocation + "  " + DOLLARCHECKAPPID.get(i);  //Dollar check
					System.out.println("Run dollar value checks,  DOLLARCHECKAPPID = " + DOLLARCHECKAPPID.get(i));
					recordCountRunStatus = runCommand(cmd, true);
				}
			}
			
			 //2. Run all below jobs (step 2 to 6 in sequence) if job RCCOUNTAPPID executed successfully.
			 //2. Field matching
			if(runNextJobs.equalsIgnoreCase("PASSED")) {
				System.out.println("..... Field Matching run  ........");
				for(int i = 0; i < FIELDMATCHING.size(); i++) {
					tableName = fieldMachingTableNames.get(i).toLowerCase();
					cmd = scriptLocation + "  " + FIELDMATCHING.get(i) ; 
					
					if(countCheckMaster && tableName.contains("-Master-".toLowerCase())) {
						System.out.println("Field matchig run for Master, table name = " + tableName + " Command = " + cmd);
						runCommand(cmd, true);	
					} else if(countCheckIncremental && tableName.contains("-Incremental-".toLowerCase())) {
						System.out.println("Field matchig run for Incremental, table name = " + tableName + " Command = " + cmd);
						runCommand(cmd, true);
					} else if(countCheckUpdate && tableName.contains("-Update-".toLowerCase())) {
						System.out.println("Field matchig run for Update, table name = " + tableName + " Command = " + cmd);
						runCommand(cmd, true);	
					} else if(countCheckMonthly && tableName.contains("-Monthly-".toLowerCase())) {
						System.out.println("Field matchig run for Monthly, table name = " + tableName + " Command = " + cmd);
						runCommand(cmd, true);	
					} else {
						System.out.println("Field Matching run for other job, table name = " + tableName + " Command = " + cmd);
						runCommand(cmd, true);	
					}
				}
			}
			
							
			RCCOUNTAPPID.clear();
			FIELDMATCHING.clear();
			NULLCHECKAPPID.clear();
			DUPLICATECHECKAPPID.clear();
			DEPENDENCYAPPID.clear();
			DOLLARCHECKAPPID.clear();
			
		}catch(Exception E){
			System.out.println("My SQL ERROR, job Failed ");
			E.printStackTrace();
			return 1;
		}
		return 0;
	}

}
