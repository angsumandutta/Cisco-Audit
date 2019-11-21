package com.databuck.processCiscojobs;

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
import org.springframework.stereotype.Service;

import com.databuck.bean.AppConfig;

@Service
public class RunScript {

	private Process process = null;

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
		
		String databuckHome = "/opt/dfadmrpm/databuck";
		String scriptLocation = "";
		
		if (System.getenv("DATABUCK_HOME") != null) {
			databuckHome = System.getenv("DATABUCK_HOME");
		} 
		
		System.out.println("DATABUCK_HOME path = " + System.getenv("DATABUCK_HOME") );
		scriptLocation = databuckHome + "/executeJobs.sh";
		
		String cmd = scriptLocation + "  " + sourceTableName + " " + targetTableName + "  " + loadType;
		
		System.out.println("Run Command = " + cmd);
		try {
			boolean runStatus = runCommand(cmd, false);
			if (runStatus) {
				System.out.println("Command executed.");
			}
		} catch (Exception e) {
			System.out.println("..........Error while executing the script............");
			e.printStackTrace();
		}
		return 0;
	}
}