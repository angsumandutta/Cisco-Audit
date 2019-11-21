package com.databuck.ExecuteCiscoJobs;

/**
 * Hello world!
 *
 */
public class Application 
{
	public static void main(String[] args) {
		String sourceTableName = null;
		String targetTableName = null;
		String loadType = null;
		
		sourceTableName = args[0];
		targetTableName = args[1];
		loadType = args[2];
		ExcuteJobs executeJobs = new ExcuteJobs();
		executeJobs.runJob(sourceTableName, targetTableName, loadType);
		System.out.println("..... End......");
	}
}
