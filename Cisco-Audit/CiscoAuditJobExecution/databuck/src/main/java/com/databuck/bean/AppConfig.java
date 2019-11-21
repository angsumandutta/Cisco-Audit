package com.databuck.bean;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.springframework.context.annotation.Bean;

public class AppConfig {
	@Bean
	public Properties appDbConnectionProperties(String dbPropertiesFile, String dbPassoword) {
		java.util.Properties propFile = new java.util.Properties();
		try {
			InputStream is = new FileInputStream(
					new File(System.getenv("DATABUCK_HOME")+ dbPropertiesFile));
			propFile.load(is);
		} catch (Exception e) {
			e.printStackTrace();
		}
		StandardPBEStringEncryptor decryptor = new StandardPBEStringEncryptor();
		decryptor.setPassword("4qsE9gaz%!L@UMrK5myY");
		String decryptedText = decryptor.decrypt(propFile.getProperty(dbPassoword));
		propFile.setProperty(dbPassoword, decryptedText);
		return propFile;
	}
}
