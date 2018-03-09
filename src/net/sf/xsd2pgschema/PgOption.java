/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2018 Masashi Yokochi

    https://sourceforge.net/projects/xsd2pgschema/

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package net.sf.xsd2pgschema;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * PostgreSQL schema constructor option.
 *
 * @author yokochi
 */
public class PgOption {

	/** The default host name. */
	public String host = PgSchemaUtil.host;

	/** The default port number. */
	public int port = PgSchemaUtil.port;

	/** The database name. */
	public String name = "";

	/** The database user name. */
	public String user = "";

	/** The database password. */
	public String pass = "";

	/** Whether perform consistency test on PostgreSQL DDL. */
	public boolean test = false;

	/** Whether adopt strict synchronization (insert if not exists, update if required, and delete rows if XML not exists). */
	public boolean sync = false;

	/** Whether adopt weak synchronization (insert if not exists, no update even if exists, no deletion). */
	public boolean sync_weak = false;

	/** The directory for check sum files. */
	public File check_sum_dir = null;

	/** The default algorithm for check sum. */
	public String check_sum_algorithm = "MD5";

	/** The instance of message digest. */
	public MessageDigest message_digest = null;

	public PgOption() {

		setCheckSumAlgorithm(check_sum_algorithm);

	}

	/**
	 * Return database URL for JDBC connection.
	 *
	 * @return String database URL
	 */
	public String getDbUrl() {
		return "jdbc:postgresql://" + host + ":" + port + "/" + name;
	}

	/**
	 * Instance message digest by algorithm name.
	 *  
	 * @param check_sum_algorithm algorithm name for check sum
	 * @return whether algorithm name is valid or not
	 */
	public boolean setCheckSumAlgorithm(String check_sum_algorithm) {

		try {

			message_digest = MessageDigest.getInstance(check_sum_algorithm);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return false;
		}

		this.check_sum_algorithm = check_sum_algorithm;

		return true;
	}

	/**
	 * Set a directory for check sum files.
	 * 
	 * @param check_sum_dir directory for check sum files.
	 */
	public void setCheckSumDirectory(File check_sum_dir) {
		this.check_sum_dir = check_sum_dir;
	}

}
