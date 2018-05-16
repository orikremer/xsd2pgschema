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

	/**
	 * Return database URL for JDBC connection.
	 *
	 * @return String database URL
	 */
	public String getDbUrl() {
		return "jdbc:postgresql://" + host + ":" + port + "/" + name;
	}

	/**
	 * Clear authentication information.  
	 */
	public void clear() {

		user = pass = null;

	}

}
