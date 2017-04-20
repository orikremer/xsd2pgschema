/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2017 Masashi Yokochi

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
 * PostgreSQL option
 * @author yokochi
 */
public class PgOption {

	public String host = PgSchemaUtil.host; // default host name
	public int port = PgSchemaUtil.port; // default port number
	public String database = ""; // database name
	public String user = ""; // database user name
	public String password = ""; // database password

	public boolean update = false; // whether copy or update (delete before insert)

	/**
	 * Return database URL for JDBC connection
	 * @return String database URL
	 */
	public String getDbUrl() {
		return "jdbc:postgresql://" + host + ":" + port + "/" + database;
	}

}
