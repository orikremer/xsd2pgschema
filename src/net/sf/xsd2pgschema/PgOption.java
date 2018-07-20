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

	/** Whether create PostgreSQL index on document key. */
	public boolean create_doc_key_index = false;

	/** Whether drop PostgreSQL index on document key. */
	public boolean drop_doc_key_index = false;

	/** The minimum rows for creation of PostgreSQL index on document key. */
	public int min_rows_for_doc_key_index = PgSchemaUtil.pg_min_rows_for_doc_key_index;

	/**
	 * Return database URL for JDBC connection.
	 *
	 * @return String database URL
	 */
	public String getDbUrl() {
		return "jdbc:postgresql://" + host + ":" + port + "/" + name;
	}

	/**
	 * Return database URL for JDBC connection.
	 *
	 * @param encoding default encoding
	 * @return String database URL
	 */
	public String getDbUrl(String encoding) {
		return "jdbc:postgresql://" + host + ":" + port + "/" + name + "?charSet=" + encoding;
	}

	/**
	 * Clear authentication information.
	 */
	public void clear() {

		user = pass = null;

	}

	/**
	 * Set minimum rows to create PostgreSQL index on document key.
	 *
	 * @param min_rows_for_doc_key_index argument value
	 */
	public void setMinRowsForDocKeyIndex(String min_rows_for_doc_key_index) {

		if (min_rows_for_doc_key_index.endsWith("k"))
			this.min_rows_for_doc_key_index = (int) (Float.valueOf(min_rows_for_doc_key_index.substring(0, min_rows_for_doc_key_index.indexOf('k'))) * 1000);

		else if (min_rows_for_doc_key_index.endsWith("M"))
			this.min_rows_for_doc_key_index = (int) (Float.valueOf(min_rows_for_doc_key_index.substring(0, min_rows_for_doc_key_index.indexOf('M'))) * 1000 * 1000);

		else if (min_rows_for_doc_key_index.endsWith("G"))
			this.min_rows_for_doc_key_index = (int) (Float.valueOf(min_rows_for_doc_key_index.substring(0, min_rows_for_doc_key_index.indexOf('G'))) * 1000 * 1000 * 1000);

		else
			this.min_rows_for_doc_key_index = Integer.valueOf(min_rows_for_doc_key_index);

		if (this.min_rows_for_doc_key_index < PgSchemaUtil.pg_min_rows_for_doc_key_index) {
			System.err.println("Minimum rows for creation of PostgreSQL index on document key is less than " + PgSchemaUtil.pg_min_rows_for_doc_key_index + ". Set to the default value.");
			this.min_rows_for_doc_key_index = PgSchemaUtil.pg_min_rows_for_doc_key_index;
		}

	}

}
