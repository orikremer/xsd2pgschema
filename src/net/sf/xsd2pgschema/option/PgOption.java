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

package net.sf.xsd2pgschema.option;

import net.sf.xsd2pgschema.PgSchemaUtil;

/**
 * PostgreSQL schema constructor option.
 *
 * @author yokochi
 */
public class PgOption {

	/** The default host name of PostgreSQL server. */
	public String pg_host = PgSchemaUtil.pg_host;

	/** The default port number of PostgreSQL server. */
	public int pg_port = PgSchemaUtil.pg_port;

	/** The database name. */
	public String name = "";

	/** The database user name. */
	public String user = "";

	/** The database password. */
	public String pass = "";

	/** Whether to perform consistency test on PostgreSQL DDL. */
	public boolean test = false;

	/** Whether to create PostgreSQL index on document key. */
	public boolean create_doc_key_index = false;

	/** Whether to drop PostgreSQL index on document key. */
	public boolean drop_doc_key_index = false;

	/** Whether to create PostgreSQL index on attribute. */
	public boolean create_attr_index = true;

	/** Whether to drop PostgreSQL index on attribute. */
	public boolean drop_attr_index = false;

	/** The minimum rows for creation of PostgreSQL index on document key. */
	public int min_rows_for_doc_key_index = PgSchemaUtil.pg_min_rows_for_doc_key_index;

	/** The maximum attribute columns for creation of PostgreSQL index on the attributes (except for in-place document key). */
	public int max_attr_cols_for_index = PgSchemaUtil.pg_max_attr_cols_for_index;

	/** The internal status corresponding to --create-doc-key-index option. */
	private boolean _create_doc_key_index = false;

	/** The internal status corresponding to --no-create-doc-key-index option. */
	private boolean _no_create_doc_key_index = false;

	/** The internal status corresponding to --drop-doc-key-index option. */
	private boolean _drop_doc_key_index = false;

	/** The internal status corresponding to --create-attr-index option. */
	private boolean _create_attr_index = false;

	/** The internal status corresponding to --no-create-attr-index option. */
	private boolean _no_create_attr_index = false;

	/** The internal status corresponding to --drop-attr-index option. */
	private boolean _drop_attr_index = false;

	/**
	 * Return database URL for JDBC connection.
	 *
	 * @return String database URL
	 */
	public String getDbUrl() {
		return "jdbc:postgresql://" + pg_host + ":" + pg_port + "/" + name;
	}

	/**
	 * Return database URL for JDBC connection.
	 *
	 * @param encoding default encoding
	 * @return String database URL
	 */
	public String getDbUrl(String encoding) {
		return "jdbc:postgresql://" + pg_host + ":" + pg_port + "/" + name + "?charSet=" + encoding;
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
			this.min_rows_for_doc_key_index = (int) (Float.valueOf(min_rows_for_doc_key_index.substring(0, min_rows_for_doc_key_index.indexOf('k'))) * 1024);

		else if (min_rows_for_doc_key_index.endsWith("M"))
			this.min_rows_for_doc_key_index = (int) (Float.valueOf(min_rows_for_doc_key_index.substring(0, min_rows_for_doc_key_index.indexOf('M'))) * 1024 * 1024);

		else if (min_rows_for_doc_key_index.endsWith("G"))
			this.min_rows_for_doc_key_index = (int) (Float.valueOf(min_rows_for_doc_key_index.substring(0, min_rows_for_doc_key_index.indexOf('G'))) * 1024 * 1024 * 1024);

		else
			this.min_rows_for_doc_key_index = Integer.valueOf(min_rows_for_doc_key_index);

		if (this.min_rows_for_doc_key_index < PgSchemaUtil.pg_min_rows_for_doc_key_index) {
			System.err.println("Minimum rows for creation of PostgreSQL index on document key is less than " + PgSchemaUtil.pg_min_rows_for_doc_key_index + ". Set to the default value.");
			this.min_rows_for_doc_key_index = PgSchemaUtil.pg_min_rows_for_doc_key_index;
		}

	}

	/**
	 * Set internal status corresponding to --sync option.
	 *
	 * @param create whether to try to create index on document key
	 * @return boolean whether status changed
	 */
	public boolean tryToCreateDocKeyIndex(boolean create) {

		if (create) {

			if (_no_create_doc_key_index || _drop_doc_key_index)
				return false;

			drop_doc_key_index = false;

		}

		else if (_create_doc_key_index)
			return false;

		create_doc_key_index = create;

		return true;
	}

	/**
	 * Set internal status corresponding to --create-doc-key-index and --no-create-doc-key-index options.
	 *
	 * @param create whether to create index on document key
	 * @return boolean whether status changed
	 */
	public boolean setCreateDocKeyIndex(boolean create) {

		if (create) {

			if (_no_create_doc_key_index) {
				System.err.println("--no-create-doc-key-index is already set.");
				return false;
			}

			if (_drop_doc_key_index) {
				System.err.println("--drop-doc-key-index is already set.");
				return false;
			}

			_create_doc_key_index = true;
			drop_doc_key_index = false;

		}

		else {

			if (_create_doc_key_index) {
				System.err.println("--create-doc-key-index is already set.");
				return false;
			}

			_no_create_doc_key_index = true;

		}

		create_doc_key_index = create;

		return true;
	}

	/**
	 * Set internal status corresponding to --drop-doc-key-index option.
	 *
	 * @return boolean whether status changed
	 */
	public boolean setDropDocKeyIndex() {

		if (_create_doc_key_index) {
			System.err.println("--create-doc-key-index is already set.");
			return false;
		}

		drop_doc_key_index = _drop_doc_key_index = true;
		create_doc_key_index = false;

		return true;
	}

	/**
	 * Set maximum attribute columns to create PostgreSQL index on the attributes.
	 *
	 * @param max_attr_cols_for_index argument value
	 */
	public void setMaxAttrColsForIndex(String max_attr_cols_for_index) {

		this.max_attr_cols_for_index = Integer.valueOf(max_attr_cols_for_index);

		if (this.max_attr_cols_for_index > PgSchemaUtil.pg_limit_attr_cols_for_index) {
			System.err.println("Maximum attribute colomuns for creation of PostgreSQL index on the attributes is greater than " + PgSchemaUtil.pg_limit_attr_cols_for_index + ". Set to the default value.");
			this.max_attr_cols_for_index = PgSchemaUtil.pg_limit_attr_cols_for_index;
		}

	}

	/**
	 * Set internal status corresponding to --create-attr-index and --no-create-attr-index options.
	 *
	 * @param create whether to create index on attribute
	 * @return boolean whether status changed
	 */
	public boolean setCreateAttrIndex(boolean create) {

		if (create) {

			if (_no_create_attr_index) {
				System.err.println("--no-create-attr-index is already set.");
				return false;
			}

			if (_drop_attr_index) {
				System.err.println("--drop-attr-index is already set.");
				return false;
			}

			_create_attr_index = true;
			drop_attr_index = false;

		}

		else {

			if (_create_attr_index) {
				System.err.println("--create-attr-index is already set.");
				return false;
			}

			_no_create_attr_index = true;

		}

		create_attr_index = create;

		return true;
	}

	/**
	 * Set internal status corresponding to --drop-attr-index option.
	 *
	 * @return boolean whether status changed
	 */
	public boolean setDropAttrIndex() {

		if (_create_attr_index) {
			System.err.println("--create-attr-index is already set.");
			return false;
		}

		drop_attr_index = _drop_attr_index = true;
		create_attr_index = false;

		return true;
	}

}
