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

import org.w3c.dom.Node;

/**
 * PostgreSQL key declaration from xs:key or xs:unique.
 *
 * @author yokochi
 */
public class PgKey {

	/** The key name in PostgreSQL. */
	protected String name;

	/** The PostgreSQL schema name. */
	protected String schema_name;

	/** The table name (canonical). */
	protected String table_xname;

	/** The table name (in PostgreSQL). */
	protected String table_pname;

	/** The field names (canonical), separated by comma character. */
	protected String field_xnames;

	/** The field names (in PostgreSQL), separated by comma character. */
	protected String field_pnames;

	/**
	 * Instance of PgKey.
	 *
	 * @param schema_name PostgreSQL schema name
	 * @param node current node
	 * @param name key name
	 * @param case_sense whether retain case sensitive name in PostgreSQL DDL
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgKey(String schema_name, Node node, String name, boolean case_sense) throws PgSchemaException {

		this.name = name;

		this.schema_name = schema_name;

		table_xname = PgSchemaUtil.extractSelectorXPath(node);
		table_pname = case_sense ? table_xname : table_xname.toLowerCase();

		field_xnames = PgSchemaUtil.extractFieldXPath(node);
		field_pnames = case_sense ? field_xnames : field_xnames.toLowerCase();

	}

	/**
	 * Return whether key is empty.
	 *
	 * @return boolean whether key is empty
	 */
	public boolean isEmpty() {

		if (table_xname == null || table_xname.isEmpty())
			return true;

		if (field_xnames == null || field_xnames.isEmpty())
			return true;

		return false;
	}

	/**
	 * Return equality of key.
	 *
	 * @param key compared key
	 * @return boolean whether key matches
	 */
	public boolean equals(PgKey key) {
		return schema_name.equals(key.schema_name) &&
				table_xname.equals(key.table_xname) &&
				field_xnames.equals(key.field_xnames);
	}

}
