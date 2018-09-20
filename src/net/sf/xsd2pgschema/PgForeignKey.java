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

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * PostgreSQL foreign key declaration from xs:keyref.
 *
 * @author yokochi
 */
public class PgForeignKey {

	/** The foreign key name in PostgreSQL. */
	protected String name;

	/** The PostgreSQL schema name. */
	protected String pg_schema_name;

	/** The child table name (canonical). */
	protected String child_table_xname;

	/** The child table name (in PostgreSQL). */
	protected String child_table_pname;

	/** The child field names (canonical), separated by comma character. */
	protected String child_field_xnames;

	/** The child field names (in PostgreSQL), separated by comma character. */
	protected String child_field_pnames;

	/** The parent table name (canonical). */
	protected String parent_table_xname = null;

	/** The parent table name (in PostgreSQL). */
	protected String parent_table_pname = null;

	/** The parent field names (canonical), separated by comma character. */
	protected String parent_field_xnames = null;

	/** The parent field names (in PostgreSQL), separated by comma character. */
	protected String parent_field_pnames = null;

	/**
	 * Instance of PgForeignKey.
	 *
	 * @param pg_schema_name PostgreSQL schema name
	 * @param key_nodes node list of xs:key
	 * @param node current node
	 * @param name foreign key name
	 * @param key_name key name
	 * @param case_sense whether retain case sensitive name in PostgreSQL DDL
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgForeignKey(String pg_schema_name, NodeList key_nodes, Node node, String name, String key_name, boolean case_sense) throws PgSchemaException {

		this.name = name;

		this.pg_schema_name = pg_schema_name;

		child_table_xname = PgSchemaUtil.extractSelectorXPath(node);
		child_table_pname = case_sense ? child_table_xname : child_table_xname.toLowerCase();

		child_field_xnames = PgSchemaUtil.extractFieldXPath(node);
		child_field_pnames = case_sense ? child_field_xnames : child_field_xnames.toLowerCase();

		Node key_node;

		for (int i = 0; i < key_nodes.getLength(); i++) {

			key_node = key_nodes.item(i);

			if (!key_name.equals(((Element) key_node).getAttribute("name")))
				continue;

			parent_table_xname = PgSchemaUtil.extractSelectorXPath(key_node);
			parent_table_pname = case_sense ? parent_table_xname : parent_table_xname.toLowerCase();

			parent_field_xnames = PgSchemaUtil.extractFieldXPath(key_node);
			parent_field_pnames = case_sense ? parent_field_xnames : parent_field_xnames.toLowerCase();

			break;
		}

	}

	/**
	 * Return whether foreign key is empty.
	 *
	 * @return boolean whether foreign key is empty
	 */
	public boolean isEmpty() {

		if (child_table_xname == null || child_table_xname.isEmpty())
			return true;

		if (parent_table_xname == null || parent_table_xname.isEmpty())
			return true;

		if (child_field_xnames == null || child_field_xnames.isEmpty())
			return true;

		if (parent_field_xnames == null || parent_field_xnames.isEmpty())
			return true;

		return false;
	}

	/**
	 * Return equality of foreign key.
	 *
	 * @param foreign_key compared foreign key
	 * @return boolean whether foreign key matches
	 */
	public boolean equals(PgForeignKey foreign_key) {
		return pg_schema_name.equals(foreign_key.pg_schema_name) &&
				child_table_xname.equals(foreign_key.child_table_xname) && parent_table_xname.equals(foreign_key.parent_table_xname) &&
				child_field_xnames.equals(foreign_key.child_field_xnames) && parent_field_xnames.equals(foreign_key.parent_field_xnames);
	}

}
