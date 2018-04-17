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

/**
 * PostgreSQL foreign key declaration.
 *
 * @author yokochi
 */
public class PgForeignKey {

	/** The foreign key name in PostgreSQL. */
	String name = "";

	/** The PostgreSQL schema name. */
	String pg_schema_name = null;

	/** The child table name. */
	String child_table = null;

	/** The child field names, separated by comma character. */
	String child_fields = null;

	/** The parent table name. */
	String parent_table = null;

	/** The parent field names, separated by comma character. */
	String parent_fields = null;

	/**
	 * Instance of PgForeignKey.
	 *
	 * @param option PostgreSQL data model option
	 * @param pg_schema_name PostgreSQL schema name
	 * @param node current node
	 * @param parent_node parent node
	 * @param name foreign key name
	 * @param key_name key name
	 */
	public PgForeignKey(PgSchemaOption option, String pg_schema_name, Node node, Node parent_node, String name, String key_name) {

		String xs_prefix_ = option.xs_prefix_;

		this.name = name;

		this.pg_schema_name = pg_schema_name;

		child_table = extractTable(option, node);
		child_fields = extractFields(option, node);

		parent_table = parent_fields = null;

		for (Node child = parent_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (!child.getNodeName().equals(xs_prefix_ + "key"))
				continue;

			Element e = (Element) child;

			if (!key_name.equals(e.getAttribute("name")))
				continue;

			parent_table = extractTable(option, child);
			parent_fields = extractFields(option, child);

			break;
		}

	}

	/**
	 * Extract child table name from xs:selector/@xpath.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 * @return String child table name
	 */
	private String extractTable(PgSchemaOption option, Node node) {

		String xs_prefix_ = option.xs_prefix_;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (!child.getNodeName().equals(xs_prefix_ + "selector"))
				continue;

			Element e = (Element) child;

			String[] xpath = e.getAttribute("xpath").split("/");

			return option.getUnqualifiedName(xpath[xpath.length - 1]).replaceAll("@", "");
		}

		return null;
	}

	/**
	 * Extract child field names from xs:field/@xpath.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 * @return String child field names separated by comma
	 */
	private String extractFields(PgSchemaOption option, Node node) {

		String xs_prefix_ = option.xs_prefix_;

		String fields = "";

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (!child.getNodeName().equals(xs_prefix_ + "field"))
				continue;

			Element e = (Element) child;

			String[] xpath = e.getAttribute("xpath").split(":");

			if (fields.isEmpty())
				fields = xpath[xpath.length - 1].replaceAll("@", "");
			else
				fields = fields.concat(", " + xpath[xpath.length - 1]).replaceAll("@", "");

		}

		return fields;
	}

	/**
	 * Return whether foreign key is empty.
	 *
	 * @return boolean whether foreign key is empty
	 */
	public boolean isEmpty() {

		if (child_table == null || child_table.isEmpty())
			return true;

		if (parent_table == null || parent_table.isEmpty())
			return true;

		if (child_fields == null || child_fields.isEmpty())
			return true;

		if (parent_fields == null || parent_fields.isEmpty())
			return true;

		return false;
	}

	/**
	 * Return equality of foreign key.
	 *
	 * @param foreign_key compared foreign key
	 * @return boolean whether the foreign key matches or not
	 */
	public boolean equals(PgForeignKey foreign_key) {
		return pg_schema_name.equals(foreign_key.pg_schema_name) &&
				child_table.equals(foreign_key.child_table) && parent_table.equals(foreign_key.parent_table) &&
				child_fields.equals(foreign_key.child_fields) && parent_fields.equals(foreign_key.parent_fields);
	}

}
