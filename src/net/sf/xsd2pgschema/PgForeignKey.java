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

import java.io.Serializable;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * PostgreSQL foreign key declaration.
 *
 * @author yokochi
 */
public class PgForeignKey implements Serializable {

	/** The default serial version ID. */
	private static final long serialVersionUID = 1L;

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

		child_table_xname = extractTableName(option, node);
		child_table_pname = option.case_sense ? child_table_xname : child_table_xname.toLowerCase();

		child_field_xnames = extractFieldNames(option, node);
		child_field_pnames = option.case_sense ? child_field_xnames : child_field_xnames.toLowerCase();

		for (Node child = parent_node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (!child.getNodeName().equals(xs_prefix_ + "key"))
				continue;

			Element e = (Element) child;

			if (!key_name.equals(e.getAttribute("name")))
				continue;

			parent_table_xname = extractTableName(option, child);
			parent_table_pname = option.case_sense ? parent_table_xname : parent_table_xname.toLowerCase();

			parent_field_xnames = extractFieldNames(option, child);
			parent_field_pnames = option.case_sense ? parent_field_xnames : parent_field_xnames.toLowerCase();

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
	private String extractTableName(PgSchemaOption option, Node node) {

		String xs_prefix_ = option.xs_prefix_;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (!child.getNodeName().equals(xs_prefix_ + "selector"))
				continue;

			Element e = (Element) child;

			String[] xpath = e.getAttribute("xpath").split("/");

			return PgSchemaUtil.getUnqualifiedName(xpath[xpath.length - 1]).replace("@", "");
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
	private String extractFieldNames(PgSchemaOption option, Node node) {

		String xs_prefix_ = option.xs_prefix_;

		String fields = "";

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (!child.getNodeName().equals(xs_prefix_ + "field"))
				continue;

			Element e = (Element) child;

			String[] xpath = e.getAttribute("xpath").split(":");

			if (fields.isEmpty())
				fields = xpath[xpath.length - 1].replace("@", "");

			else
				fields = fields.concat(", " + xpath[xpath.length - 1]).replace("@", "");

		}

		return fields;
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
