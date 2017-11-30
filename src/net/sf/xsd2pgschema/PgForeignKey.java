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

	/** The child table name. */
	String child_table = null;

	/** The child field names, separated by comma character. */
	String child_fields = null;

	/** The parent table name. */
	String parent_table = null;

	/** The parent field names, separated by comma character. */
	String parent_fields = null;

	/**
	 * Set child table name.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 */
	public void extractChildTable(PgSchemaOption option, Node node) {

		child_table = extractTable(option, node);

	}

	/**
	 * Set parent table name from xs:key/@name.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 * @param key_name the key name
	 */
	public void extractParentTable(PgSchemaOption option, Node node, String key_name) {

		String xs_prefix_ = option.xs_prefix_;

		parent_table = null;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (!child.getNodeName().equals(xs_prefix_ + "key"))
				continue;

			Element e = (Element) child;

			if (!key_name.equals(e.getAttribute("name")))
				continue;

			parent_table = extractTable(option, child);

			break;
		}

	}

	/**
	 * Extract child field names from xs:field/@xpath.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 * @return child field names separated by comma
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
	 * Set child field names.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 */
	public void extractChildFields(PgSchemaOption option, Node node) {

		child_fields = extractFields(option, node);

	}

	/**
	 * Set parent field names from xs:key/@name.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 * @param key_name key name
	 */
	public void extractParentFields(PgSchemaOption option, Node node, String key_name) {

		String xs_prefix_ = option.xs_prefix_;

		parent_fields = null;

		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

			if (!child.getNodeName().equals(xs_prefix_ + "key"))
				continue;

			Element e = (Element) child;

			if (!key_name.equals(e.getAttribute("name")))
				continue;

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

}
