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

import java.io.IOException;
import java.util.Arrays;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;

import org.w3c.dom.Node;

/**
 * Node parser for JSON conversion.
 *
 * @author yokochi
 */
public class PgSchemaNode2Json extends PgSchemaNodeParser {

	/** The position of header begins in JSON buffer. */
	protected int jsonb_header_begin;

	/** The position of header ends in JSON buffer. */
	protected int jsonb_header_end;

	/** The white spaces between JSON item and JSON data. */
	private String key_value_space = " ";

	/**
	 * Node parser for JSON conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table
	 * @param table current table
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerConfigurationException the transformer configuration exception
	 */
	public PgSchemaNode2Json(final PgSchema schema, final PgTable parent_table, final PgTable table) throws ParserConfigurationException, TransformerConfigurationException {

		super(schema, parent_table, table);

		key_value_space = schema.jsonb.key_value_space;

	}

	/**
	 * Parse processing node (root).
	 *
	 * @param proc_node processing node
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void parseRootNode(final Node proc_node) throws TransformerException, IOException {

		parse(proc_node, null, current_key = document_id + "/" + table.xname, nested, 1);

	}

	/**
	 * Parse processing node (child).
	 *
	 * @param node_test node tester
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws TransformerException the transformer exception
	 */
	@Override
	public void parseChildNode(final PgSchemaNodeTester node_test) throws IOException, TransformerException {

		parse(node_test.proc_node, node_test.parent_key, node_test.current_key, node_test.nested, node_test.key_id);

	}

	/**
	 * Parse processing node (child).
	 *
	 * @param proc_node processing node
	 * @param parent_key key name of parent node
	 * @param proc_key processing key name
	 * @param nested whether it is nested
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void parseChildNode(final Node proc_node, final String parent_key, final String proc_key, final boolean nested) throws TransformerException, IOException {

		parse(proc_node, parent_key, proc_key, nested, 1);

	}

	/**
	 * Parse processing node.
	 *
	 * @param proc_node processing node
	 * @param parent_key name of parent node
	 * @param current_key name of current node
	 * @param nested whether it is nested
	 * @param key_id ordinal number of current node
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void parse(final Node proc_node, final String parent_key, final String current_key, final boolean nested, final int key_id) throws TransformerException, IOException {

		Arrays.fill(values, "");

		filled = true;

		nested_fields = 0;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			// document_key, serial_key, xpath_key, primary_key, foreign_key

			if (field.user_key || field.primary_key || field.foreign_key)
				continue;

			// nested_key

			else if (field.nested_key) {

				if (setNestedKey(field, current_key, key_id))
					nested_fields++;

			}

			// attribute, simple_content, element

			else if (field.attribute || field.simple_content || field.element) {

				if (setContent(proc_node, field, false))
					values[f] = content;

				else if (field.required) {
					filled = false;
					break;
				}

			}

			// any, any_attribute

			else if (field.any || field.any_attribute) {

				if (setAnyContent(proc_node, field))
					values[f] = content;

			}

			if (!filled)
				break;

		}

		if (filled) {

			write();

			this.proc_node = proc_node;
			this.current_key = current_key;
			this.nested = nested;

		}

	}

	/**
	 * Writer of processing node.
	 */
	private void write() {

		written = true;

		boolean not_empty = false;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.jsonb == null)
				continue;

			if (field.jsonable && field.writeValue2JsonBuf(values[f], key_value_space))
				not_empty = true;

		}

		if (not_empty && !table.jsonb_not_empty)
			table.jsonb_not_empty = true;

	}

	/**
	 * Invoke nested node (root): Object-oriented JSON.
	 *
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void invokeRootNestedNodeObj(int json_indent_level) throws PgSchemaException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++)
			schema.parseChildNode2ObjJson(proc_node, table, schema.getTable(nested_table_id[n]), current_key, nested_key[n], list_holder[n], table.bridge, 0, json_indent_level + (table.relational ? 0 : 1));

	}

	/**
	 * Invoke nested node (root): Column-oriented JSON.
	 *
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void invokeRootNestedNodeCol(int json_indent_level) throws PgSchemaException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++)
			schema.parseChildNode2ColJson(proc_node, table, schema.getTable(nested_table_id[n]), current_key, nested_key[n], list_holder[n], table.bridge, 0, json_indent_level + (table.virtual ? 0 : 1));

	}

	/**
	 * Invoke nested node (root): Relational-oriented JSON.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeRootNestedNode() throws PgSchemaException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++)
			schema.parseChildNode2Json(proc_node, table, schema.getTable(nested_table_id[n]), current_key, nested_key[n], list_holder[n], table.bridge, 0);

	}

	/**
	 * Invoke nested node (child): Object-oriented JSON.
	 *
	 * @param node_test node tester
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void invokeChildNestedNodeObj(PgSchemaNodeTester node_test, int json_indent_level) throws PgSchemaException {

		if (!filled)
			return;

		visited = true;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			boolean exists = existsNestedNode(nested_table, node_test.proc_node);

			schema.parseChildNode2ObjJson(exists || nested ? node_test.proc_node : proc_node, table, nested_table, node_test.primary_key, nested_key[n], list_holder[n], !exists, exists ? 0 : node_test.key_id, json_indent_level + (table.relational ? 0 : 1));

		}

	}

	/**
	 * Invoke nested node (child): Column-oriented JSON.
	 *
	 * @param node_test node tester
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void invokeChildNestedNodeCol(PgSchemaNodeTester node_test, int json_indent_level) throws PgSchemaException {

		if (!filled)
			return;

		visited = true;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			boolean exists = existsNestedNode(nested_table, node_test.proc_node);

			schema.parseChildNode2ColJson(exists || nested ? node_test.proc_node : proc_node, table, nested_table, node_test.primary_key, nested_key[n], list_holder[n], !exists, exists ? 0 : node_test.key_id, json_indent_level + (table.virtual ? 0 : 1));

		}

	}

	/**
	 * Invoke nested node (child): Relational-oriented JSON.
	 *
	 * @param node_test node tester
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeChildNestedNode(PgSchemaNodeTester node_test) throws PgSchemaException {

		if (!filled)
			return;

		visited = true;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			boolean exists = existsNestedNode(nested_table, node_test.proc_node);

			schema.parseChildNode2Json(exists || nested ? node_test.proc_node : proc_node, table, nested_table, node_test.primary_key, nested_key[n], list_holder[n], !exists, exists ? 0 : node_test.key_id);

		}

	}

	/**
	 * Invoke nested node (child): Object-oriented JSON.
	 *
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void invokeChildNestedNodeObj(int json_indent_level) throws PgSchemaException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			if (existsNestedNode(nested_table, proc_node))
				schema.parseChildNode2ObjJson(proc_node, table, nested_table, current_key, nested_key[n], list_holder[n], false, 0, json_indent_level + (table.relational ? 0 : 1));

		}

	}

	/**
	 * Invoke nested node (child): Column-oriented JSON.
	 *
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void invokeChildNestedNodeCol(int json_indent_level) throws PgSchemaException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			if (existsNestedNode(nested_table, proc_node))
				schema.parseChildNode2ColJson(proc_node, table, nested_table, current_key, nested_key[n], list_holder[n], false, 0, json_indent_level + (table.virtual ? 0 : 1));

		}

	}

	/**
	 * Invoke nested node (child): Relational-oriented JSON.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeChildNestedNode() throws PgSchemaException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			if (existsNestedNode(nested_table, proc_node))
				schema.parseChildNode2Json(proc_node, table, nested_table, current_key, nested_key[n], list_holder[n], false, 0);

		}

	}

}
