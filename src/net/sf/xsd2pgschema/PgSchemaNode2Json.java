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

	/** Whether any content was written. */
	protected boolean written = false;

	/** The JSON Schema version. */
	protected JsonSchemaVersion schema_ver = null;

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

		schema_ver = schema.jsonb.schema_ver;

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

		parse(proc_node, current_key = document_id + "/" + table.xname, false, indirect);

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

		parse(node_test.proc_node, node_test.current_key, node_test.as_attr, node_test.indirect);

	}

	/**
	 * Parse processing node (child).
	 *
	 * @param proc_node processing node
	 * @param nested_key nested key
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void parseChildNode(final Node proc_node, final PgSchemaNestedKey nested_key) throws TransformerException, IOException {

		parse(proc_node, nested_key.current_key, nested_key.as_attr, nested_key.indirect);

	}

	/**
	 * Parse processing node.
	 *
	 * @param proc_node processing node
	 * @param current_key current key
	 * @param as_attr whether parent key as attribute
	 * @param indirect whether child node is not nested node
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void parse(final Node proc_node, final String current_key, final boolean as_attr, final boolean indirect) throws TransformerException, IOException {

		Arrays.fill(values, "");

		filled = true;

		null_simple_primitive_list = false;

		if (nested_keys != null && nested_keys.size() > 0)
			nested_keys.clear();

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			// document_key, serial_key, xpath_key, primary_key, foreign_key

			if (field.user_key || field.primary_key || field.foreign_key)
				continue;

			// nested_key

			else if (field.nested_key)
				setNestedKey(proc_node, field, current_key);

			// attribute, simple_content, element

			else if (field.attribute || field.simple_content || field.element) {

				if (setContent(proc_node, field, current_key, as_attr, false))
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

		if (!filled || (null_simple_primitive_list && (nested_keys == null || nested_keys.size() == 0)))
			return;

		write();

		this.proc_node = proc_node;
		this.current_key = current_key;
		this.indirect = indirect;

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

			if (field.jsonable && field.writeValue2JsonBuf(schema_ver, values[f], key_value_space))
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

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys)
			schema.parseChildNode2ObjJson(proc_node, table, nested_key.asOfRoot(this), json_indent_level + (table.relational ? 0 : 1));

	}

	/**
	 * Invoke nested node (root): Column-oriented JSON.
	 *
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void invokeRootNestedNodeCol(int json_indent_level) throws PgSchemaException {

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys)
			schema.parseChildNode2ColJson(proc_node, table, nested_key.asOfRoot(this), json_indent_level + (table.virtual ? 0 : 1));

	}

	/**
	 * Invoke nested node (root): Relational-oriented JSON.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeRootNestedNode() throws PgSchemaException {

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys)
			schema.parseChildNode2Json(proc_node, table, nested_key.asOfRoot(this));

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

		if (nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys) {

			boolean exists = existsNestedNode(nested_key.table, node_test.proc_node);

			schema.parseChildNode2ObjJson(exists || indirect ? node_test.proc_node : proc_node, table, nested_key.asOfChild(node_test, exists), json_indent_level + (table.relational ? 0 : 1));

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

		if (nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys) {

			boolean exists = existsNestedNode(nested_key.table, node_test.proc_node);

			schema.parseChildNode2ColJson(exists || indirect ? node_test.proc_node : proc_node, table, nested_key.asOfChild(node_test, exists), json_indent_level + (table.virtual ? 0 : 1));

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

		if (nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys) {

			boolean exists = existsNestedNode(nested_key.table, node_test.proc_node);

			schema.parseChildNode2Json(exists || indirect ? node_test.proc_node : proc_node, table, nested_key.asOfChild(node_test, exists));

		}

	}

	/**
	 * Invoke nested node (child): Object-oriented JSON.
	 *
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void invokeChildNestedNodeObj(int json_indent_level) throws PgSchemaException {

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys) {

			if (existsNestedNode(nested_key.table, proc_node))
				schema.parseChildNode2ObjJson(proc_node, table, nested_key.asOfChild(this), json_indent_level + (table.relational ? 0 : 1));

		}

	}

	/**
	 * Invoke nested node (child): Column-oriented JSON.
	 *
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void invokeChildNestedNodeCol(int json_indent_level) throws PgSchemaException {

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys) {

			if (existsNestedNode(nested_key.table, proc_node))
				schema.parseChildNode2ColJson(proc_node, table, nested_key.asOfChild(this), json_indent_level + (table.virtual ? 0 : 1));

		}

	}

	/**
	 * Invoke nested node (child): Relational-oriented JSON.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeChildNestedNode() throws PgSchemaException {

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys) {

			if (existsNestedNode(nested_key.table, proc_node))
				schema.parseChildNode2Json(proc_node, table, nested_key.asOfChild(this));

		}

	}

}
