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

import javax.xml.transform.TransformerException;

import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * Node parser for JSON conversion.
 *
 * @author yokochi
 */
public class PgSchemaNode2Json extends PgSchemaNodeParser {

	/** Whether any content was written. */
	protected boolean written = false;

	/** The JSON builder. */
	protected JsonBuilder jsonb;

	/** The JSON type. */
	protected JsonType type;

	/** The JSON Schema version. */
	protected JsonSchemaVersion schema_ver;

	/** The white spaces between JSON item and JSON data. */
	private String key_value_space;

	/**
	 * Node parser for JSON conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table
	 * @param table current table
	 */
	public PgSchemaNode2Json(final PgSchema schema, final PgTable parent_table, final PgTable table) {

		super(schema, parent_table, table, PgSchemaNodeParserType.json_conversion);

		jsonb = schema.jsonb;

		type = jsonb.type;

		schema_ver = jsonb.schema_ver;

		key_value_space = jsonb.key_value_space;

	}

	/**
	 * Parse processing node (root): Relational-oriented JSON format
	 *
	 * @param proc_node processing node
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void parseRootNode(final Node proc_node) throws PgSchemaException {

		parse(proc_node, current_key = document_id + "/" + table.xname, false, indirect);

		if (!filled)
			return;

		if (nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys)
			schema.parseChildNode2Json(proc_node, table, nested_key.asOfRoot(this));

	}

	/**
	 * Parse processing node (root).
	 *
	 * @param proc_node processing node
	 * @param indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void parseRootNode(final Node proc_node, int indent_level) throws PgSchemaException {

		parse(proc_node, current_key = document_id + "/" + table.xname, false, indirect);

		if (!filled)
			return;

		switch (type) {
		case relational:
			if (nested_keys == null)
				return;

			for (PgSchemaNestedKey nested_key : nested_keys)
				schema.parseChildNode2Json(proc_node, table, nested_key.asOfRoot(this));
			break;
		default: // column or object
			jsonb.writeStartTable(table, true, indent_level);
			jsonb.writeFields(table, false, indent_level + 1);

			if (nested_keys != null) {

				switch (type) {
				case column:
					for (PgSchemaNestedKey nested_key : nested_keys)
						schema.parseChildNode2ColJson(proc_node, table, nested_key.asOfRoot(this), indent_level + (table.virtual ? 0 : 1));
					break;
				default: // object
					for (PgSchemaNestedKey nested_key : nested_keys)
						schema.parseChildNode2ObjJson(proc_node, table, nested_key.asOfRoot(this), indent_level + (table.relational ? 0 : 1));
				}

			}

			jsonb.writeEndTable();
		}

	}

	/**
	 * Parse processing node (child): Relational-oriented JSON format
	 *
	 * @param node_test node tester
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void parseChildNode(final PgSchemaNodeTester node_test) throws PgSchemaException {

		parse(node_test.proc_node, node_test.current_key, node_test.as_attr, node_test.indirect);

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
	 * Parse processing node (child): Column- or Object-oriented JSON format
	 *
	 * @param node_test node tester
	 * @param as_attr whether parent node as attribute
	 * @param indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void parseChildNode(final PgSchemaNodeTester node_test, boolean as_attr, int indent_level) throws PgSchemaException {

		parse(node_test.proc_node, node_test.current_key, node_test.as_attr, node_test.indirect);

		switch (type) {
		case column:
			boolean list_and_bridge = !table.virtual && table.list_holder && table.bridge;
			boolean last_node = node_test.isLastNode();

			if (list_and_bridge) {

				jsonb.writeStartTable(table, true, indent_level);
				jsonb.writeFields(table, as_attr, indent_level + (table.virtual ? 0 : 1));

			}

			else if (last_node) {

				if (filled && table.jsonb_not_empty)
					jsonb.writeFields(table, as_attr, indent_level + (table.virtual ? 0 : 1));

			}

			if (list_and_bridge || last_node) {

				if (filled) {

					visited = true;

					if (nested_keys != null) {

						for (PgSchemaNestedKey nested_key : nested_keys) {

							boolean exists = existsNestedNode(nested_key.table, node_test.proc_node);

							schema.parseChildNode2ColJson(exists || indirect ? node_test.proc_node : proc_node, table, nested_key.asOfChild(node_test, exists), indent_level + (table.virtual ? 0 : 1));

						}

					}

				}

			}

			if (list_and_bridge)
				jsonb.writeEndTable();
			break;
		case object:
			if (written) {

				if (!table.virtual) {

					jsonb.writeStartTable(table, true, indent_level);
					jsonb.writeFields(table, as_attr, indent_level + 1);
					jsonb.writeEndTable();

				}

				else
					jsonb.writeFields(table, as_attr, indent_level + 1);

			}

			if (node_test.isLastNode()) {

				if (!filled)
					return;

				visited = true;

				if (nested_keys == null)
					return;

				for (PgSchemaNestedKey nested_key : nested_keys) {

					boolean exists = existsNestedNode(nested_key.table, node_test.proc_node);

					schema.parseChildNode2ObjJson(exists || indirect ? node_test.proc_node : proc_node, table, nested_key.asOfChild(node_test, exists), indent_level + (table.relational ? 0 : 1));

				}

			}
			break;
		default:
		}

	}

	/**
	 * Parse processing node (child): Relational-oriented JSON format
	 *
	 * @param proc_node processing node
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void parseChildNode(final Node proc_node, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		parse(proc_node, nested_key.current_key, nested_key.as_attr, nested_key.indirect);

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey _nested_key : nested_keys) {

			if (existsNestedNode(_nested_key.table, proc_node))
				schema.parseChildNode2Json(proc_node, table, _nested_key.asOfChild(this));

		}

	}

	/**
	 * Parse processing node (child): Column- or Object-oriented JSON format
	 *
	 * @param proc_node processing node
	 * @param nested_key nested key
	 * @param indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void parseChildNode(final Node proc_node, final PgSchemaNestedKey nested_key, int indent_level) throws PgSchemaException {

		parse(proc_node, nested_key.current_key, nested_key.as_attr, nested_key.indirect);

		switch (type) {
		case column:
			if (written)
				jsonb.writeFields(table, nested_key.as_attr, indent_level + (table.virtual ? 0 : 1));

			if (!filled || nested_keys == null)
				return;

			for (PgSchemaNestedKey _nested_key : nested_keys) {

				if (existsNestedNode(_nested_key.table, proc_node))
					schema.parseChildNode2ColJson(proc_node, table, _nested_key.asOfChild(this), indent_level + (table.virtual ? 0 : 1));

			}
			break;
		case object:
			if (written) {

				if (!table.virtual)
					jsonb.writeStartTable(table, !parent_table.bridge, indent_level);

				jsonb.writeFields(table, nested_key.as_attr, indent_level + 1);

			}

			if (filled && nested_keys != null) {

				for (PgSchemaNestedKey _nested_key : nested_keys) {

					if (existsNestedNode(_nested_key.table, proc_node))
						schema.parseChildNode2ObjJson(proc_node, table, _nested_key.asOfChild(this), indent_level + (table.relational ? 0 : 1));

				}

			}

			if (!table.virtual)
				jsonb.writeEndTable();
			break;
		default:
		}

	}

	/**
	 * Parse processing node.
	 *
	 * @param proc_node processing node
	 * @param current_key current key
	 * @param as_attr whether parent key as attribute
	 * @param indirect whether child node is not nested node
	 * @throws PgSchemaException the pg schema exception
	 */
	private void parse(final Node proc_node, final String current_key, final boolean as_attr, final boolean indirect) throws PgSchemaException {

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

				try {

					if (setAnyContent(proc_node, field)) {

						values[f] = any_content.toString().trim();
						any_content.setLength(0);

					}

				} catch (TransformerException | IOException | SAXException e) {
					throw new PgSchemaException(e);
				}

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
	private synchronized void write() {

		written = true;

		boolean not_empty = false;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.jsonb == null)
				continue;

			if (field.jsonable && field.writeValue2JsonBuf(schema_ver, values[f], false, key_value_space))
				not_empty = true;

		}

		if (not_empty && !table.jsonb_not_empty)
			table.jsonb_not_empty = true;

	}

}
