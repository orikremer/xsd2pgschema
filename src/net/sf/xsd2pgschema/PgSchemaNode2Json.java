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
import java.security.MessageDigest;
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

	/** The JSON key value space with concatenation. */
	private String concat_value_space;

	/**
	 * Node parser for JSON conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param md_hash_key instance of message digest
	 * @param parent_table parent table
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2Json(final PgSchema schema, final MessageDigest md_hash_key, final PgTable parent_table, final PgTable table) throws PgSchemaException {

		super(schema, md_hash_key, parent_table, table, PgSchemaNodeParserType.json_conversion);

		jsonb = schema.jsonb;

		type = jsonb.type;

		schema_ver = jsonb.schema_ver;

		concat_value_space = jsonb.concat_value_space;

	}

	/**
	 * Parse root node: Column- or Object-oriented JSON format
	 *
	 * @param proc_node processing node
	 * @param indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void parseRootNode(final Node proc_node, int indent_level) throws PgSchemaException {

		parse(new PgSchemaNodeTester(proc_node, current_key = document_id + "/" + table.xname));

		if (!filled)
			return;

		switch (type) {
		case column:
		case object:
			jsonb.writeStartTable(table, true, indent_level);
			jsonb.writeFields(table, false, indent_level + 1);

			if (nested_keys != null) {

				switch (type) {
				case column:
					for (PgSchemaNestedKey nested_key : nested_keys)
						traverseNestedNodeCol(proc_node, nested_key.asOfRoot(this), indent_level + (table.virtual ? 0 : 1));
					break;
				default: // object
					for (PgSchemaNestedKey nested_key : nested_keys)
						traverseNestedNodeObj(proc_node, nested_key.asOfRoot(this), indent_level + (table.relational ? 0 : 1));
				}

			}

			jsonb.writeEndTable();
			break;
		default:
		}

	}

	/**
	 * Parse child node: Column- or Object-oriented JSON format
	 *
	 * @param node_test node tester
	 * @param nested_key nested key
	 * @param indent_level current indent level
	 * @return boolean whether current node is the last one
	 * @throws PgSchemaException the pg schema exception
	 */
	protected boolean parseChildNode(final PgSchemaNodeTester node_test, final PgSchemaNestedKey nested_key, int indent_level) throws PgSchemaException {

		parse(node_test);

		boolean as_attr = nested_key.as_attr;
		boolean last_node = isLastNode(nested_key, node_test.node_count);

		switch (type) {
		case column:
			boolean list_and_bridge = !table.virtual && table.list_holder && table.bridge;

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

						for (PgSchemaNestedKey _nested_key : nested_keys) {

							boolean exists = existsNestedNode(_nested_key.table, node_test.proc_node);

							traverseNestedNodeCol(exists || indirect ? node_test.proc_node : proc_node, _nested_key.asOfChild(node_test, exists), indent_level + (table.virtual ? 0 : 1));

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

			if (last_node) {

				if (filled) {

					visited = true;

					if (nested_keys != null) {

						for (PgSchemaNestedKey _nested_key : nested_keys) {

							boolean exists = existsNestedNode(_nested_key.table, node_test.proc_node);

							traverseNestedNodeObj(exists || indirect ? node_test.proc_node : proc_node, _nested_key.asOfChild(node_test, exists), indent_level + (table.relational ? 0 : 1));

						}

					}

				}

			}
			break;
		default:
		}

		return last_node;
	}

	/**
	 * Parse child node: Column- or Object-oriented JSON format
	 *
	 * @param proc_node processing node
	 * @param nested_key nested key
	 * @param indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode(final Node proc_node, final PgSchemaNestedKey nested_key, int indent_level) throws PgSchemaException {

		boolean as_attr = nested_key.as_attr;

		parse(new PgSchemaNodeTester(proc_node, nested_key));

		switch (type) {
		case column:
			if (written)
				jsonb.writeFields(table, as_attr, indent_level + (table.virtual ? 0 : 1));

			if (!filled || nested_keys == null)
				return;

			for (PgSchemaNestedKey _nested_key : nested_keys) {

				if (existsNestedNode(_nested_key.table, proc_node))
					traverseNestedNodeCol(proc_node, _nested_key.asOfChild(this), indent_level + (table.virtual ? 0 : 1));

			}
			break;
		case object:
			if (written) {

				if (!table.virtual)
					jsonb.writeStartTable(table, !parent_table.bridge, indent_level);

				jsonb.writeFields(table, as_attr, indent_level + 1);

			}

			if (filled && nested_keys != null) {

				for (PgSchemaNestedKey _nested_key : nested_keys) {

					if (existsNestedNode(_nested_key.table, proc_node))
						traverseNestedNodeObj(proc_node, _nested_key.asOfChild(this), indent_level + (table.relational ? 0 : 1));

				}

			}

			if (!table.virtual)
				jsonb.writeEndTable();
			break;
		default:
		}

	}

	/**
	 * Traverse nested node: Relational-oriented JSON format
	 *
	 * @param parent_node parent node
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void traverseNestedNode(final Node parent_node, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(schema, md_hash_key, table, nested_key.table);

		try {

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(parent_node, node, table, nested_key, node2json.node_count, node2json.node_ordinal);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				if (node2json.parseChildNode(node_test, nested_key))
					break;

			}

			if (node2json.visited)
				return;

			node2json.parseChildNode(parent_node, nested_key);

		} finally {
			node2json.clear();
		}

	}

	/**
	 * Traverse nested node: Column-oriented JSON format
	 *
	 * @param parent_node parent node
	 * @param nested_key nested key
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void traverseNestedNodeCol(final Node parent_node, final PgSchemaNestedKey nested_key, int json_indent_level) throws PgSchemaException {

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(schema, md_hash_key, table, nested_key.table);

		try {

			boolean list_and_bridge = table.list_holder && table.bridge;

			if (!table.virtual && !list_and_bridge)
				jsonb.writeStartTable(table, true, json_indent_level);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(parent_node, node, table, nested_key, node2json.node_count, node2json.node_ordinal);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				if (node2json.parseChildNode(node_test, nested_key, json_indent_level))
					break;

			}

			try {

				if (node2json.visited)
					return;

				node2json.parseChildNode(parent_node, nested_key, json_indent_level);

			} finally {

				if (!table.virtual && !list_and_bridge)
					jsonb.writeEndTable();

			}

		} finally {
			node2json.clear();
		}

	}

	/**
	 * Traverse nested node: Object-oriented JSON format
	 *
	 * @param parent_node parent node
	 * @param nested_key nested key
	 * @param json_indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void traverseNestedNodeObj(final Node parent_node, final PgSchemaNestedKey nested_key, int json_indent_level) throws PgSchemaException {

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(schema, md_hash_key, table, nested_key.table);

		try {

			if (!table.virtual && table.bridge) {
				jsonb.writeStartTable(table, true, ++json_indent_level);
				++json_indent_level;
			}

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(parent_node, node, table, nested_key, node2json.node_count, node2json.node_ordinal);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
					continue;

				if (node2json.parseChildNode(node_test, nested_key, json_indent_level))
					break;

			}

			try {

				if (node2json.visited)
					return;

				node2json.parseChildNode(parent_node, nested_key, json_indent_level);

			} finally {

				if (!table.virtual && table.bridge)
					jsonb.writeEndTable();

			}

		} finally {
			node2json.clear();
		}

	}

	/**
	 * Parse processing node.
	 *
	 * @param node_test node tester
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void parse(final PgSchemaNodeTester node_test) throws PgSchemaException {

		proc_node = node_test.proc_node;
		current_key = node_test.current_key;
		indirect = node_test.indirect;

		Arrays.fill(values, "");

		filled = true;
		null_simple_primitive_list = false;

		if (nested_keys != null)
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

				if (setContent(proc_node, field, current_key, node_test.as_attr, false))
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

		written = true;

		boolean not_empty = false;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.jsonb == null)
				continue;

			if (field.jsonable && field.writeValue2JsonBuf(schema_ver, values[f], false, concat_value_space))
				not_empty = true;

		}

		if (not_empty && !table.jsonb_not_empty)
			table.jsonb_not_empty = true;

	}

}
