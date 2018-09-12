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

	/** The JSON builder. */
	private JsonBuilder jsonb;

	/** The JSON type. */
	private JsonType type;

	/** Whether any content was written. */
	private boolean written = false;

	/** The JSON Schema version. */
	private JsonSchemaVersion schema_ver;

	/** The JSON key value space with concatenation. */
	private String concat_value_space;

	/** The content of fields. */
	private String[] values;

	/**
	 * Node parser for JSON conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param md_hash_key instance of message digest
	 * @param document_id document id
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @param jsonb JSON builder
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2Json(final PgSchema schema, final MessageDigest md_hash_key, final String document_id, final PgTable parent_table, final PgTable table, final JsonBuilder jsonb) throws PgSchemaException {

		super(schema, md_hash_key, document_id, parent_table, table, PgSchemaNodeParserType.json_conversion);

		this.jsonb = jsonb;

		type = jsonb.type;

		if (table.jsonable) {

			schema_ver = jsonb.schema_ver;

			concat_value_space = jsonb.concat_value_space;

			values = new String[fields_size];

		}

	}

	/**
	 * Parse root node: Column- or Object-oriented JSON format
	 *
	 * @param root_node root node
	 * @param indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public void parseRootNode(final Node root_node, int indent_level) throws PgSchemaException {

		parse(new PgSchemaNodeTester(root_node, current_key = document_id + "/" + table.xname));

		if (not_complete)
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
						traverseNestedNodeCol(root_node, nested_key.asOfRoot(this), indent_level + (table.virtual ? 0 : 1));
					break;
				default: // object
					for (PgSchemaNestedKey nested_key : nested_keys)
						traverseNestedNodeObj(root_node, nested_key.asOfRoot(this), indent_level + (table.relational ? 0 : 1));
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

		boolean virtual = table.virtual;
		boolean as_attr = nested_key.as_attr;
		boolean last_node = isLastNode(node_test, nested_key);

		switch (type) {
		case column:
			boolean start_table = !virtual && table.list_holder && table.bridge;

			if (start_table) {

				jsonb.writeStartTable(table, true, indent_level);
				jsonb.writeFields(table, as_attr, indent_level + (virtual ? 0 : 1));

			}

			else if (last_node) {

				if (!not_complete && table.jsonb_not_empty)
					jsonb.writeFields(table, as_attr, indent_level + (virtual ? 0 : 1));

			}

			if ((start_table || last_node) && !not_complete) {

				visited = true;

				if (nested_keys != null) {

					Node test_node = node_test.proc_node;

					boolean exists;

					for (PgSchemaNestedKey _nested_key : nested_keys) {

						exists = existsNestedNode(test_node, _nested_key.table);

						traverseNestedNodeCol(exists || indirect ? test_node : proc_node, _nested_key.asOfChild(node_test, exists), indent_level + (virtual ? 0 : 1));

					}

				}

			}

			if (start_table)
				jsonb.writeEndTable();
			break;
		case object:
			if (written) {

				if (!virtual) {

					jsonb.writeStartTable(table, true, indent_level);
					jsonb.writeFields(table, as_attr, indent_level + 1);
					jsonb.writeEndTable();

				}

				else
					jsonb.writeFields(table, as_attr, indent_level + 1);

			}

			if (last_node && !not_complete) {

				visited = true;

				if (nested_keys != null) {

					Node test_node = node_test.proc_node;

					boolean exists;

					for (PgSchemaNestedKey _nested_key : nested_keys) {

						exists = existsNestedNode(test_node, _nested_key.table);

						traverseNestedNodeObj(exists || indirect ? test_node : proc_node, _nested_key.asOfChild(node_test, exists), indent_level + (table.relational ? 0 : 1));

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
	 * @param node current node
	 * @param nested_key nested key
	 * @param indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseChildNode(final Node node, final PgSchemaNestedKey nested_key, int indent_level) throws PgSchemaException {

		parse(new PgSchemaNodeTester(node, nested_key));

		boolean virtual = table.virtual;
		boolean as_attr = nested_key.as_attr;

		switch (type) {
		case column:
			if (written)
				jsonb.writeFields(table, as_attr, indent_level + (virtual ? 0 : 1));

			if (not_complete || nested_keys == null)
				return;

			for (PgSchemaNestedKey _nested_key : nested_keys) {

				if (existsNestedNode(node, _nested_key.table))
					traverseNestedNodeCol(node, _nested_key.asOfChild(this), indent_level + (virtual ? 0 : 1));

			}
			break;
		case object:
			if (written) {

				if (!virtual)
					jsonb.writeStartTable(table, !parent_table.bridge, indent_level);

				jsonb.writeFields(table, as_attr, indent_level + 1);

			}

			if (!not_complete && nested_keys != null) {

				for (PgSchemaNestedKey _nested_key : nested_keys) {

					if (existsNestedNode(node, _nested_key.table))
						traverseNestedNodeObj(node, _nested_key.asOfChild(this), indent_level + (table.relational ? 0 : 1));

				}

			}

			if (!virtual)
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

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(schema, md_hash_key, document_id, table, nested_key.table, jsonb);

		try {

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(parent_node, node, table, nested_key, node2json);

				if (node_test.omissible)
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

		PgTable current_table = nested_key.table;

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(schema, md_hash_key, document_id, table, current_table, jsonb);

		try {

			boolean start_table = !current_table.virtual && !(current_table.list_holder && current_table.bridge);

			if (start_table)
				jsonb.writeStartTable(current_table, true, json_indent_level);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(parent_node, node, table, nested_key, node2json);

				if (node_test.omissible)
					continue;

				if (node2json.parseChildNode(node_test, nested_key, json_indent_level))
					break;

			}

			try {

				if (node2json.visited)
					return;

				node2json.parseChildNode(parent_node, nested_key, json_indent_level);

			} finally {

				if (start_table)
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

		PgTable current_table = nested_key.table;

		PgSchemaNode2Json node2json = new PgSchemaNode2Json(schema, md_hash_key, document_id, table, current_table, jsonb);

		try {

			boolean start_table = !current_table.virtual && current_table.bridge;

			if (start_table) {

				jsonb.writeStartTable(current_table, true, ++json_indent_level);
				++json_indent_level;

			}

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(parent_node, node, table, nested_key, node2json);

				if (node_test.omissible)
					continue;

				if (node2json.parseChildNode(node_test, nested_key, json_indent_level))
					break;

			}

			try {

				if (node2json.visited)
					return;

				node2json.parseChildNode(parent_node, nested_key, json_indent_level);

			} finally {

				if (start_table)
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

		if (table.visited_key.equals(current_key = node_test.current_key))
			return;

		proc_node = node_test.proc_node;
		indirect = node_test.indirect;

		if (!table.jsonable) {

			fields.stream().filter(field -> field.nested_key).forEach(field -> setNestedKey(proc_node, field, current_key));

			return;
		}

		if (node_test.node_ordinal > 1) {

			not_complete = null_simple_list = false;

			Arrays.fill(values, null);

			if (nested_keys != null)
				nested_keys.clear();

		}

		for (int f = 0; f < fields_size; f++) {

			PgField field = fields.get(f);

			// nested_key

			if (field.nested_key)
				setNestedKey(proc_node, field, current_key);

			else if (field.jsonable) {

				// attribute, simple_content, element

				if (field.attribute || field.simple_content || field.element) {

					if (setContent(proc_node, field, current_key, node_test.as_attr, false))
						values[f] = content;

					else if (field.required) {

						not_complete = true;

						return;
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

			}

		}

		if (null_simple_list && (nested_keys == null || nested_keys.size() == 0))
			return;

		written = true;

		boolean not_empty = false;

		for (int f = 0; f < fields_size; f++) {

			PgField field = fields.get(f);

			if (field.jsonable && field.writeValue2JsonBuf(schema_ver, values[f], false, concat_value_space))
				not_empty = true;

		}

		if (not_empty && !table.jsonb_not_empty)
			table.jsonb_not_empty = true;

		table.visited_key = current_key;

	}

}
