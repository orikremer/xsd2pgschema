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

package net.sf.xsd2pgschema.nodeparser;

import java.io.IOException;
import java.util.Arrays;

import javax.xml.transform.TransformerException;

import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchema;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgTable;
import net.sf.xsd2pgschema.docbuilder.JsonBuilder;
import net.sf.xsd2pgschema.docbuilder.JsonSchemaVersion;
import net.sf.xsd2pgschema.docbuilder.JsonType;

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

	/** Whether bridge table | virtual table | !content_holder. */
	public boolean relational;

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
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2Json(final PgSchema schema, final PgTable parent_table, final PgTable table, final boolean as_attr) throws PgSchemaException {

		super(schema, parent_table, table, PgSchemaNodeParserType.json_conversion);

		jsonb = schema.jsonb;
		type = jsonb.type;

		relational = table.relational;

		if (table.jsonable) {

			this.as_attr = as_attr;

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

		node_test.setRootNode(root_node, document_id + "/" + table.xname);

		parse();

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
						traverseNestedNodeCol(root_node, nested_key.asIs(this), indent_level + (virtual ? 0 : 1));
					break;
				default: // object
					for (PgSchemaNestedKey nested_key : nested_keys)
						traverseNestedNodeObj(root_node, nested_key.asIs(this), indent_level + (relational ? 0 : 1));
				}

			}

			jsonb.writeEndTable();
			break;
		default:
		}

	}

	/**
	 * Parse processing node: Column- or Object-oriented JSON format
	 *
	 * @param indent_level current indent level
	 * @return boolean whether the node is the last one
	 * @throws PgSchemaException the pg schema exception
	 */
	protected boolean parseProcNode(int indent_level) throws PgSchemaException {

		parse();

		boolean last_node = node_test.isLastNode();

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

				if (nested_keys != null && nested_keys.size() > 0) {

					Node proc_node = node_test.proc_node;

					for (PgSchemaNestedKey nested_key : nested_keys)
						traverseNestedNodeCol(proc_node, nested_key.asOfChild(this), indent_level + (virtual ? 0 : 1));

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

				if (nested_keys != null && nested_keys.size() > 0) {

					Node proc_node = node_test.proc_node;

					for (PgSchemaNestedKey nested_key : nested_keys)
						traverseNestedNodeObj(proc_node, nested_key.asOfChild(this), indent_level + (relational ? 0 : 1));

				}

			}
			break;
		default:
		}

		return last_node;
	}

	/**
	 * Parse current node: Column- or Object-oriented JSON format
	 *
	 * @param node current node
	 * @param indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	protected void parseNode(final Node node, int indent_level) throws PgSchemaException {

		node_test.setProcNode(node);

		parse();

		switch (type) {
		case column:
			if (written)
				jsonb.writeFields(table, as_attr, indent_level + (virtual ? 0 : 1));

			if (not_complete || nested_keys == null)
				return;

			for (PgSchemaNestedKey nested_key : nested_keys) {

				if (existsNestedNode(node, nested_key.table))
					traverseNestedNodeCol(node, nested_key.asIs(this), indent_level + (virtual ? 0 : 1));

			}
			break;
		case object:
			if (written) {

				if (!virtual)
					jsonb.writeStartTable(table, !parent_table.bridge, indent_level);

				jsonb.writeFields(table, as_attr, indent_level + 1);

			}

			if (!not_complete && nested_keys != null) {

				for (PgSchemaNestedKey nested_key : nested_keys) {

					if (existsNestedNode(node, nested_key.table))
						traverseNestedNodeObj(node, nested_key.asIs(this), indent_level + (relational ? 0 : 1));

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

		PgSchemaNode2Json node_parser = new PgSchemaNode2Json(schema, table, nested_key.table, nested_key.as_attr);
		PgSchemaNodeTester node_test = node_parser.node_test;

		node_test.prepForTraversal(table, parent_node, nested_key);

		try {

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (node_test.isOmissibleNode(node))
					continue;

				if (node_parser.parseProcNode())
					break;

			}

			if (node_parser.visited)
				return;

			node_parser.parseNode(parent_node);

		} finally {
			node_parser.clear();
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

		PgSchemaNode2Json node_parser = new PgSchemaNode2Json(schema, table, current_table, nested_key.as_attr);
		PgSchemaNodeTester node_test = node_parser.node_test;

		node_test.prepForTraversal(table, parent_node, nested_key);

		try {

			boolean start_table = !current_table.virtual && !(current_table.list_holder && current_table.bridge);

			if (start_table)
				jsonb.writeStartTable(current_table, true, json_indent_level);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (node_test.isOmissibleNode(node))
					continue;

				if (node_parser.parseProcNode(json_indent_level))
					break;

			}

			try {

				if (node_parser.visited)
					return;

				node_parser.parseNode(parent_node, json_indent_level);

			} finally {

				if (start_table)
					jsonb.writeEndTable();

			}

		} finally {
			node_parser.clear();
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

		PgSchemaNode2Json node_parser = new PgSchemaNode2Json(schema, table, current_table, nested_key.as_attr);
		PgSchemaNodeTester node_test = node_parser.node_test;

		node_test.prepForTraversal(table, parent_node, nested_key);

		try {

			boolean start_table = !current_table.virtual && current_table.bridge;

			if (start_table) {

				jsonb.writeStartTable(current_table, true, ++json_indent_level);
				++json_indent_level;

			}

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (node_test.isOmissibleNode(node))
					continue;

				if (node_parser.parseProcNode(json_indent_level))
					break;

			}

			try {

				if (node_parser.visited)
					return;

				node_parser.parseNode(parent_node, json_indent_level);

			} finally {

				if (start_table)
					jsonb.writeEndTable();

			}

		} finally {
			node_parser.clear();
		}

	}

	/**
	 * Parse processing node.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void parse() throws PgSchemaException {

		if (table.visited_key.equals(current_key = node_test.proc_key))
			return;

		if (table.has_path_restriction)
			extractParentAncestorNodeName();

		Node proc_node = node_test.proc_node;

		clear();

		if (!table.jsonable) {

			fields.stream().filter(field -> field.nested_key).forEach(field -> setNestedKey(proc_node, field));

			return;
		}

		if (visited) {

			not_complete = null_simple_list = false;

			Arrays.fill(values, null);

		}

		PgField field;

		for (int f = 0; f < fields_size; f++) {

			field = fields.get(f);

			// nested_key

			if (field.nested_key)
				setNestedKey(proc_node, field);

			else if (field.jsonable) {

				// attribute, simple_content, element

				if (field.content_holder) {

					if (setContent(proc_node, field, false))
						values[f] = content;

					else if (field.required) {

						not_complete = true;

						return;
					}

				}

				// any, any_attribute

				else if (field.any_content_holder) {

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

			field = fields.get(f);

			if (field.jsonable && field.write(schema_ver, values[f], false, concat_value_space))
				not_empty = true;

		}

		if (not_empty && !table.jsonb_not_empty)
			table.jsonb_not_empty = true;

		table.visited_key = current_key;

	}

}
