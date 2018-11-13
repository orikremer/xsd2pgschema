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

	/** Whether any content was written. */
	private boolean written = false;

	/** The JSON Schema version. */
	private JsonSchemaVersion schema_ver;

	/** The JSON key value space with concatenation. */
	private String concat_value_space;

	/** The content of fields. */
	private String[] values;

	/**
	 * Parse root node and store to JSON buffer: Relational-oriented JSON format
	 *
	 * @param npb node parser builder
	 * @param table current table
	 * @param root_node root node
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2Json(final PgSchemaNodeParserBuilder npb, final PgTable table, final Node root_node) throws PgSchemaException {

		super(npb, null, table);

		jsonb = npb.jsonb;
		type = jsonb.type;

		if (table.jsonable) {

			as_attr = false;

			schema_ver = jsonb.schema_ver;
			concat_value_space = jsonb.concat_value_space;

			values = new String[fields_size];

		}

		parseRootNode(root_node);

		clear();

	}

	/**
	 * Parse root node and store to JSON buffer: Column- or Object-oriented JSON format
	 *
	 * @param npb node parser builder
	 * @param table current table
	 * @param root_node root node
	 * @param indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2Json(final PgSchemaNodeParserBuilder npb, final PgTable table, final Node root_node, final int indent_level) throws PgSchemaException {

		super(npb, null, table);

		jsonb = npb.jsonb;
		type = jsonb.type;

		if (table.jsonable)
			init(false);

		parseRootNode(root_node, indent_level);

		clear();

	}

	/**
	 * Node parser for JSON conversion.
	 *
	 * @param npb node parser builder
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @throws PgSchemaException the pg schema exception
	 */
	protected PgSchemaNode2Json(final PgSchemaNodeParserBuilder npb, final PgTable parent_table, final PgTable table, final boolean as_attr) throws PgSchemaException {

		super(npb, parent_table, table);

		jsonb = npb.jsonb;
		type = jsonb.type;

		if (table.jsonable)
			init(as_attr);

	}

	/**
	 * Initialize node parser.
	 *
	 * @param as_attr whether parent node as attribute
	 */
	@Override
	protected void init(boolean as_attr) {

		this.as_attr = as_attr;

		schema_ver = jsonb.schema_ver;
		concat_value_space = jsonb.concat_value_space;

		values = new String[fields_size];

	}

	/**
	 * Parse root node: Column- or Object-oriented JSON format
	 *
	 * @param root_node root node
	 * @param indent_level current indent level
	 * @throws PgSchemaException the pg schema exception
	 */
	private void parseRootNode(final Node root_node, int indent_level) throws PgSchemaException {

		node_test.setRootNode(root_node, npb.document_id + "/" + table.xname);

		parse();

		if (not_complete)
			return;

		switch (type) {
		case column:
		case object:
			jsonb.writeStartTable(table, true, indent_level);
			jsonb.writeFields(table, false, indent_level + 1);

			if (total_nested_fields > 0) {

				switch (type) {
				case column:
					for (PgSchemaNestedKey nested_key : nested_keys)
						traverseNestedNodeCol(root_node, nested_key.asIs(this), indent_level + (virtual ? 0 : 1));
					break;
				default: // object
					boolean relational = table.relational;

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

				if (total_nested_fields > 0 && nested_keys.size() > 0) {

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

				if (total_nested_fields > 0 && nested_keys.size() > 0) {

					Node proc_node = node_test.proc_node;
					boolean relational = table.relational;

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

			if (not_complete || total_nested_fields == 0)
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

			if (!not_complete && total_nested_fields > 0) {

				boolean relational = table.relational;

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

		PgSchemaNode2Json node_parser = new PgSchemaNode2Json(npb, table, nested_key.table, nested_key.as_attr);
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

		PgSchemaNode2Json node_parser = new PgSchemaNode2Json(npb, table, current_table, nested_key.as_attr);
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

		PgSchemaNode2Json node_parser = new PgSchemaNode2Json(npb, table, current_table, nested_key.as_attr);
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

			if (total_nested_fields > 0)
				table.nested_fields.forEach(field -> setNestedKey(proc_node, field));

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

						if (npb.setAnyContent(proc_node, table, field)) {

							values[f] = npb.any_content.toString().trim();
							npb.any_content.setLength(0);

						}

					} catch (TransformerException | IOException | SAXException e) {
						throw new PgSchemaException(e);
					}

				}

			}

		}

		if (null_simple_list && (total_nested_fields == 0 || nested_keys.size() == 0))
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
