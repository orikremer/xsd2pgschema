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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import javax.xml.transform.TransformerException;

import org.apache.commons.text.StringEscapeUtils;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;

/**
 * Node parser for data (CSV/TSV) conversion.
 *
 * @author yokochi
 */
public class PgSchemaNode2PgCsv extends PgSchemaNodeParser {

	/** The string builder for a line of CSV/TSV format. */
	private StringBuilder sb;

	/** The buffered writer for data conversion. */
	private BufferedWriter buffw;

	/** Whether to use TSV format in PostgreSQL data migration. */
	private boolean pg_tab_delimiter;

	/** The current delimiter code. */
	private char pg_delimiter;

	/** The current null code. */
	private String pg_null;

	/** The content of fields. */
	private String[] values;

	/**
	 * Parse root node and write to data (CSV/TSV) file.
	 *
	 * @param npb node parser builder
	 * @param table current table
	 * @param root_node root node
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2PgCsv(final PgSchemaNodeParserBuilder npb, final PgTable table, final Node root_node) throws PgSchemaException {

		super(npb, null, table);

		if (table.writable) {

			as_attr = false;

			sb = new StringBuilder();

			buffw = table.buffw;

			pg_tab_delimiter = npb.schema.option.pg_tab_delimiter;
			pg_delimiter = npb.schema.option.pg_delimiter;
			pg_null = npb.schema.option.pg_null;

			values = new String[fields_size];

			Arrays.fill(values, pg_null);

		}

		parseRootNode(root_node);

		clear();

	}

	/**
	 * Node parser for CSV/TSV conversion.
	 *
	 * @param npb node parser builder
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @throws PgSchemaException the pg schema exception
	 */
	protected PgSchemaNode2PgCsv(final PgSchemaNodeParserBuilder npb, final PgTable parent_table, final PgTable table, final boolean as_attr) throws PgSchemaException {

		super(npb, parent_table, table);

		if (table.writable) {

			this.as_attr = as_attr;

			sb = new StringBuilder();

			buffw = table.buffw;

			pg_tab_delimiter = npb.schema.option.pg_tab_delimiter;
			pg_delimiter = npb.schema.option.pg_delimiter;
			pg_null = npb.schema.option.pg_null;

			values = new String[fields_size];

			Arrays.fill(values, pg_null);

		}

	}

	/**
	 * Traverse nested node.
	 *
	 * @param parent_node parent node
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void traverseNestedNode(final Node parent_node, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		PgSchemaNode2PgCsv node_parser = new PgSchemaNode2PgCsv(npb, table, nested_key.table, nested_key.as_attr);
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

		if (!table.writable) {

			if (total_nested_fields > 0)
				table.nested_fields.forEach(field -> setNestedKey(proc_node, field));

			return;
		}

		if (visited) {

			not_complete = null_simple_list = false;

			Arrays.fill(values, pg_null);

		}

		PgField field;

		if (npb.rel_data_ext) {

			for (int f = 0; f < fields_size; f++) {

				field = fields.get(f);

				if (field.omissible)
					continue;

				// document_key

				else if (field.document_key)
					values[f] = npb.document_id;

				// primary_key

				else if (field.primary_key)
					values[f] = npb.getHashKeyString(node_test.primary_key);

				// foreign_key

				else if (field.foreign_key) {

					if (parent_table.xname.equals(field.foreign_table_xname))
						values[f] = npb.getHashKeyString(node_test.parent_key);

				}

				// nested_key

				else if (field.nested_key) {

					String nested_key;

					if ((nested_key = setNestedKey(proc_node, field)) != null)
						values[f] = npb.getHashKeyString(nested_key);

				}

				// attribute, simple_content, element

				else if (field.content_holder) {

					if (setContent(proc_node, field, true)) {

						if (!content.isEmpty())
							values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(content) : StringEscapeUtils.escapeCsv(content);

					} else if (field.required) {

						not_complete = true;

						return;
					}

				}

				// any, any_attribute

				else if (field.any_content_holder) {

					try {

						if (npb.setAnyContent(proc_node, table, field))
							values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(npb.content) : StringEscapeUtils.escapeCsv(npb.content);

					} catch (TransformerException | IOException | SAXException e) {
						throw new PgSchemaException(e);
					}

				}

				// serial_key

				else if (field.serial_key) {
					values[f] = npb.is_def_ser_size ? Integer.toString(node_test.node_ordinal) : Short.toString((short) node_test.node_ordinal);
				}

				// xpath_key

				else if (field.xpath_key)
					values[f] = npb.getHashKeyString(current_key.substring(npb.document_id_len));

			}

		}

		else {

			for (int f = 0; f < fields_size; f++) {

				field = fields.get(f);

				// nested_key should be processed

				if (field.nested_key)
					setNestedKey(proc_node, field);

				if (field.omissible)
					continue;

				// document_key

				else if (field.document_key)
					values[f] = npb.document_id;

				// attribute, simple_content, element

				else if (field.content_holder) {

					if (setContent(proc_node, field, true)) {

						if (!content.isEmpty())
							values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(content) : StringEscapeUtils.escapeCsv(content);

					} else if (field.required) {

						not_complete = true;

						return;
					}

				}

				// any, any_attribute

				else if (field.any_content_holder) {

					try {

						if (npb.setAnyContent(proc_node, table, field))
							values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(npb.content) : StringEscapeUtils.escapeCsv(npb.content);

					} catch (TransformerException | IOException | SAXException e) {
						throw new PgSchemaException(e);
					}

				}

				// serial_key

				else if (field.serial_key) {
					values[f] = npb.is_def_ser_size ? Integer.toString(node_test.node_ordinal) : Short.toString((short) node_test.node_ordinal);
				}

				// xpath_key

				else if (field.xpath_key)
					values[f] = npb.getHashKeyString(current_key.substring(npb.document_id_len));

			}

		}

		if (null_simple_list && (total_nested_fields == 0 || nested_keys.size() == 0))
			return;

		try {

			for (int f = 0; f < fields_size; f++) {

				if (fields.get(f).omissible)
					continue;

				sb.append(values[f] + pg_delimiter);

			}

			if (buffw == null)
				buffw = table.buffw = Files.newBufferedWriter(table.pathw);

			buffw.write(sb.substring(0, sb.length() - 1) + "\n");

			sb.setLength(0);

		} catch (IOException e) {
			System.err.println("Exception occurred while processing table: " + table.xname);
			throw new PgSchemaException(e);
		}

		table.visited_key = current_key;

	}

}
