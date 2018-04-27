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

import org.apache.commons.text.StringEscapeUtils;
import org.w3c.dom.Node;

/**
 * Node parser for data (CSV/TSV) conversion.
 *
 * @author yokochi
 */
public class PgSchemaNode2PgCsv extends PgSchemaNodeParser {

	/** The string builder for a line of CSV/TSV format. */
	private StringBuilder sb = null;

	/** Whether use TAB delimiter code for PostgreSQL data migration. */
	private boolean pg_tab_delimiter = true;

	/**
	 * Node parser for CSV conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table
	 * @param table current table
	 * @throws ParserConfigurationException the parser configuration exception
	 * @throws TransformerConfigurationException the transformer configuration exception
	 */
	public PgSchemaNode2PgCsv(final PgSchema schema, final PgTable parent_table, final PgTable table) throws ParserConfigurationException, TransformerConfigurationException {

		super(schema, parent_table, table);

		sb = new StringBuilder();

		pg_tab_delimiter = option.pg_tab_delimiter;

	}

	/**
	 * Parse processing node (root).
	 *
	 * @param proc_node processing node
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws TransformerException the transformer exception
	 */
	@Override
	public void parseRootNode(final Node proc_node) throws IOException, TransformerException {

		current_key = document_id + "/" + table.name;

		parse(proc_node, null, current_key, current_key, nested, 1);

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

		parse(node_test.proc_node, node_test.parent_key, node_test.primary_key, node_test.current_key, node_test.nested, node_test.key_id);

	}

	/**
	 * Parser processing node (child).
	 *
	 * @param proc_node processing node
	 * @param parent_key key name of parent node
	 * @param proc_key processing key name
	 * @param nested whether it is nested
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws TransformerException the transformer exception
	 */
	@Override
	public void parseChildNode(final Node proc_node, final String parent_key, final String proc_key, final boolean nested) throws IOException, TransformerException {

		parse(proc_node, parent_key, proc_key, proc_key, nested, 1);

	}

	/**
	 * Parse processing node.
	 *
	 * @param proc_node processing node
	 * @param parent_key name of parent node
	 * @param primary_key name of primary key
	 * @param current_key name of current node
	 * @param nested whether it is nested
	 * @param key_id ordinal number of current node
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws TransformerException the transformer exception
	 */
	private void parse(final Node proc_node, final String parent_key, final String primary_key, final String current_key, final boolean nested, final int key_id) throws IOException, TransformerException {

		Arrays.fill(values, "");

		filled = true;

		nested_fields = 0;

		if (pg_tab_delimiter) {

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				// document_key

				if (field.document_key) {

					if (table.buffw != null)
						values[f] = document_id;

				}

				// serial_key

				else if (field.serial_key) {

					if (table.buffw != null)
						values[f] = option.ser_size.equals(PgSerSize.unsigned_int_32) ? Integer.toString(key_id) : Short.toString((short) key_id);

				}

				// xpath_key

				else if (field.xpath_key) {

					if (table.buffw != null)
						values[f] = schema.getHashKeyString(current_key.substring(document_id_len));

				}

				// primary_key

				else if (field.primary_key) {

					if (table.buffw != null && rel_data_ext)
						values[f] = schema.getHashKeyString(primary_key);

				}

				// foreign_key

				else if (field.foreign_key) {

					if (parent_table.name.equals(field.foreign_table_name)) {

						if (table.buffw != null && rel_data_ext)
							values[f] = schema.getHashKeyString(parent_key);

					}

				}

				// nested_key

				else if (field.nested_key) {

					if (setNestedKey(field, current_key, key_id)) {

						if (table.buffw != null && rel_data_ext)
							values[f] = schema.getHashKeyString(nested_key[nested_fields]);

						nested_fields++;

					}

				}

				// attribute, simple_content, element

				else if (field.attribute || field.simple_content || field.element) {

					if (setContent(proc_node, field, true)) {

						if (table.buffw != null)
							values[f] = field.normalize(content).replaceAll("\t", "\\t").replaceAll("\n", "\\n");

					} else if (field.required) {
						filled = false;
						break;
					}

				}

				// any, any_attribute

				else if ((field.any || field.any_attribute) && table.buffw != null) {

					if (setAnyContent(proc_node, field))
						values[f] = field.normalize(content).replaceAll("\t", "\\t").replaceAll("\n", "\\n");

				}

				if (!filled)
					break;

			}

		}

		else {

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				// document_key

				if (field.document_key) {

					if (table.buffw != null)
						values[f] = document_id;

				}

				// serial_key

				else if (field.serial_key) {

					if (table.buffw != null)
						values[f] = option.ser_size.equals(PgSerSize.unsigned_int_32) ? Integer.toString(key_id) : Short.toString((short) key_id);

				}

				// xpath_key

				else if (field.xpath_key) {

					if (table.buffw != null)
						values[f] = schema.getHashKeyString(current_key.substring(document_id_len));

				}

				// primary_key

				else if (field.primary_key) {

					if (table.buffw != null && rel_data_ext)
						values[f] = schema.getHashKeyString(primary_key);

				}

				// foreign_key

				else if (field.foreign_key) {

					if (parent_table.name.equals(field.foreign_table_name)) {

						if (table.buffw != null && rel_data_ext)
							values[f] = schema.getHashKeyString(parent_key);

					}

				}

				// nested_key

				else if (field.nested_key) {

					if (setNestedKey(field, current_key, key_id)) {

						if (table.buffw != null && rel_data_ext)
							values[f] = schema.getHashKeyString(nested_key[nested_fields]);

						nested_fields++;

					}

				}

				// attribute, simple_content, element

				else if (field.attribute || field.simple_content || field.element) {

					if (setContent(proc_node, field, true)) {

						if (table.buffw != null)
							values[f] = StringEscapeUtils.escapeCsv(field.normalize(content));

					} else if (field.required) {
						filled = false;
						break;
					}

				}

				// any, any_attribute

				else if ((field.any || field.any_attribute) && table.buffw != null) {

					if (setAnyContent(proc_node, field))
						values[f] = StringEscapeUtils.escapeCsv(field.normalize(content));

				}

				if (!filled)
					break;

			}

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
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void write() throws IOException {

		written = false;

		if (table.buffw != null) {

			written = true;

			if (pg_tab_delimiter) {

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.omissible)
						continue;

					String value = values[f];

					sb.append((value.isEmpty() ? "\\N" : value) + "\t");

				}

			}

			else {

				for (int f = 0; f < fields.size(); f++) {

					PgField field = fields.get(f);

					if (field.omissible)
						continue;

					sb.append(values[f] + ",");

				}

			}

			table.buffw.write(sb.substring(0, sb.length() - 1) + "\n");

			sb.setLength(0);

		}

	}

	/**
	 * Invoke nested node (root).
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeRootNestedNode() throws PgSchemaException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++)
			schema.parseChildNode2PgCsv(proc_node, table, schema.getTable(nested_table_id[n]), current_key, nested_key[n], list_holder[n], table.bridge, 0);

	}

	/**
	 * Invoke nested node (child).
	 *
	 * @param node_test node tester
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeChildNestedNode(PgSchemaNodeTester node_test) throws PgSchemaException {

		if (!filled)
			return;

		invoked = true;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			boolean exists = existsNestedNode(nested_table, node_test.proc_node);

			schema.parseChildNode2PgCsv(exists || nested ? node_test.proc_node : proc_node, table, nested_table, node_test.primary_key, nested_key[n], list_holder[n], !exists, exists ? 0 : node_test.key_id);

		}

	}

	/**
	 * Invoke nested node (child).
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
				schema.parseChildNode2PgCsv(proc_node, table, nested_table, current_key, nested_key[n], list_holder[n], false, 0);

		}

	}

}
