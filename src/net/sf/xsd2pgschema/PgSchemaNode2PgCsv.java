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

	/** Whether use TSV format in PostgreSQL data migration. */
	private boolean pg_tab_delimiter = true;

	/** The current delimiter code. */
	private char pg_delimiter = '\t';

	/** The current null code. */
	private String pg_null = PgSchemaUtil.pg_tsv_null;

	/** Whether use default serial key size (unsigned int 32 bit). */
	private boolean def_ser_size = true;

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

		pg_delimiter = option.pg_delimiter;

		pg_null = option.pg_null;

		def_ser_size = option.ser_size.equals(PgSerSize.defaultSize());

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

		current_key = document_id + "/" + table.xname;

		parse(proc_node, null, current_key, current_key, false, indirect, 1);

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

		parse(node_test.proc_node, node_test.parent_key, node_test.primary_key, node_test.current_key, node_test.as_attr, node_test.indirect, node_test.ordinal);

	}

	/**
	 * Parser processing node (child).
	 *
	 * @param proc_node processing node
	 * @param nested_key nested key
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws TransformerException the transformer exception
	 */
	@Override
	public void parseChildNode(final Node proc_node, PgSchemaNestedKey nested_key) throws IOException, TransformerException {

		parse(proc_node, nested_key.parent_key, nested_key.current_key, nested_key.current_key, nested_key.as_attr, nested_key.indirect, 1);

	}

	/**
	 * Parse processing node.
	 *
	 * @param proc_node processing node
	 * @param parent_key parent key
	 * @param primary_key primary key
	 * @param current_key current key
	 * @param as_attr whether parent key as attribute
	 * @param indirect whether child node is not nested node
	 * @param ordinal ordinal number of current node
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws TransformerException the transformer exception
	 */
	private void parse(final Node proc_node, final String parent_key, final String primary_key, final String current_key, final boolean as_attr, final boolean indirect, final int ordinal) throws IOException, TransformerException {

		Arrays.fill(values, pg_null);

		filled = true;

		null_simple_primitive_list = false;

		if (nested_keys != null && nested_keys.size() > 0)
			nested_keys.clear();

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
					values[f] = def_ser_size ? Integer.toString(ordinal) : Short.toString((short) ordinal);

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

				if (parent_table.xname.equals(field.foreign_table_xname)) {

					if (table.buffw != null && rel_data_ext)
						values[f] = schema.getHashKeyString(parent_key);

				}

			}

			// nested_key

			else if (field.nested_key) {

				String nested_key;

				if ((nested_key = setNestedKey(proc_node, field, current_key)) != null) {

					if (table.buffw != null && rel_data_ext)
						values[f] = schema.getHashKeyString(nested_key);

				}

			}

			// attribute, simple_content, element

			else if (field.attribute || field.simple_content || field.element) {

				if (setContent(proc_node, field, current_key, as_attr, true)) {

					if (table.buffw != null && !content.isEmpty())
						values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(content) : StringEscapeUtils.escapeCsv(content);

				} else if (field.required) {
					filled = false;
					break;
				}

			}

			// any, any_attribute

			else if ((field.any || field.any_attribute) && table.buffw != null) {

				if (setAnyContent(proc_node, field) && !content.isEmpty())
					values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(content) : StringEscapeUtils.escapeCsv(content);

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
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void write() throws IOException {

		if (table.buffw == null)
			return;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.omissible)
				continue;

			String value = values[f];

			sb.append(value + pg_delimiter);

		}

		table.buffw.write(sb.substring(0, sb.length() - 1) + "\n");

		sb.setLength(0);

	}

	/**
	 * Invoke nested node (root).
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeRootNestedNode() throws PgSchemaException {

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys)
			schema.parseChildNode2PgCsv(proc_node, table, nested_key.asOfRoot(this));

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

		visited = true;

		if (nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys) {

			boolean exists = existsNestedNode(nested_key.table, node_test.proc_node);

			schema.parseChildNode2PgCsv(exists || indirect ? node_test.proc_node : proc_node, table, nested_key.asOfChild(node_test, exists));

		}

	}

	/**
	 * Invoke nested node (child).
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void invokeChildNestedNode() throws PgSchemaException {

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys) {

			if (existsNestedNode(nested_key.table, proc_node))
				schema.parseChildNode2PgCsv(proc_node, table, nested_key.asOfChild(this));

		}

	}

}
