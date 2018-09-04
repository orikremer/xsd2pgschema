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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;

import javax.xml.transform.TransformerException;

import org.apache.commons.text.StringEscapeUtils;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * Node parser for data (CSV/TSV) conversion.
 *
 * @author yokochi
 */
public class PgSchemaNode2PgCsv extends PgSchemaNodeParser {

	/** The string builder for a line of CSV/TSV format. */
	private StringBuilder sb = null;

	/** The buffered writer for data (CSV/TSV) conversion. */
	private BufferedWriter buffw = null;

	/** Whether table could have writer. */
	private boolean writable;

	/** Whether to use TSV format in PostgreSQL data migration. */
	private boolean pg_tab_delimiter;

	/** The current delimiter code. */
	private char pg_delimiter;

	/** The current null code. */
	private String pg_null;

	/** Whether default serial key size (unsigned int 32 bit). */
	private boolean def_ser_size;

	/**
	 * Node parser for CSV conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param md_hash_key instance of message digest
	 * @param parent_table parent table
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2PgCsv(final PgSchema schema, final MessageDigest md_hash_key, final PgTable parent_table, final PgTable table) throws PgSchemaException {

		super(schema, md_hash_key, parent_table, table, PgSchemaNodeParserType.pg_data_migration);

		if (writable = table.writable) {

			sb = new StringBuilder();
			buffw = table.buffw;

		}

		pg_tab_delimiter = schema.option.pg_tab_delimiter;

		pg_delimiter = schema.option.pg_delimiter;

		pg_null = schema.option.pg_null;

		def_ser_size = schema.option.ser_size.equals(PgSerSize.defaultSize());

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

		PgSchemaNode2PgCsv node2pgcsv = new PgSchemaNode2PgCsv(schema, md_hash_key, table, nested_key.table);

		try {

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(parent_node, node, table, nested_key, node2pgcsv);

				if (node_test.omissible)
					continue;

				if (node2pgcsv.parseChildNode(node_test, nested_key))
					break;

			}

			if (node2pgcsv.visited)
				return;

			node2pgcsv.parseChildNode(parent_node, nested_key);

		} finally {
			node2pgcsv.clear();
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

		Arrays.fill(values, pg_null);

		filled = true;
		null_simple_primitive_list = false;

		if (nested_keys != null)
			nested_keys.clear();

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			// document_key

			if (field.document_key) {

				if (writable)
					values[f] = document_id;

			}

			// serial_key

			else if (field.serial_key) {

				if (writable)
					values[f] = def_ser_size ? Integer.toString(node_test.node_ordinal) : Short.toString((short) node_test.node_ordinal);

			}

			// xpath_key

			else if (field.xpath_key) {

				if (writable)
					values[f] = getHashKeyString(current_key.substring(document_id_len));

			}

			// primary_key

			else if (field.primary_key) {

				if (writable && rel_data_ext)
					values[f] = getHashKeyString(node_test.primary_key);

			}

			// foreign_key

			else if (field.foreign_key) {

				if (parent_table.xname.equals(field.foreign_table_xname)) {

					if (writable && rel_data_ext)
						values[f] = getHashKeyString(node_test.parent_key);

				}

			}

			// nested_key

			else if (field.nested_key) {

				String nested_key;

				if ((nested_key = setNestedKey(proc_node, field, current_key)) != null) {

					if (writable && rel_data_ext)
						values[f] = getHashKeyString(nested_key);

				}

			}

			// attribute, simple_content, element

			else if (field.attribute || field.simple_content || field.element) {

				if (setContent(proc_node, field, current_key, node_test.as_attr, true)) {

					if (writable && !content.isEmpty())
						values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(content) : StringEscapeUtils.escapeCsv(content);

				} else if (field.required) {
					filled = false;
					break;
				}

			}

			// any, any_attribute

			else if (field.any || field.any_attribute) {

				try {

					if (writable && setAnyContent(proc_node, field) && !content.isEmpty())
						values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(content) : StringEscapeUtils.escapeCsv(content);

				} catch (TransformerException | IOException | SAXException e) {
					throw new PgSchemaException(e);
				}

			}

			if (!filled)
				break;

		}

		if (!writable || !filled || (null_simple_primitive_list && (nested_keys == null || nested_keys.size() == 0)))
			return;

		try {

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.omissible)
					continue;

				String value = values[f];

				sb.append(value + pg_delimiter);

			}

			if (buffw == null)
				buffw = table.buffw = Files.newBufferedWriter(table.pathw);

			buffw.write(sb.substring(0, sb.length() - 1) + "\n");

			sb.setLength(0);

		} catch (IOException e) {
			throw new PgSchemaException(e);
		}

		table.visited_key = current_key;

	}

}
