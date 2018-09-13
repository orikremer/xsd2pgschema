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
	private StringBuilder sb;

	/** The buffered writer for data conversion. */
	private BufferedWriter buffw;

	/** Whether to use TSV format in PostgreSQL data migration. */
	private boolean pg_tab_delimiter;

	/** The current delimiter code. */
	private char pg_delimiter;

	/** The current null code. */
	private String pg_null;

	/** Whether default serial key size (unsigned int 32 bit). */
	private boolean def_ser_size;

	/** The content of fields. */
	private String[] values;

	/**
	 * Node parser for CSV conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param md_hash_key instance of message digest
	 * @param document_id document id
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2PgCsv(final PgSchema schema, final MessageDigest md_hash_key, final String document_id, final PgTable parent_table, final PgTable table) throws PgSchemaException {

		super(schema, md_hash_key, document_id, parent_table, table, PgSchemaNodeParserType.pg_data_migration);

		if (table.writable) {

			sb = new StringBuilder();

			buffw = table.buffw;

			pg_tab_delimiter = schema.option.pg_tab_delimiter;

			pg_delimiter = schema.option.pg_delimiter;

			pg_null = schema.option.pg_null;

			def_ser_size = schema.option.ser_size.equals(PgSerSize.defaultSize());

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

		PgSchemaNode2PgCsv node2pgcsv = new PgSchemaNode2PgCsv(schema, md_hash_key, document_id, table, nested_key.table);

		try {

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (node2pgcsv.isOmissible(parent_node, node, nested_key))
					continue;

				if (node2pgcsv.parseChildNode(nested_key))
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
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void parse() throws PgSchemaException {

		if (table.visited_key.equals(current_key = node_test.current_key))
			return;

		proc_node = node_test.proc_node;
		indirect = node_test.indirect;

		if (!table.writable) {

			fields.stream().filter(field -> field.nested_key).forEach(field -> setNestedKey(proc_node, field, current_key));

			return;
		}

		if (node_test.node_ordinal > 1) {

			not_complete = null_simple_list = false;

			Arrays.fill(values, pg_null);

			if (nested_keys != null)
				nested_keys.clear();

		}

		if (rel_data_ext) {

			for (int f = 0; f < fields_size; f++) {

				PgField field = fields.get(f);

				if (field.omissible)
					continue;

				// document_key

				else if (field.document_key)
					values[f] = document_id;

				// primary_key

				else if (field.primary_key)
					values[f] = getHashKeyString(node_test.primary_key);

				// foreign_key

				else if (field.foreign_key) {

					if (parent_table.xname.equals(field.foreign_table_xname))
						values[f] = getHashKeyString(node_test.parent_key);

				}

				// nested_key

				else if (field.nested_key) {

					String nested_key;

					if ((nested_key = setNestedKey(proc_node, field, current_key)) != null)
						values[f] = getHashKeyString(nested_key);

				}

				// attribute, simple_content, element

				else if (field.attribute || field.simple_content || field.element) {

					if (setContent(proc_node, field, current_key, node_test.as_attr, true)) {

						if (!content.isEmpty())
							values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(content) : StringEscapeUtils.escapeCsv(content);

					} else if (field.required) {

						not_complete = true;

						return;
					}

				}

				// any, any_attribute

				else if (field.any || field.any_attribute) {

					try {

						if (setAnyContent(proc_node, field) && !content.isEmpty())
							values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(content) : StringEscapeUtils.escapeCsv(content);

					} catch (TransformerException | IOException | SAXException e) {
						throw new PgSchemaException(e);
					}

				}

				// serial_key

				else if (field.serial_key) {
					values[f] = def_ser_size ? Integer.toString(node_test.node_ordinal) : Short.toString((short) node_test.node_ordinal);
				}

				// xpath_key

				else if (field.xpath_key)
					values[f] = getHashKeyString(current_key.substring(document_id_len));

			}

		}

		else {

			for (int f = 0; f < fields_size; f++) {

				PgField field = fields.get(f);

				// nested_key

				if (field.nested_key)
					setNestedKey(proc_node, field, current_key);

				else if (field.omissible)
					continue;

				// document_key

				else if (field.document_key)
					values[f] = document_id;

				// attribute, simple_content, element

				else if (field.attribute || field.simple_content || field.element) {

					if (setContent(proc_node, field, current_key, node_test.as_attr, true)) {

						if (!content.isEmpty())
							values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(content) : StringEscapeUtils.escapeCsv(content);

					} else if (field.required) {

						not_complete = true;

						return;
					}

				}

				// any, any_attribute

				else if (field.any || field.any_attribute) {

					try {

						if (setAnyContent(proc_node, field) && !content.isEmpty())
							values[f] = pg_tab_delimiter ? PgSchemaUtil.escapeTsv(content) : StringEscapeUtils.escapeCsv(content);

					} catch (TransformerException | IOException | SAXException e) {
						throw new PgSchemaException(e);
					}

				}

				// serial_key

				else if (field.serial_key) {
					values[f] = def_ser_size ? Integer.toString(node_test.node_ordinal) : Short.toString((short) node_test.node_ordinal);
				}

				// xpath_key

				else if (field.xpath_key)
					values[f] = getHashKeyString(current_key.substring(document_id_len));

			}

		}

		if (null_simple_list && (nested_keys == null || nested_keys.size() == 0))
			return;

		try {

			for (int f = 0; f < fields_size; f++) {

				PgField field = fields.get(f);

				if (field.omissible)
					continue;

				sb.append(values[f] + pg_delimiter);

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
