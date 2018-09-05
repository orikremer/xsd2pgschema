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
import java.security.MessageDigest;
import java.util.Arrays;

import javax.xml.transform.TransformerException;

import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * Node parser for Sphinx xmlpipe2 conversion.
 *
 * @author yokochi
 */
public class PgSchemaNode2SphDs extends PgSchemaNodeParser {

	/** The buffered writer for data conversion. */
	private BufferedWriter buffw;

	/** The prefix of index field. */
	private String field_prefix;

	/** The minimum word length for indexing. */
	private int min_word_len;

	/** The content of fields. */
	private String[] values;

	/**
	 * Node parser for Sphinx xmlpipe2 conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param md_hash_key instance of message digest
	 * @param parent_table parent table
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2SphDs(final PgSchema schema, final MessageDigest md_hash_key, final PgTable parent_table, final PgTable table) throws PgSchemaException {

		super(schema, md_hash_key, parent_table, table, PgSchemaNodeParserType.full_text_indexing);

		if (table.indexable) {

			buffw = table.buffw;

			field_prefix = table.name + PgSchemaUtil.sph_member_op;

			min_word_len = schema.index_filter.min_word_len;

			values = new String[fields.size()];

			Arrays.fill(values, "");

		}

	}

	/**
	 * Traverse nested node.
	 *
	 * @param parent_node parent node
	 * @param nested_key nested_key
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void traverseNestedNode(final Node parent_node, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		PgSchemaNode2SphDs node2sphds = new PgSchemaNode2SphDs(schema, md_hash_key, table, nested_key.table);

		try {

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(parent_node, node, table, nested_key, node2sphds);

				if (node_test.omissible)
					continue;

				if (node2sphds.parseChildNode(node_test, nested_key))
					break;

			}

			if (node2sphds.visited)
				return;

			node2sphds.parseChildNode(parent_node, nested_key);

		} finally {
			node2sphds.clear();
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

		if (!table.indexable) {

			fields.stream().filter(field -> field.nested_key).forEach(field -> setNestedKey(proc_node, field, current_key));

			return;
		}

		if (node_test.node_ordinal > 1) {

			not_complete = null_simple_list = false;

			Arrays.fill(values, "");

			if (nested_keys != null)
				nested_keys.clear();

		}

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

		if (null_simple_list && (nested_keys == null || nested_keys.size() == 0))
			return;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			if (field.indexable) {

				String value = values[f];

				int value_len = value.length();

				if (value_len == 0)
					continue;

				field.writeValue2SphDs(buffw, field_prefix + field.name, value, value_len >= min_word_len);

			}

		}

		table.visited_key = current_key;

	}

}
