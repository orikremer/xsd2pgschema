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
import java.util.Arrays;

import javax.xml.transform.TransformerException;

import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchema;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;

/**
 * Node parser for Sphinx xmlpipe2 conversion.
 *
 * @author yokochi
 */
public class PgSchemaNode2SphDs extends PgSchemaNodeParser {

	/** The minimum word length for indexing. */
	private int min_word_len;

	/** The buffered writer of Sphinx xmlpipe2 file. */
	private BufferedWriter sph_ds_buffw;

	/** The prefix of index field. */
	private String field_prefix;

	/** The content of fields. */
	private String[] values;

	/**
	 * Node parser for Sphinx xmlpipe2 conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param min_word_len minimum word length for indexing
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2SphDs(final PgSchema schema, final PgTable parent_table, final PgTable table, final boolean as_attr, final int min_word_len) throws PgSchemaException {

		super(schema, parent_table, table, PgSchemaNodeParserType.full_text_indexing);

		this.min_word_len = min_word_len;

		if (table.indexable) {

			this.as_attr = as_attr;

			sph_ds_buffw = schema.sph_ds_buffw;

			field_prefix = table.name + PgSchemaUtil.sph_member_op;

			values = new String[fields_size];

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

		PgSchemaNode2SphDs node_parser = new PgSchemaNode2SphDs(schema, table, nested_key.table, nested_key.as_attr, min_word_len);
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

	/** The parsed value. */
	private String value;

	/** The length of parsed value. */
	private int value_len;

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

		if (!table.indexable) {

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

			else if (field.indexable) {

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

		for (int f = 0; f < fields_size; f++) {

			field = fields.get(f);

			if (field.indexable) {

				value = values[f];

				if ((value_len = (value == null ? 0 : value.length())) == 0)
					continue;

				field.write(sph_ds_buffw, field_prefix + field.name, value, value_len >= min_word_len);

			}

		}

		table.visited_key = current_key;

	}

}
