/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2019 Masashi Yokochi

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

import org.apache.lucene.document.Field;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgTable;
import net.sf.xsd2pgschema.luceneutil.NoIdxStringField;

/**
 * Node parser for Lucene document conversion.
 *
 * @author yokochi
 */
public class PgSchemaNode2LucIdx extends PgSchemaNodeParser {

	/** The minimum word length for indexing. */
	private int min_word_len;

	/** Whether numeric values are stored in Lucene index. */
	private boolean numeric_index;

	/** The Lucene document. */
	private org.apache.lucene.document.Document lucene_doc;

	/** The prefix of index field. */
	private String field_prefix;

	/** The content of fields. */
	private String[] values = null;

	/**
	 * Node parser for Lucene document conversion.
	 *
	 * @param npb node parser builder
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @param as_attr whether parent node as attribute
	 * @param min_word_len minimum word length for indexing
	 * @param numeric_index whether numeric values are stored in Lucene index
	 * @throws PgSchemaException the pg schema exception
	 */
	protected PgSchemaNode2LucIdx(final PgSchemaNodeParserBuilder npb, final PgTable parent_table, final PgTable table, final boolean as_attr, final int min_word_len, boolean numeric_index) throws PgSchemaException {

		super(npb, parent_table, table);

		this.min_word_len = min_word_len;
		this.numeric_index = numeric_index;

		if (table.indexable)
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

		if (table.indexable) {

			lucene_doc = npb.schema.lucene_doc;

			field_prefix = table.name + ".";

			values = new String[_fields_size];

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

		PgSchemaNode2LucIdx node_parser = new PgSchemaNode2LucIdx(npb, table, nested_key.table, nested_key.as_attr, min_word_len, numeric_index);
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

			if (total_nested_fields > 0)
				table.nested_fields.forEach(field -> setNestedKey(proc_node, field));

			return;
		}

		if (visited) {

			not_complete = null_simple_list = false;

			Arrays.fill(values, null);

		}

		PgField field;

		if (npb.rel_data_ext) {

			for (int f = 0; f < _fields_size; f++) {

				field = _fields.get(f);

				// primary_key

				if (field.primary_key)
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

				else if (field.indexable) {

					// attribute, simple_content, element

					if (field.content_holder) {

						if (setContent(proc_node, field))
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

			for (int f = 0; f < _fields_size; f++) {

				field = _fields.get(f);

				if (field.system_key) {

					lucene_doc.add(new NoIdxStringField(field_prefix + field.name, values[f], Field.Store.YES));

					continue;
				}

				else if (field.indexable) {

					value = values[f];

					if (value == null || (value_len = value.length()) == 0)
						continue;

					field.write(lucene_doc, field_prefix + field.name, value, value_len >= min_word_len, numeric_index);

				}

			}

		}

		else {

			for (int f = 0; f < _fields_size; f++) {

				field = _fields.get(f);

				// nested_key

				if (field.nested_key)
					setNestedKey(proc_node, field);

				else if (field.indexable) {

					// attribute, simple_content, element

					if (field.content_holder) {

						if (setContent(proc_node, field))
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

			for (int f = 0; f < _fields_size; f++) {

				field = _fields.get(f);

				if (field.indexable) {

					value = values[f];

					if (value == null || (value_len = value.length()) == 0)
						continue;

					field.write(lucene_doc, field_prefix + field.name, value, value_len >= min_word_len, numeric_index);

				}

			}

		}

		table.visited_key = current_key;

	}

}
