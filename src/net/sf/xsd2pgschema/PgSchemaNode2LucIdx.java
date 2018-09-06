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
import java.security.MessageDigest;
import java.util.Arrays;

import javax.xml.transform.TransformerException;

import org.apache.lucene.document.Field;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * Node parser for Lucene document conversion.
 *
 * @author yokochi
 */
public class PgSchemaNode2LucIdx extends PgSchemaNodeParser {

	/** The Lucene document. */
	private org.apache.lucene.document.Document lucene_doc;

	/** The prefix of index field. */
	private String field_prefix;

	/** The minimum word length for indexing. */
	private int min_word_len;

	/** Whether numeric values are stored in Lucene index. */
	private boolean lucene_numeric_index;

	/** The content of fields. */
	private String[] values;

	/**
	 * Node parser for Lucene document conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param md_hash_key instance of message digest
	 * @param document_id document id
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @param lucene_doc Lucene document
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2LucIdx(final PgSchema schema, final MessageDigest md_hash_key, final String document_id, final PgTable parent_table, final PgTable table, final org.apache.lucene.document.Document lucene_doc) throws PgSchemaException {

		super(schema, md_hash_key, document_id, parent_table, table, PgSchemaNodeParserType.full_text_indexing);

		this.lucene_doc = lucene_doc;

		if (table.indexable) {

			field_prefix = table.name + ".";

			min_word_len = schema.index_filter.min_word_len;

			lucene_numeric_index = schema.index_filter.lucene_numeric_index;

			values = new String[fields.size()];

		}

	}

	/**
	 * Traverser nested node.
	 *
	 * @param parent_node parent node
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void traverseNestedNode(final Node parent_node, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		PgSchemaNode2LucIdx node2lucidx = new PgSchemaNode2LucIdx(schema, md_hash_key, document_id, table, nested_key.table, lucene_doc);

		try {

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(parent_node, node, table, nested_key, node2lucidx);

				if (node_test.omissible)
					continue;

				if (node2lucidx.parseChildNode(node_test, nested_key))
					break;

			}

			if (node2lucidx.visited)
				return;

			node2lucidx.parseChildNode(parent_node, nested_key);

		} finally {
			node2lucidx.clear();
		}

	}

	/** The parsed value. */
	private String value;

	/** The length of parsed value. */
	private int value_len;

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

			Arrays.fill(values, null);

			if (nested_keys != null)
				nested_keys.clear();

		}

		if (!rel_data_ext) {

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				// nested_key

				if (field.nested_key)
					setNestedKey(proc_node, field, current_key);

				else if (field.indexable) {

					// attribute, simple_content, element

					if (field.attribute || field.simple_content || field.element) {

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

			}

			if (null_simple_list && (nested_keys == null || nested_keys.size() == 0))
				return;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.indexable) {

					value = values[f];

					if ((value == null ? 0 : value.length()) == 0)
						continue;

					field.writeValue2LucIdx(lucene_doc, field_prefix + field.name, value, value_len >= min_word_len, lucene_numeric_index);

				}

			}

		}

		else {

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				// primary_key

				if (field.primary_key)
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

				else if (field.indexable) {

					// attribute, simple_content, element

					if (field.attribute || field.simple_content || field.element) {

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

			}

			if (null_simple_list && (nested_keys == null || nested_keys.size() == 0))
				return;

			for (int f = 0; f < fields.size(); f++) {

				PgField field = fields.get(f);

				if (field.system_key) {

					lucene_doc.add(new NoIdxStringField(field_prefix + field.name, values[f], Field.Store.YES));

					continue;
				}

				else if (field.indexable) {

					value = values[f];

					if ((value == null ? 0 : value.length()) == 0)
						continue;

					field.writeValue2LucIdx(lucene_doc, field_prefix + field.name, value, value_len >= min_word_len, lucene_numeric_index);

				}

			}

		}

		table.visited_key = current_key;

	}

}
