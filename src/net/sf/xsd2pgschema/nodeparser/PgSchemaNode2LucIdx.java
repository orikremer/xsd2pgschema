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

import org.apache.lucene.document.Field;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchema;
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

	/** The prefix of index field. */
	private String field_prefix;

	/** The content of fields. */
	private String[] values;

	/**
	 * Node parser for Lucene document conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table (set null if current table is root table)
	 * @param table current table
	 * @param min_word_len minimum word length for indexing
	 * @param numeric_index whether numeric values are stored in Lucene index
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgSchemaNode2LucIdx(final PgSchema schema, final PgTable parent_table, final PgTable table, final int min_word_len, boolean numeric_index) throws PgSchemaException {

		super(schema, parent_table, table, PgSchemaNodeParserType.full_text_indexing);

		this.min_word_len = min_word_len;
		this.numeric_index = numeric_index;

		if (table.indexable) {

			if (rel_data_ext)
				md_hash_key = schema.md_hash_key;

			field_prefix = table.name + ".";

			values = new String[fields_size];

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

		PgSchemaNode2LucIdx node2lucidx = new PgSchemaNode2LucIdx(schema, table, nested_key.table, min_word_len, numeric_index);

		try {

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (node2lucidx.isOmissible(parent_node, node, nested_key))
					continue;

				if (node2lucidx.parseChildNode(nested_key))
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
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void parse() throws PgSchemaException {

		if (table.visited_key.equals(current_key = node_test.current_key))
			return;

		if (table.has_parent_restriction)
			current_path = current_key.substring(document_id_len).split("\\/"); // XPath notation

		proc_node = node_test.proc_node;
		indirect = node_test.indirect;

		if (!table.indexable) {

			fields.stream().filter(field -> field.nested_key).forEach(field -> setNestedKey(proc_node, field));

			return;
		}

		if (node_test.node_ordinal > 1) {

			not_complete = null_simple_list = false;

			Arrays.fill(values, null);

			if (nested_keys != null)
				nested_keys.clear();

		}

		PgField field;

		if (!rel_data_ext) {

			for (int f = 0; f < fields_size; f++) {

				field = fields.get(f);

				// nested_key

				if (field.nested_key)
					setNestedKey(proc_node, field);

				else if (field.indexable) {

					// attribute, simple_content, element

					if (field.attribute || field.simple_content || field.element) {

						if (setContent(proc_node, field, node_test.as_attr, false))
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

			org.apache.lucene.document.Document lucene_doc = schema.lucene_doc;

			for (int f = 0; f < fields_size; f++) {

				field = fields.get(f);

				if (field.indexable) {

					value = values[f];

					if ((value == null ? 0 : value.length()) == 0)
						continue;

					field.write(lucene_doc, field_prefix + field.name, value, value_len >= min_word_len, numeric_index);

				}

			}

		}

		else {

			for (int f = 0; f < fields_size; f++) {

				field = fields.get(f);

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

					if ((nested_key = setNestedKey(proc_node, field)) != null)
						values[f] = getHashKeyString(nested_key);

				}

				else if (field.indexable) {

					// attribute, simple_content, element

					if (field.attribute || field.simple_content || field.element) {

						if (setContent(proc_node, field, node_test.as_attr, false))
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

			org.apache.lucene.document.Document lucene_doc = schema.lucene_doc;

			for (int f = 0; f < fields_size; f++) {

				field = fields.get(f);

				if (field.system_key) {

					lucene_doc.add(new NoIdxStringField(field_prefix + field.name, values[f], Field.Store.YES));

					continue;
				}

				else if (field.indexable) {

					value = values[f];

					if ((value == null ? 0 : value.length()) == 0)
						continue;

					field.write(lucene_doc, field_prefix + field.name, value, value_len >= min_word_len, numeric_index);

				}

			}

		}

		table.visited_key = current_key;

	}

}
