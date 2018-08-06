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

	/** Whether table is referred from child table. */
	private boolean required;

	/** The prefix of index field. */
	private String field_prefix;

	/** The minimum word length for indexing. */
	private int min_word_len;

	/** Whether numeric values are stored in Lucene index. */
	private boolean lucene_numeric_index;

	/**
	 * Node parser for Lucene document conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table
	 * @param table current table
	 */
	public PgSchemaNode2LucIdx(final PgSchema schema, final PgTable parent_table, final PgTable table) {

		super(schema, parent_table, table, PgSchemaNodeParserType.full_text_indexing);

		if (required = table.required)
			field_prefix = table.name + ".";

		min_word_len = schema.index_filter.min_word_len;

		lucene_numeric_index = schema.index_filter.lucene_numeric_index;

	}

	/**
	 * Parse processing node (root).
	 *
	 * @param proc_node processing node
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	public void parseRootNode(final Node proc_node) throws PgSchemaException {

		current_key = document_id + "/" + table.xname;

		parse(proc_node, null, current_key, current_key, false, indirect);

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey nested_key : nested_keys)
			traverse(proc_node, nested_key.asOfRoot(this));

	}

	/**
	 * Parse processing node (child).
	 *
	 * @param node_test node tester
	 * @param nested_key nested key
	 * @return boolean whether current node is the last one
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected boolean parseChildNode(final PgSchemaNodeTester node_test, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		parse(node_test.proc_node, node_test.parent_key, node_test.primary_key, node_test.current_key, node_test.as_attr, node_test.indirect);

		if (filled) {

			visited = true;

			if (nested_keys != null) {

				for (PgSchemaNestedKey _nested_key : nested_keys) {

					boolean exists = existsNestedNode(_nested_key.table, node_test.proc_node);

					traverse(exists || indirect ? node_test.proc_node : proc_node, _nested_key.asOfChild(node_test, exists));

				}

			}

		}

		return isLastNode(nested_key, node_test.node_count);
	}

	/**
	 * Parse processing node (child).
	 *
	 * @param proc_node processing node
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void parseChildNode(final Node proc_node, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		parse(proc_node, nested_key.parent_key, nested_key.current_key, nested_key.current_key, nested_key.as_attr, nested_key.indirect);

		if (!filled || nested_keys == null)
			return;

		for (PgSchemaNestedKey _nested_key : nested_keys) {

			if (existsNestedNode(_nested_key.table, proc_node))
				traverse(proc_node, _nested_key.asOfChild(this));

		}

	}

	/**
	 * Parse current node and store into Lucene document.
	 *
	 * @param parent_node parent node
	 * @param nested_key nested key
	 * @throws PgSchemaException the pg schema exception
	 */
	@Override
	protected void traverse(final Node parent_node, final PgSchemaNestedKey nested_key) throws PgSchemaException {

		PgTable parent_table = this.table;
		PgTable table = nested_key.table;

		PgSchemaNode2LucIdx node2lucidx = null;

		try {

			node2lucidx = new PgSchemaNode2LucIdx(schema, parent_table, table);

			for (Node node = parent_node.getFirstChild(); node != null; node = node.getNextSibling()) {

				if (node.getNodeType() != Node.ELEMENT_NODE)
					continue;

				PgSchemaNodeTester node_test = new PgSchemaNodeTester(option, parent_node, node, parent_table, table, nested_key, node2lucidx.node_count, node2lucidx.node_ordinal);

				if (node_test.visited)
					return;

				else if (node_test.omissible)
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

	/**
	 * Parse processing node.
	 *
	 * @param proc_node processing node
	 * @param parent_key parent key
	 * @param primary_key primary key
	 * @param current_key current key
	 * @param as_attr whether parent key as attribute
	 * @param indirect whether child node is not nested node
	 * @throws PgSchemaException the pg schema exception
	 */
	private void parse(final Node proc_node, final String parent_key, final String primary_key, final String current_key, final boolean as_attr, final boolean indirect) throws PgSchemaException {

		Arrays.fill(values, "");

		filled = true;

		null_simple_primitive_list = false;

		if (nested_keys != null && nested_keys.size() > 0)
			nested_keys.clear();

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			// document_key, serial_key, xpath_key

			if (field.user_key)
				continue;

			// primary_key

			else if (field.primary_key) {

				if (required && rel_data_ext)
					values[f] = schema.getHashKeyString(primary_key);

			}

			// foreign_key

			else if (field.foreign_key) {

				if (parent_table.xname.equals(field.foreign_table_xname)) {

					if (required && rel_data_ext)
						values[f] = schema.getHashKeyString(parent_key);

				}

			}

			// nested_key

			else if (field.nested_key) {

				String nested_key;

				if ((nested_key = setNestedKey(proc_node, field, current_key)) != null) {

					if (required && rel_data_ext)
						values[f] = schema.getHashKeyString(nested_key);

				}

			}

			// attribute, simple_content, element

			else if (field.attribute || field.simple_content || field.element) {

				if (setContent(proc_node, field, current_key, as_attr, false)) {

					if (required)
						values[f] = content;

				} else if (field.required) {
					filled = false;
					break;
				}

			}

			// any, any_attribute

			else if (field.any || field.any_attribute) {

				try {

					if (required && setAnyContent(proc_node, field)) {

						values[f] = any_content.toString().trim();
						any_content.setLength(0);

					}

				} catch (TransformerException | IOException | SAXException e) {
					throw new PgSchemaException(e);
				}

			}

			if (!filled)
				break;

		}

		if (!filled || (null_simple_primitive_list && (nested_keys == null || nested_keys.size() == 0)))
			return;

		if (required)
			write();

		this.proc_node = proc_node;
		this.current_key = current_key;
		this.indirect = indirect;

	}

	/**
	 * Writer of processing node.
	 */
	private synchronized void write() {

		for (int f = 0; f < fields.size(); f++) {

			String value = values[f];

			if (value.isEmpty())
				continue;

			PgField field = fields.get(f);

			if (field.system_key)
				table.lucene_doc.add(new NoIdxStringField(field_prefix + field.name, value, Field.Store.YES));

			else if (field.indexable)
				field.writeValue2LucIdx(table.lucene_doc, field_prefix + field.name, value, value.length() >= min_word_len, lucene_numeric_index);

		}

	}

}
