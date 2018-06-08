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

import org.jsoup.Jsoup;
import org.w3c.dom.Node;

/**
 * Node parser for Sphinx xmlpipe2 conversion.
 *
 * @author yokochi
 */
public class PgSchemaNode2SphDs extends PgSchemaNodeParser {

	/** Whether table is referred from child table. */
	private boolean required = false;

	/** The minimum word length for indexing. */
	private int min_word_len = PgSchemaUtil.min_word_len;

	/**
	 * Node parser for Sphinx xmlpipe2 conversion.
	 *
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table
	 * @param table current table
	 * @throws TransformerConfigurationException the transformer configuration exception
	 * @throws ParserConfigurationException the parser configuration exception
	 */
	public PgSchemaNode2SphDs(final PgSchema schema, final PgTable parent_table, final PgTable table) throws TransformerConfigurationException, ParserConfigurationException {

		super(schema, parent_table, table);

		required = table.required;

		min_word_len = schema.index_filter.min_word_len;

	}

	/**
	 * Parse processing node (root).
	 *
	 * @param proc_node processing node
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void parseRootNode(final Node proc_node) throws TransformerException, IOException {

		parse(proc_node, current_key = document_id + "/" + table.xname, false, indirect);

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

		parse(node_test.proc_node, node_test.current_key, node_test.as_attr, node_test.indirect);

	}

	/**
	 * Parse processing node (child).
	 *
	 * @param proc_node processing node
	 * @param nested_key nested key
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void parseChildNode(final Node proc_node, final PgSchemaNestedKey nested_key) throws TransformerException, IOException {

		parse(proc_node, nested_key.current_key, nested_key.as_attr, nested_key.indirect);

	}

	/**
	 * Parse processing node.
	 *
	 * @param proc_node processing node
	 * @param current_key current key
	 * @param as_attr whether parent key as attribute
	 * @param indirect whether child node is not nested node
	 * @throws TransformerException the transformer exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void parse(final Node proc_node, final String current_key, final boolean as_attr, final boolean indirect) throws TransformerException, IOException {

		Arrays.fill(values, "");

		filled = true;

		null_simple_primitive_list = false;

		if (nested_keys != null && nested_keys.size() > 0)
			nested_keys.clear();

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

				if (setContent(proc_node, field, current_key, as_attr, false)) {

					if (required)
						values[f] = content;

				} else if (field.required) {
					filled = false;
					break;
				}

			}

			// any, any_attribute

			else if ((field.any || field.any_attribute) && required) {

				if (setAnyContent(proc_node, field))
					values[f] = Jsoup.parse(content).text();

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
	private void write() {

		for (int f = 0; f < fields.size(); f++) {

			String value = values[f];

			if (value.isEmpty())
				continue;

			PgField field = fields.get(f);

			if (field.indexable)
				field.writeValue2SphDs(table.buffw, table.name + PgSchemaUtil.sph_member_op + field.name, value, value.length() >= min_word_len);

		}

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
			schema.parseChildNode2SphDs(proc_node, table, nested_key.asOfRoot(this));

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

			schema.parseChildNode2SphDs(exists || indirect ? node_test.proc_node : proc_node, table, nested_key.asOfChild(node_test, exists));

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
				schema.parseChildNode2SphDs(proc_node, table, nested_key.asOfChild(this));

		}

	}

}
