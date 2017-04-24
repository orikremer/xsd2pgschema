/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2017 Masashi Yokochi

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
import java.io.StringWriter;
import java.util.Arrays;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.jsoup.Jsoup;
import org.w3c.dom.Node;

/**
 * Node parser for XML -> Lucene document conversion
 * @author yokochi
 */
public class PgSchemaNode2LucIdx extends PgSchemaNodeParser {

	/**
	 * Node parser for Lucene document conversion
	 * @param schema PostgreSQL data model
	 * @param parent_table parent table
	 * @param table current table
	 * @throws TransformerConfigurationException
	 * @throws ParserConfigurationException
	 */
	public PgSchemaNode2LucIdx(final PgSchema schema, final PgTable parent_table, final PgTable table) throws TransformerConfigurationException, ParserConfigurationException {

		super(schema, parent_table, table);

	}

	/**
	 * Parse processing node (root)
	 * @param proc_node processing node
	 */
	@Override
	public void parseRootNode(final Node proc_node) throws TransformerException, IOException {

		parse(false, proc_node, null, key_name = document_id + "/" + table.name, nested, 1);

	}

	/**
	 * Parse processing node (child)
	 * @param proc_node processing node
	 * @param parent_key key name of parent node
	 * @param key_name processing key name
	 * @param nested whether it is nested
	 * @param key_id ordinal number of current node
	 */
	@Override
	public void parseChildNode(final Node proc_node, final String parent_key, final String key_name, final boolean nested, final int key_id) throws TransformerException, IOException {

		parse(true, proc_node, parent_key, key_name, nested, key_id);

	}

	/**
	 * Parse processing node
	 * @param child_node whether if child node
	 * @param proc_node processing node
	 * @param parent_key key name of parent node
	 * @param key_name processing key name
	 * @param nested whether it is nested
	 * @param key_id ordinal number of current node
	 * @throws TransformerException
	 * @throws IOException
	 */
	private void parse(final boolean child_node, final Node proc_node, final String parent_key, final String key_name, final boolean nested, final int key_id) throws TransformerException, IOException {

		Arrays.fill(values, "");

		filled = true;

		nested_fields = 0;

		for (int f = 0; f < fields.size(); f++) {

			PgField field = fields.get(f);

			// document_key, serial_key, xpath_key

			if (field.user_key)
				continue;

			// primary_key

			else if (field.primary_key) {

				if (table.lucene_doc != null && rel_data_ext)
					values[f] = schema.getHashKeyString(key_name);

			}

			// foreign_key

			else if (field.foreign_key) {

				if (parent_table.name.equals(field.foreign_table)) {

					if (table.lucene_doc != null && rel_data_ext)
						values[f] = schema.getHashKeyString(parent_key);

				}

			}

			// nested_key

			else if (field.nested_key) {

				if (setNestedKey(field, key_name, key_id)) {

					if (table.lucene_doc != null && rel_data_ext)
						values[f] = schema.getHashKeyString(nested_key_name[nested_fields]);

					nested_fields++;

				}

			}

			// attribute, simple_cont, element

			else if (field.attribute || field.simple_cont || field.element) {

				if (setCont(proc_node, field, false)) {

					if (table.lucene_doc != null)
						values[f] = content;

				} else if (field.required) {
					filled = false;
					break;
				}

			}

			// any, any_attribute

			else if ((field.any || field.any_attribute) && table.lucene_doc != null) {

				if (field.any ? setAnyElement(proc_node) : setAnyAttr(proc_node)) {

					doc.appendChild(doc_root);

					DOMSource source = new DOMSource(doc);
					StringWriter writer = new StringWriter();
					StreamResult result = new StreamResult(writer);

					transformer.transform(source, result);

					values[f] = Jsoup.parse(writer.toString()).text();

					writer.close();

				}

			}

			if (!filled)
				break;

		}

		if (filled) {

			write();

			this.proc_node = proc_node;

			if (child_node) {

				this.nested = nested;
				this.key_name = key_name;

			}

		}

	}

	/**
	 * Writer of processing node
	 */
	private void write() {

		written = false;

		if (table.lucene_doc != null) {

			written = true;

			for (int f = 0; f < fields.size(); f++) {

				String value = values[f];

				if (value.isEmpty())
					continue;

				PgField field = fields.get(f);

				if (field.system_key)
					XsDataType.setKey(table.lucene_doc, table.name + "." + field.xname, value);

				else if (field.isIndexable(schema))
					XsDataType.setValue(field, table.lucene_doc, table.name + "." + field.xname, value, value.length() >= schema.min_word_len, schema.numeric_index);

			}

		}

	}

	/**
	 * Invoke nested node (root)
	 */
	@Override
	public void invokeRootNestedNode() throws ParserConfigurationException, TransformerException, IOException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++)
			schema.parseChildNode2LucIdx(proc_node, table, schema.getTable(nested_table_id[n]), key_name, nested_key_name[n], list_holder[n], table.bridge, 0);

	}

	/**
	 * Invoke nested node (child)
	 * @param node_test node tester
	 */
	@Override
	public void invokeChildNestedNode(PgSchemaNodeTester node_test) throws ParserConfigurationException, TransformerException, IOException {

		if (!filled)
			return;

		invoked = true;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			boolean exists = existsNestedNode(schema, nested_table, node_test.proc_node);

			schema.parseChildNode2LucIdx(exists || nested ? node_test.proc_node : proc_node, table, nested_table, node_test.key_name, nested_key_name[n], list_holder[n], !exists, exists ? 0 : node_test.key_id);

		}

	}

	/**
	 * Invoke nested node (child)
	 */
	@Override
	public void invokeChildNestedNode() throws ParserConfigurationException, TransformerException, IOException {

		if (!filled)
			return;

		for (int n = 0; n < nested_fields; n++) {

			PgTable nested_table = schema.getTable(nested_table_id[n]);

			if (existsNestedNode(schema, nested_table, proc_node))
				schema.parseChildNode2LucIdx(proc_node, table, nested_table, key_name, nested_key_name[n], list_holder[n], false, 0);

		}

	}

}
