/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018 Masashi Yokochi

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

import java.util.HashMap;

import org.apache.commons.text.StringEscapeUtils;
import org.xml.sax.Attributes;

/**
 * Retrieve any element into JSON builder.
 *
 * @author yokochi
 */
public class JsonBuilderAnyRetriever extends CommonBuilderAnyRetriever {

	/** The current field. */
	protected PgField field;

	/** The nest tester. */
	private JsonBuilderNestTester nest_test;

	/** Whether field as JSON array. */
	private boolean array_field;

	/** The JSON builder. */
	private JsonBuilder jsonb;

	/** The current indent level. */
	private int current_indent_level;

	/** The common content holder (relational-oriented JSON). */
	private StringBuilder any_content = null;

	/** Whether any element has simple content only. */
	private HashMap<String, Boolean> has_simple_cont_only = null;

	/**
	 * Instance of any retriever.
	 *
	 * @param root_node_name root node name
	 * @param field current field
	 * @param nest_test nest test result of this node
	 * @param array_field whether field as JSON array
	 * @param jsonb JSON builder
	 */
	public JsonBuilderAnyRetriever(String root_node_name, PgField field, JsonBuilderNestTester nest_test, boolean array_field, JsonBuilder jsonb) {

		super(root_node_name);

		this.field = field;
		this.nest_test = nest_test;
		this.array_field = array_field;
		this.jsonb = jsonb;

		current_indent_level = nest_test.current_indent_level;

		if (array_field)
			any_content = new StringBuilder();
		else
			has_simple_cont_only = new HashMap<String, Boolean>();

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	public void startElement(String namespaceURI, String localName, String qName, Attributes atts) {

		if (qName.contains(":"))
			qName = qName.substring(qName.indexOf(':') + 1);

		if (!root_node) {

			if (qName.equals(root_node_name)) {

				// compose JSON document

				if (!array_field) {

					JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

					if (elem != null)
						jsonb.writePendingElems(false);

					jsonb.writePendingSimpleCont();

				}

				root_node = true;

			}

			else
				return;

		}

		cur_path.append("/" + qName);

		if (cur_path.length() > cur_path_offset) {

			// compose JSON document

			if (!array_field) {

				String parent_path = jsonb.getParentPath(cur_path.toString()).substring(cur_path_offset);

				StringBuilder simple_content;

				if ((simple_content = simple_contents.get(parent_path)) != null && simple_content.length() > 0) {

					String content = simple_content.toString();

					if (!PgSchemaUtil.null_simple_cont_pattern.matcher(content).matches()) {

						jsonb.writeAnyField(jsonb.simple_content_name, false, content, current_indent_level);

						nest_test.has_content = true;

					}

					simple_content.setLength(0);

				}

				jsonb.writeStartAnyTable(qName, current_indent_level++);

				String _cur_path = cur_path.substring(cur_path_offset);

				has_simple_cont_only.put(_cur_path, true);
				has_simple_cont_only.put(jsonb.getParentPath(_cur_path), false);

			}

			nest_test.has_child_elem = true;

			String attr_name, content;

			for (int i = 0; i < atts.getLength(); i++) {

				attr_name = atts.getQName(i);

				if (attr_name.startsWith("xmlns"))
					continue;

				content = atts.getValue(i);

				if (content != null && !content.isEmpty()) {

					String _cur_path = cur_path.substring(cur_path_offset);

					// compose relational-oriented JSON document

					if (array_field) {

						content = StringEscapeUtils.escapeCsv(content);

						if (!content.startsWith("\""))
							content = "\"" + content + "\"";

						any_content.append(_cur_path + "/@" + attr_name + ":" + content + "\n");

					}

					// compose JSON document

					else {

						jsonb.writeAnyField(attr_name, true, content, current_indent_level);

						has_simple_cont_only.put(_cur_path, false);

					}

					nest_test.has_content = true;

				}

			}

		}

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
	 */
	public void endElement(String namespaceURI, String localName, String qName) {

		if (!root_node)
			return;

		if (qName.contains(":"))
			qName = qName.substring(qName.indexOf(':') + 1);

		int len = cur_path.length() - qName.length() - 1;

		if (cur_path.length() > cur_path_offset) {

			String _cur_path = cur_path.substring(cur_path_offset);

			StringBuilder simple_content;
			String content = null;

			if ((simple_content = simple_contents.get(_cur_path)) != null && simple_content.length() > 0) {

				content = simple_content.toString();

				if (!PgSchemaUtil.null_simple_cont_pattern.matcher(content).matches()) {

					// compose relational-oriented JSON document

					if (array_field) {

						content = StringEscapeUtils.escapeCsv(content);

						if (!content.startsWith("\""))
							content = "\"" + content + "\"";

						any_content.append(cur_path.substring(cur_path_offset) + ":" + content + "\n");

					}

					// compose JSON document

					else {

						if (!has_simple_cont_only.get(_cur_path))
							jsonb.writeAnyField(jsonb.simple_content_name, false, content, current_indent_level);

					}

					nest_test.has_content = true;

				}

				simple_content.setLength(0);

			}

			// compose JSON document

			if (!array_field) {

				--current_indent_level;

				if (content != null) {

					if (has_simple_cont_only.get(_cur_path)) {

						jsonb.writeEndTable(); // vanish table object automatically

						jsonb.writeAnyField(qName, false, content, current_indent_level);

					}

					else {

						jsonb.writeAnyField(jsonb.simple_content_name, false, content, current_indent_level);

						jsonb.writeEndTable();

					}

				}

				else
					jsonb.writeEndTable();

			}

		}

		cur_path.setLength(len);

		if (len == 0) {

			simple_contents.clear();

			// compose relational-oriented JSON document

			if (array_field) {

				field.write(jsonb.schema_ver, any_content.toString(), false, jsonb.concat_value_space);

				any_content.setLength(0);

			}

			// compose JSON document

			else
				has_simple_cont_only.clear();

			root_node = false;

		}

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
	 */
	public void characters(char[] chars, int offset, int length) {

		if (!root_node)
			return;

		String content = new String(chars, offset, length);

		if (content != null && !content.isEmpty()) {

			String _cur_path = cur_path.substring(cur_path_offset);

			if (!simple_contents.containsKey(_cur_path))
				simple_contents.put(_cur_path, new StringBuilder());

			simple_contents.get(_cur_path).append(content);

		}

	}

}
