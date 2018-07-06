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

import org.apache.commons.text.StringEscapeUtils;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Retrieve any attribute into JSON builder.
 *
 * @author yokochi
 */
public class JsonBuilderAnyAttrRetriever extends DefaultHandler {

	/** The root node name. */
	private String root_node_name;

	/** The current field. */
	private PgField field;

	/** The nest tester. */
	private JsonBuilderNestTester nest_test;

	/** Whether field as JSON array. */
	private boolean array_field;

	/** The JSON builder. */
	private JsonBuilder jsonb;

	/** The current state for root node. */
	private boolean root_node = false;

	/** The common content holder. */
	private StringBuilder any_content = null;

	/**
	 * Instance of any attribute retriever.
	 *
	 * @param root_node_name root node name
	 * @param field current field
	 * @param nest_test nest test result of this node
	 * @param array_field whether field as JSON array
	 * @param jsonb JSON builder
	 */
	public JsonBuilderAnyAttrRetriever(String root_node_name, PgField field, JsonBuilderNestTester nest_test, boolean array_field, JsonBuilder jsonb) {

		this.root_node_name = root_node_name;
		this.field = field;
		this.nest_test = nest_test;
		this.array_field = array_field;
		this.jsonb = jsonb;

		if (array_field)
			any_content = new StringBuilder();

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	public void startElement(String namespaceURI, String localName, String qName, Attributes atts) {

		if (qName.contains(":"))
			qName = qName.substring(qName.indexOf(':') + 1);

		if (!root_node) {

			if (qName.equals(root_node_name))
				root_node = true;

			else
				return;

		}

		for (int i = 0; i < atts.getLength(); i++) {

			String attr_name = atts.getQName(i);

			if (attr_name.startsWith("xmlns"))
				continue;

			String content = atts.getValue(i);

			if (content != null && !content.isEmpty()) {

				// compose column-oriented JSON document

				if (array_field) {

					content = StringEscapeUtils.escapeCsv(content);

					if (!content.startsWith("\""))
						content = "\"" + content + "\"";

					any_content.append("/@" + attr_name + ":" + content + "\n");

				}

				// compose JSON document

				else {

					JsonBuilderPendingAttr attr = new JsonBuilderPendingAttr(field, attr_name, content, nest_test.current_indent_level);

					JsonBuilderPendingElem elem = jsonb.pending_elem.peek();

					if (elem != null)
						elem.appendPendingAttr(attr);
					else
						attr.write(jsonb);

				}

				nest_test.has_content = true;

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

		if (qName.equals(root_node_name)) {

			// compose column-oriented JSON document

			if (array_field) {

				field.writeValue2JsonBuf(jsonb.schema_ver, any_content.toString(), false, jsonb.key_value_space);

				any_content.setLength(0);

			}

			root_node = false;

		}

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
	 */
	public void characters(char[] chars, int offset, int length) {
	}

}
