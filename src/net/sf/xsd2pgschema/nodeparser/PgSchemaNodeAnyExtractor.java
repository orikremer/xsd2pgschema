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

package net.sf.xsd2pgschema.nodeparser;

import java.util.HashMap;

import org.apache.commons.text.StringEscapeUtils;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import net.sf.xsd2pgschema.PgSchemaUtil;

/**
 * Extract any content
 *
 * @author yokochi
 */
public class PgSchemaNodeAnyExtractor extends DefaultHandler {

	/** The node parser type. */
	private PgSchemaNodeParserType parser_type;

	/** The root node name. */
	private String root_node_name;

	/** The common content holder for xs:any and xs:anyAttribute. */
	private StringBuilder any_content;

	/** The simple content holder for xs:any and xs:anyAttribute. */
	private HashMap<String, StringBuilder> simple_content = new HashMap<String, StringBuilder>();

	/** The current state for root node. */
	private boolean root_node = false;

	/** The current path. */
	private StringBuilder cur_path = new StringBuilder();

	/** The offset value of current path. */
	private int cur_path_offset;

	/**
	 * Instance of any attribute extractor.
	 *
	 * @param parser_type node parser type
	 * @param root_node_name root node name
	 * @param any_content common content holder
	 */
	public PgSchemaNodeAnyExtractor(PgSchemaNodeParserType parser_type, String root_node_name, StringBuilder any_content) {

		this.parser_type = parser_type;
		this.root_node_name = root_node_name;
		this.any_content = any_content;

		cur_path_offset = root_node_name.length() + 1;

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

		cur_path.append("/" + qName);

		String attr_name, content;

		for (int i = 0; i < atts.getLength(); i++) {

			attr_name = atts.getQName(i);

			if (attr_name.startsWith("xmlns"))
				continue;

			content = atts.getValue(i);

			if (content != null && !content.isEmpty()) {

				switch (parser_type) {
				case full_text_indexing:
					any_content.append(content + " ");
					break;
				case json_conversion:
					content = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(content));

					if (!content.startsWith("\""))
						content = "\"" + content + "\"";

					any_content.append(cur_path.substring(cur_path_offset) + "/@" + attr_name + ":" + content + "\n");
					break;
				default:
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

		String _cur_path = cur_path.substring(cur_path_offset);

		StringBuilder _simple_content;

		if ((_simple_content = simple_content.get(_cur_path)) != null && _simple_content.length() > 0) {

			String content = _simple_content.toString();

			if (!PgSchemaUtil.null_simple_cont_pattern.matcher(content).matches()) {

				switch (parser_type) {
				case full_text_indexing:
					any_content.append(content + " ");
					break;
				case json_conversion:
					content = StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeEcmaScript(content));

					if (!content.startsWith("\""))
						content = "\"" + content + "\"";

					any_content.append(_cur_path + ":" + content + "\n");
					break;
				default:
				}

			}

			_simple_content.setLength(0);

		}

		int len = cur_path.length() - qName.length() - 1;

		cur_path.setLength(len);

		if (len == 0) {

			simple_content.clear();
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

			if (!simple_content.containsKey(_cur_path))
				simple_content.put(_cur_path, new StringBuilder());

			simple_content.get(_cur_path).append(content);

		}

	}

}
