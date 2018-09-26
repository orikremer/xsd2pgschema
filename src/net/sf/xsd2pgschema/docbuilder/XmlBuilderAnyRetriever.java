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

package net.sf.xsd2pgschema.docbuilder;

import javax.xml.stream.XMLStreamException;

import org.xml.sax.Attributes;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchemaUtil;

/**
 * Retrieve any element into XML builder.
 *
 * @author yokochi
 */
public class XmlBuilderAnyRetriever extends CommonBuilderAnyRetriever {

	/** The target namespace. */
	private String target_namespace;

	/** The prefix of target namespace. */
	private String prefix;

	/** The nest tester. */
	private XmlBuilderNestTester nest_test;

	/** The XML builder. */
	private XmlBuilder xmlb;

	/** Whether this is first node. */
	private boolean first_node = true;

	/** The current indent space. */
	private String current_indent_space;

	/**
	 * Instance of any retriever.
	 *
	 * @param root_node_name root node name
	 * @param field current field
	 * @param nest_test nest test result of this node
	 * @param xmlb XML builder
	 */
	public XmlBuilderAnyRetriever(String root_node_name, PgField field, XmlBuilderNestTester nest_test, XmlBuilder xmlb) {

		super(root_node_name);

		target_namespace = field.namespace;
		prefix = field.prefix;
		this.nest_test = nest_test;
		this.xmlb = xmlb;

		current_indent_space = nest_test.child_indent_space;

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

				try {

					XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

					if (elem != null)
						xmlb.writePendingElems(false);

					xmlb.writePendingSimpleCont();

					xmlb.writer.writeCharacters((nest_test.has_child_elem ? "" : xmlb.line_feed_code) + current_indent_space);

				} catch (XMLStreamException e) {
					e.printStackTrace();
				}

				root_node = true;

			}

			else
				return;

		}

		boolean root_child = cur_path.length() == cur_path_offset;

		cur_path.append("/" + qName);

		if (cur_path.length() > cur_path_offset) {

			try {

				boolean has_simple_content = false;

				String parent_path = xmlb.getParentPath(cur_path.toString()).substring(cur_path_offset);

				StringBuilder simple_content;

				if ((simple_content = simple_contents.get(parent_path)) != null && simple_content.length() > 0) {

					String content = simple_content.toString();

					if (!PgSchemaUtil.null_simple_cont_pattern.matcher(content).matches()) {

						xmlb.writer.writeCharacters(content);

						nest_test.has_content = has_simple_content = true;

					}

					simple_content.setLength(0);

				}

				if (!has_simple_content && !first_node)
					xmlb.writer.writeCharacters(xmlb.line_feed_code + current_indent_space);

				xmlb.writer.writeStartElement(prefix, qName, target_namespace);

				if (root_child && xmlb.append_xmlns) {

					if (!prefix.isEmpty() && !xmlb.appended_xmlns.contains(prefix))
						xmlb.writer.writeNamespace(prefix, target_namespace);

				}

				first_node = false;

				current_indent_space += nest_test.indent_space;

				nest_test.has_child_elem = true;

				String attr_name, content;

				for (int i = 0; i < atts.getLength(); i++) {

					attr_name = atts.getQName(i);

					if (attr_name.startsWith("xmlns"))
						continue;

					content = atts.getValue(i);

					if (content != null && !content.isEmpty()) {

						xmlb.writer.writeAttribute(attr_name, content);

						nest_test.has_content = true;

					}

				}

			} catch (XMLStreamException e) {
				e.printStackTrace();
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

			current_indent_space = current_indent_space.substring(nest_test.indent_offset);

			boolean has_simple_content = false;

			try {

				String _cur_path = cur_path.substring(cur_path_offset);

				StringBuilder simple_content;

				if ((simple_content = simple_contents.get(_cur_path)) != null && simple_content.length() > 0) {

					String content = simple_content.toString();

					if (!PgSchemaUtil.null_simple_cont_pattern.matcher(content).matches()) {

						xmlb.writer.writeCharacters(content);

						nest_test.has_content = has_simple_content = true;

					}

					simple_content.setLength(0);

				}

				if (!has_simple_content)
					xmlb.writer.writeCharacters(current_indent_space);

				xmlb.writer.writeEndElement();

				if (len > cur_path_offset)
					xmlb.writer.writeCharacters(xmlb.line_feed_code);

			} catch (XMLStreamException e) {
				e.printStackTrace();
			}

		}

		cur_path.setLength(len);

		if (len == 0) {

			try {
				xmlb.writer.writeCharacters(xmlb.line_feed_code);
			} catch (XMLStreamException e) {
				e.printStackTrace();
			}

			simple_contents.clear();

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
