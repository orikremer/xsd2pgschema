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

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Retrieve any attribute into XML builder
 *
 * @author yokochi
 */
public class XmlBuilderAnyAttrRetriever extends DefaultHandler {

	/** The root node name. */
	private String root_node_name;

	/** The nest tester. */
	private XmlBuilderNestTester nest_test = null;

	/** The XML builder. */
	private XmlBuilder xmlb = null;

	/** The current state for root node. */
	private boolean root_node = false;

	/**
	 * Instance of any attribute retriever.
	 *
	 * @param root_node_name root node name
	 * @param nest_test nest test result of this node
	 * @param xmlb XML builder
	 */
	public XmlBuilderAnyAttrRetriever(String root_node_name, XmlBuilderNestTester nest_test, XmlBuilder xmlb) {

		this.root_node_name = root_node_name;
		this.nest_test = nest_test;
		this.xmlb = xmlb;

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

				// compose XML document

				if (xmlb != null) {

					XmlBuilderPendingAttr attr = new XmlBuilderPendingAttr(attr_name, content);

					XmlBuilderPendingElem elem = xmlb.pending_elem.peek();

					if (elem != null)
						elem.appendPendingAttr(attr);

					else {

						try {
							attr.write(xmlb);
						} catch (PgSchemaException e) {
							e.printStackTrace();
						}

					}

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

		if (qName.equals(root_node_name))
			root_node = false;

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
	 */
	public void characters(char[] chars, int offset, int length) {
	}

}
