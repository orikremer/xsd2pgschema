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

	/** The current table. */
	private PgTable table;

	/** The XML builder. */
	private XmlBuilder xmlb = null;

	/** The current state for root node. */
	private boolean root_node = false;

	/** Whether root node has content. */
	protected boolean has_content = false;

	/**
	 * Instance of any attribute retriever.
	 *
	 * @param table current table
	 * @param xmlb XML builder
	 */
	public XmlBuilderAnyAttrRetriever(PgTable table, XmlBuilder xmlb) {

		this.table = table;
		this.xmlb = xmlb;

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	public void startElement(String namespaceURI, String localName, String qName, Attributes atts) {

		if (localName.equals(table.pname))
			root_node = true;

		else if (!root_node)
			return;

		for (int i = 0; i < atts.getLength(); i++) {

			String content = atts.getValue(i);

			if (content != null && !content.isEmpty()) {

				// compose XML document

				if (xmlb != null) {

					XmlBuilderPendingAttr attr = new XmlBuilderPendingAttr(atts.getLocalName(i), content);

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

				has_content = true;

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

		if (localName.equals(table.pname))
			root_node = false;

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
	 */
	public void characters(char[] chars, int offset, int length) {
	}

}
