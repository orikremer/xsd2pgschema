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

import javax.xml.stream.XMLStreamException;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Retrieve any attribute stored in PostgreSQL.
 *
 * @author yokochi
 */
public class PgAnyAttrRetriever extends DefaultHandler {

	/** The root node name. */
	private String root_node_name = null;

	/** The XML builder. */
	private XmlBuilder xmlb = null;

	/** The current state for root node. */
	private boolean root_node = false;

	/** Whether root node has content. */
	public boolean has_content = false;

	/**
	 * Instance of any attribute retriever.
	 *
	 * @param root_node_name root node name
	 * @param xmlb XML builder
	 */
	public PgAnyAttrRetriever(String root_node_name, XmlBuilder xmlb) {

		this.root_node_name = root_node_name;
		this.xmlb = xmlb;

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	public void startElement(String namespaceURI, String localName, String qName, Attributes atts) {

		if (localName.equals(root_node_name))
			root_node = true;

		else if (!root_node)
			return;

		try {

			for (int i = 0; i < atts.getLength(); i++) {

				String content = atts.getValue(i);

				if (content != null && !content.isEmpty()) {

					xmlb.writePendingTableStartElements();

					xmlb.writer.writeAttribute(atts.getLocalName(i), content);

					has_content = true;

				}

			}

		} catch (XMLStreamException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
	 */
	public void endElement(String namespaceURI, String localName, String qName) {

		if (!root_node)
			return;

		if (localName.equals(root_node_name))
			root_node = false;

	}

	/*
	 * (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
	 */
	public void characters(char[] chars, int offset, int length) {
	}

}
