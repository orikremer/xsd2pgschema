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
 * Retrieve any attribute stored in PostgreSQL.
 *
 * @author yokochi
 */
public class PgAnyAttrRetriever extends DefaultHandler {

	/** The current table. */
	private PgTable table;

	/** The XML builder. */
	private XmlBuilder xmlb = null;

	/** The JSON builder. */
	private JsonBuilder jsonb = null;

	/** The current indent level (JSON). */
	private int indent_level;

	/** The current field (column-oriented JSON). */
	private PgField field = null;

	/** The JSON key value space (column-oriented JSON). */
	private String key_value_space = null;

	/** The current state for root node. */
	private boolean root_node = false;

	/** Whether root node has content. */
	public boolean has_content = false;

	/**
	 * Instance of any attribute retriever.
	 *
	 * @param table current table
	 * @param xmlb XML builder
	 */
	public PgAnyAttrRetriever(PgTable table, XmlBuilder xmlb) {

		this.table = table;
		this.xmlb = xmlb;

	}

	/**
	 * Instance of any attribute retriever (JSON).
	 *
	 * @param table current table
	 * @param jsonb JSON builder
	 * @param indent_level current indent level
	 */
	public PgAnyAttrRetriever(PgTable table, JsonBuilder jsonb, int indent_level) {

		this.table = table;
		this.jsonb = jsonb;
		this.indent_level = indent_level;

	}

	/**
	 * Instance of any attribute retriever (column-oriented JSON).
	 *
	 * @param table current table
	 * @param field current field
	 * @param key_value_space the JSON key value space
	 */
	public PgAnyAttrRetriever(PgTable table, PgField field, String key_value_space) {

		this.table = table;
		this.field = field;
		this.key_value_space = key_value_space;

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

					PgPendingAttr attr = new PgPendingAttr(atts.getLocalName(i), content);

					PgPendingElem elem = xmlb.pending_elem.peek();

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

				// compose JSON document

				else if (jsonb != null) {

					PgPendingAttr attr = new PgPendingAttr(atts.getLocalName(i), content, indent_level);

					PgPendingElem elem = jsonb.pending_elem.peek();

					if (elem != null)
						elem.appendPendingAttr(attr);
					else
						attr.write(jsonb);

				}

				// compose column-oriented JSON document

				else if (field != null)
					field.writeValue2JsonBuf(content, key_value_space);

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
