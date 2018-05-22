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

import javax.xml.stream.XMLStreamException;

/**
 * Pending start element of table.
 *
 * @author yokochi
 */
public class PgPendingStartElem {

	/** The header string. */
	protected String header;

	/** The table of element. */
	protected PgTable table;

	/** Whether start element has attribute only. */
	protected boolean attr_only = true;

	/** The pending attribute. */
	protected HashMap<String, PgPendingAttr> pending_attrs = new HashMap<String, PgPendingAttr>();

	/**
	 * Instance of pending start element of table.
	 *
	 * @param header header string
	 * @param table current table
	 * @param attr_only whether start element has attribute only.
	 */
	public PgPendingStartElem(String header, PgTable table, boolean attr_only) {

		this.header = header;
		this.table = table;
		this.attr_only = attr_only;

	}

	/**
	 * Append pending attribute.
	 *
	 * @param attr pending attribute
	 */
	public void appendPendingAttr(PgPendingAttr attr) {

		pending_attrs.put(attr.field.xname, attr);

	}

	/**
	 * Write pending start element of table.
	 *
	 * @param xmlb XML builder
	 * @throws XMLStreamException the XML stream exception
	 */
	public void write(XmlBuilder xmlb) throws XMLStreamException {

		xmlb.writer.writeCharacters(header);

		if (attr_only)
			xmlb.writer.writeEmptyElement(table.prefix, table.xname, table.target_namespace);
		else
			xmlb.writer.writeStartElement(table.prefix, table.xname, table.target_namespace);

		if (xmlb.append_xmlns) {

			if (!xmlb.appended_xmlns.contains(table.prefix)) {
				xmlb.writer.writeNamespace(table.prefix, table.target_namespace);
				xmlb.appended_xmlns.add(table.prefix);
			}

			if (table.has_required_field && !xmlb.appended_xmlns.contains(PgSchemaUtil.xsi_prefix)) {
				xmlb.writer.writeNamespace(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri);
				xmlb.appended_xmlns.add(PgSchemaUtil.xsi_prefix);
			}

		}

		pending_attrs.values().forEach(pending_attr -> {

			try {
				pending_attr.write(xmlb);
			} catch (XMLStreamException e) {
				e.printStackTrace();
				System.exit(1);
			}

		});

		pending_attrs.clear();

	}

}
