/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018-2019 Masashi Yokochi

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

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;
import net.sf.xsd2pgschema.type.XsTableType;

/**
 * Pending element in XML builder.
 *
 * @author yokochi
 */
public class XmlBuilderPendingElem {

	/** The table of element. */
	protected PgTable table;

	/** The header string. */
	protected String header;

	/** Whether element has attribute only. */
	protected boolean attr_only;

	/** The pending attribute. */
	private LinkedHashMap<String, XmlBuilderPendingAttr> pending_attrs = null;

	/**
	 * Instance of pending element.
	 *
	 * @param table current table
	 * @param header header string
	 * @param attr_only whether element has attribute only
	 */
	public XmlBuilderPendingElem(PgTable table, String header, boolean attr_only) {

		this.table = table;
		this.header = header;
		this.attr_only = attr_only;

	}

	/**
	 * Append pending attribute.
	 *
	 * @param attr pending attribute
	 */
	public void appendPendingAttr(XmlBuilderPendingAttr attr) {

		if (pending_attrs == null)
			pending_attrs = new LinkedHashMap<String, XmlBuilderPendingAttr>();

		// attribute, simple attribute

		if (attr.field != null)
			pending_attrs.put(attr.field.xname, attr);

		// any attribute

		else
			pending_attrs.put(attr.local_name, attr);

	}

	/**
	 * Write pending element.
	 *
	 * @param xmlb XML builder
	 * @throws XMLStreamException the XML stream exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void write(XmlBuilder xmlb) throws XMLStreamException, IOException {

		XMLStreamWriter xml_writer = xmlb.writer;

		String table_ns = table.target_namespace;
		String table_prefix = table.xprefix;

		xmlb.writeSimpleCharacters(header);

		if (attr_only)
			xml_writer.writeEmptyElement(table_prefix, table.xname, table_ns);
		else
			xml_writer.writeStartElement(table_prefix, table.xname, table_ns);

		if (xmlb.append_xmlns) {

			if (!xmlb.appended_xmlns.contains(table_prefix)) {

				xml_writer.writeNamespace(table_prefix, table_ns);
				xmlb.appended_xmlns.add(table_prefix);

			}

			if (!xmlb.appended_xmlns.contains(PgSchemaUtil.xsi_prefix)) {

				boolean root_table = table.xs_type.equals(XsTableType.xs_root);

				if (root_table || table.has_nillable_element) {

					xml_writer.writeNamespace(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri);
					xmlb.appended_xmlns.add(PgSchemaUtil.xsi_prefix);

					if (root_table)
						xml_writer.writeAttribute(PgSchemaUtil.xsi_prefix, PgSchemaUtil.xsi_namespace_uri, "schemaLocation", xmlb.xsi_schema_location);

				}

			}

		}

		if (pending_attrs != null) {

			HashSet<String> other_namespaces = new HashSet<String>();

			pending_attrs.values().forEach(pending_attr -> {

				try {
					pending_attr.write(xmlb, other_namespaces);
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}

			});

			other_namespaces.clear();

			clear();

		}

	}

	/**
	 * Clear pending element.
	 */
	public void clear() {

		if (pending_attrs != null)
			pending_attrs.clear();

	}

}
