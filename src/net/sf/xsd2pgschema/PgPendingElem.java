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
 * Pending element.
 *
 * @author yokochi
 */
public class PgPendingElem {

	/** The table of element. */
	protected PgTable table;

	/** The header string. */
	protected String header;

	/** Whether element has attribute only. */
	protected boolean attr_only = true;

	/** the current indent level (JSON). */
	protected int indent_level;

	/** The pending attribute. */
	protected HashMap<String, PgPendingAttr> pending_attrs = new HashMap<String, PgPendingAttr>();

	/**
	 * Instance of pending element.
	 *
	 * @param table current table
	 * @param header header string
	 * @param attr_only whether element has attribute only.
	 */
	public PgPendingElem(PgTable table, String header, boolean attr_only) {

		this.table = table;
		this.header = header;
		this.attr_only = attr_only;

	}

	/**
	 * Instance of pending element (JSON).
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 */
	public PgPendingElem(PgTable table, int indent_level) {

		this.table = table;
		this.indent_level = indent_level;

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
	 * Write pending element.
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

		if (pending_attrs.size() > 0) {

			pending_attrs.values().forEach(pending_attr -> {

				try {
					pending_attr.write(xmlb);
				} catch (PgSchemaException e) {
					e.printStackTrace();
				}

			});

			clear();

		}

	}

	/**
	 * Write pending element (JSON).
	 *
	 * @param jsonb JSON builder
	 */
	public void write(JsonBuilder jsonb) {

		jsonb.writeTableHeader(table, true, indent_level);

		if (pending_attrs.size() > 0) {

			pending_attrs.values().forEach(pending_attr -> pending_attr.write(jsonb));

			clear();

		}

	}

	/**
	 * Clear pending element.
	 */
	public void clear() {

		pending_attrs.clear();

	}

}
