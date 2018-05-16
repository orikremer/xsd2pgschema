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

/**
 * Pending table element.
 *
 * @author yokochi
 */
public class PgPendingTableElem {

	/** The indent space. */
	protected String indent_space;

	/** The table of element. */
	protected PgTable table;

	/**
	 * Instance of pending table element.
	 *
	 * @param indent_space current indent space
	 * @param table current table
	 */
	public PgPendingTableElem(String indent_space, PgTable table) {

		this.indent_space = indent_space;
		this.table = table;

	}

	/**
	 * Write start element of pending table.
	 * 
	 * @param xmlb XML builder
	 * @throws XMLStreamException the XML stream exception
	 */
	public void writeStartElement(XmlBuilder xmlb) throws XMLStreamException {

		xmlb.writer.writeCharacters(indent_space);

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

	}

}
