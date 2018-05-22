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
 * Pending attribute.
 *
 * @author yokochi
 */
public class PgPendingAttr {

	/** The field. */
	protected PgField field;

	/** The local name. */
	protected String local_name = null;

	/** The content. */
	protected String content = null;

	/**
	 * Instance of pending attribute.
	 *
	 * @param field current field
	 * @param content content
	 */
	public PgPendingAttr(PgField field, String content) {

		this.field = field;
		this.content = content != null ? content : "";

	}

	/**
	 * Instance of pending attribute (any attribute).
	 *
	 * @param local_name local name
	 * @param content content
	 */
	public PgPendingAttr(String local_name, String content) {

		field = null;

		this.local_name = local_name;
		this.content = content != null ? content : "";

	}

	/**
	 * Write pending start element of table.
	 *
	 * @param xmlb XML builder
	 * @throws XMLStreamException the XML stream exception
	 */
	public void write(XmlBuilder xmlb) throws XMLStreamException {

		if (field != null) {

			if (field.is_xs_namespace)
				xmlb.writer.writeAttribute(field.xname, content);
			else
				xmlb.writer.writeAttribute(field.prefix, field.target_namespace, field.xname, content);

		}

		else
			xmlb.writer.writeAttribute(local_name, content);

	}

}
