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
 * Pending attribute in XML builder.
 *
 * @author yokochi
 */
public class XmlBuilderPendingAttr extends AbstractPendingAttr {

	/**
	 * Instance of pending attribute.
	 *
	 * @param field current field
	 * @param content content
	 */
	public XmlBuilderPendingAttr(PgField field, String content) {

		super(field, content);

	}

	/**
	 * Instance of pending simple attribute.
	 *
	 * @param field current field
	 * @param foreign_table foregin table
	 * @param content content
	 */
	public XmlBuilderPendingAttr(PgField field, PgTable foreign_table, String content) {

		super(field, foreign_table, content);

	}

	/**
	 * Instance of pending any attribute.
	 *
	 * @param local_name local name
	 * @param content content
	 */
	public XmlBuilderPendingAttr(String local_name, String content) {

		super(local_name, content);

	}

	/**
	 * Write pending attribute.
	 *
	 * @param xmlb XML builder
	 * @throws PgSchemaException the pg schema exception
	 */
	public void write(XmlBuilder xmlb) throws PgSchemaException {

		try {

			if (field != null) {

				// attribute

				if (field.attribute) {

					if (field.is_xs_namespace)
						xmlb.writer.writeAttribute(field.xname, content);
					else
						xmlb.writer.writeAttribute(field.prefix, field.target_namespace, field.xname, content);

				}

				// simple attribute

				else {

					if (field.is_xs_namespace)
						xmlb.writer.writeAttribute(field.foreign_table_xname, content);
					else	
						xmlb.writer.writeAttribute(field.prefix, field.target_namespace, field.foreign_table_xname, content);

				}

			}

			// any attribute

			else
				xmlb.writer.writeAttribute(local_name, content);

		} catch (XMLStreamException e) {
			if (xmlb.insert_doc_key)
				System.err.println("Not allowed to insert document key to element has any child element.");
			throw new PgSchemaException(e);
		}

	}

}
