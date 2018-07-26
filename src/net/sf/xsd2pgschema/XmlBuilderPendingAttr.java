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

import java.util.HashSet;

import javax.xml.stream.XMLStreamException;

/**
 * Pending attribute in XML builder.
 *
 * @author yokochi
 */
public class XmlBuilderPendingAttr extends CommonPendingAttr {

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
	 * @param any_field any field
	 * @param local_name local name
	 * @param content content
	 */
	public XmlBuilderPendingAttr(PgField any_field, String local_name, String content) {

		super(any_field, local_name, content);

	}

	/**
	 * Write pending attribute.
	 *
	 * @param xmlb XML builder
	 * @param other_namespaces other namespaces
	 * @throws PgSchemaException the pg schema exception
	 */
	public void write(XmlBuilder xmlb, HashSet<String> other_namespaces) throws PgSchemaException {

		try {

			if (field != null) {

				// attribute

				if (field.attribute) {

					if (field.is_xs_namespace)
						xmlb.writer.writeAttribute(field.xname, content);

					else {

						if (xmlb.append_xmlns && other_namespaces != null) {

							if (other_namespaces.contains(field.target_namespace))
								xmlb.writer.writeNamespace(field.prefix, field.target_namespace);
							else
								other_namespaces.add(field.target_namespace);

						}

						xmlb.writer.writeAttribute(field.prefix, field.target_namespace, field.xname, content);

					}

				}

				// simple attribute

				else {

					if (field.is_xs_namespace)
						xmlb.writer.writeAttribute(field.foreign_table_xname, content);

					else {

						if (xmlb.append_xmlns && other_namespaces != null) {

							if (other_namespaces.contains(field.target_namespace))
								xmlb.writer.writeNamespace(field.prefix, field.target_namespace);
							else
								other_namespaces.add(field.target_namespace);

						}

						xmlb.writer.writeAttribute(field.prefix, field.target_namespace, field.foreign_table_xname, content);

					}

				}

			}

			// any attribute

			else {

				if (any_field.prefix.isEmpty() || any_field.namespace.isEmpty() || xmlb.appended_xmlns.contains(any_field.prefix))
					xmlb.writer.writeAttribute(local_name, content);

				else {

					if (xmlb.append_xmlns && other_namespaces != null) {

						if (other_namespaces.contains(any_field.namespace))
							xmlb.writer.writeNamespace(any_field.prefix, any_field.namespace);
						else
							other_namespaces.add(any_field.namespace);

					}

					xmlb.writer.writeAttribute(any_field.prefix, any_field.namespace, local_name, content);

				}

			}

		} catch (XMLStreamException e) {
			if (xmlb.insert_doc_key)
				System.err.println("Not allowed to insert document key to element has any child element.");
			throw new PgSchemaException(e);
		}

	}

}
