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

package net.sf.xsd2pgschema.docbuilder;

import java.util.HashSet;

import javax.xml.stream.XMLStreamException;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgTable;

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

				String field_ns = field.target_namespace;
				String field_prefix = field.prefix;

				// attribute

				if (field.attribute) {

					if (field.is_xs_namespace)
						xmlb.writer.writeAttribute(field.xname, content);

					else {

						if (xmlb.append_xmlns && other_namespaces != null) {

							if (other_namespaces.contains(field_ns))
								xmlb.writer.writeNamespace(field_prefix, field_ns);
							else
								other_namespaces.add(field_ns);

						}

						xmlb.writer.writeAttribute(field_prefix, field_ns, field.xname, content);

					}

				}

				// simple attribute

				else {

					if (field.is_xs_namespace)
						xmlb.writer.writeAttribute(field.foreign_table_xname, content);

					else {

						if (xmlb.append_xmlns && other_namespaces != null) {

							if (other_namespaces.contains(field_ns))
								xmlb.writer.writeNamespace(field_prefix, field_ns);
							else
								other_namespaces.add(field_ns);

						}

						xmlb.writer.writeAttribute(field_prefix, field_ns, field.foreign_table_xname, content);

					}

				}

			}

			// any attribute

			else {

				String field_ns = any_field.namespace;
				String field_prefix = any_field.prefix;

				if (field_prefix.isEmpty() || field_ns.isEmpty() || xmlb.appended_xmlns.contains(field_prefix))
					xmlb.writer.writeAttribute(local_name, content);

				else {

					if (xmlb.append_xmlns && other_namespaces != null) {

						if (other_namespaces.contains(field_ns))
							xmlb.writer.writeNamespace(field_prefix, field_ns);
						else
							other_namespaces.add(field_ns);

					}

					xmlb.writer.writeAttribute(field_prefix, field_ns, local_name, content);

				}

			}

		} catch (XMLStreamException e) {
			if (xmlb.insert_doc_key)
				System.err.println("Not allowed to insert document key to element has any child element.");
			throw new PgSchemaException(e);
		}

	}

}
