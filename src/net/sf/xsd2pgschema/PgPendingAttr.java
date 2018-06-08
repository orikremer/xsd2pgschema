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

	/** The field (attribute). */
	protected PgField field;

	/** The foreign table (simple attribute). */
	protected PgTable foreign_table;

	/** The local name (any attribute). */
	protected String local_name = null;

	/** The content. */
	protected String content = null;

	/** The current indent level (JSON). */
	protected int indent_level;

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
	 * Instance of pending attribute (JSON).
	 *
	 * @param field current field
	 * @param content content
	 * @param indent_level current indent level
	 */
	public PgPendingAttr(PgField field, String content, int indent_level) {

		this.field = field;
		this.content = content != null ? content : "";
		this.indent_level = indent_level;

	}

	/**
	 * Instance of pending simple attribute.
	 *
	 * @param field current field
	 * @param foreign_table foregin table
	 * @param content content
	 */
	public PgPendingAttr(PgField field, PgTable foreign_table, String content) {

		this.field = field;
		this.foreign_table = foreign_table;
		this.content = content != null ? content : "";

	}

	/**
	 * Instance of pending simple attribute (JSON).
	 *
	 * @param field current field
	 * @param foreign_table foregin table
	 * @param content content
	 * @param indent_level current indent level
	 */
	public PgPendingAttr(PgField field, PgTable foreign_table, String content, int indent_level) {

		this.field = field;
		this.foreign_table = foreign_table;
		this.content = content != null ? content : "";
		this.indent_level = indent_level;

	}

	/**
	 * Instance of pending any attribute.
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
	 * Instance of pending any attribute (JSON).
	 *
	 * @param local_name local name
	 * @param content content
	 * @param indent_level current indent level
	 */
	public PgPendingAttr(String local_name, String content, int indent_level) {

		field = null;

		this.local_name = local_name;
		this.content = content != null ? content : "";
		this.indent_level = indent_level;

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

	/**
	 * Write pending attribute (JSON).
	 *
	 * @param jsonb JSON builder
	 */
	public void write(JsonBuilder jsonb) {

		// attribute, simple attribute

		if (field != null)
			jsonb.writeField(null, field, !field.attribute, content, indent_level);

		// any attribute

		else
			jsonb.writeAnyAttr(local_name, content, indent_level);

	}

}
