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

/**
 * Pending attribute in JSON builder.
 *
 * @author yokochi
 */
public class JsonBuilderPendingAttr extends AbstractPendingAttr {

	/** The current indent level. */
	protected int indent_level;

	/** The field contains any attribute. */
	protected PgField any_field = null;

	/**
	 * Instance of pending attribute.
	 *
	 * @param field current field
	 * @param content content
	 * @param indent_level current indent level
	 */
	public JsonBuilderPendingAttr(PgField field, String content, int indent_level) {

		super(field, content);

		this.indent_level = indent_level;

	}

	/**
	 * Instance of pending simple attribute.
	 *
	 * @param field current field
	 * @param foreign_table foregin table
	 * @param content content
	 * @param indent_level current indent level
	 */
	public JsonBuilderPendingAttr(PgField field, PgTable foreign_table, String content, int indent_level) {

		super(field, foreign_table, content);

		this.indent_level = indent_level;

	}

	/**
	 * Instance of pending any attribute.
	 *
	 * @param any_field any field
	 * @param local_name local name
	 * @param content content
	 * @param indent_level current indent level
	 */
	public JsonBuilderPendingAttr(PgField any_field, String local_name, String content, int indent_level) {

		super(any_field, local_name, content);

		this.any_field = any_field;
		this.indent_level = indent_level;

	}

	/**
	 * Write pending attribute.
	 *
	 * @param jsonb JSON builder
	 */
	public void write(JsonBuilder jsonb) {

		// attribute, simple attribute

		if (field != null)
			jsonb.writeField(null, field, !field.attribute, content, indent_level);

		// any attribute

		else
			jsonb.writeAnyAttr(this);

	}

}
