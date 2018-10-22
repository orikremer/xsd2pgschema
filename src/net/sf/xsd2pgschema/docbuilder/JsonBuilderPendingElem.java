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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.xsd2pgschema.PgTable;

/**
 * Pending element in JSON builder.
 *
 * @author yokochi
 */
public class JsonBuilderPendingElem {

	/** The table of element. */
	protected PgTable table;

	/** The current indent level. */
	protected int indent_level;

	/** The pending attribute. */
	private LinkedHashMap<String, JsonBuilderPendingAttr> pending_attrs = null;

	/**
	 * Instance of pending element.
	 *
	 * @param table current table
	 * @param indent_level current indent level
	 */
	public JsonBuilderPendingElem(PgTable table, int indent_level) {

		this.table = table;
		this.indent_level = indent_level;

	}

	/**
	 * Append pending attribute.
	 *
	 * @param attr pending attribute
	 */
	public void appendPendingAttr(JsonBuilderPendingAttr attr) {

		if (pending_attrs == null)
			pending_attrs = new LinkedHashMap<String, JsonBuilderPendingAttr>();

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
	 * @param jsonb JSON builder
	 */
	public void write(JsonBuilder jsonb) {

		jsonb.writeStartTable(table, true, indent_level);

		if (pending_attrs != null) {

			// attribute

			pending_attrs.values().stream().filter(pending_attr -> pending_attr.field != null).forEach(pending_attr -> pending_attr.write(jsonb));

			// any attribute

			if (pending_attrs.values().parallelStream().anyMatch(pending_attr -> pending_attr.field == null)) {

				List<JsonBuilderPendingAttr> any_attrs = new ArrayList<JsonBuilderPendingAttr>();

				pending_attrs.values().stream().filter(pending_attr -> pending_attr.field == null).forEach(pending_attr -> any_attrs.add(pending_attr));

				jsonb.writeAnyAttrs(any_attrs);

				any_attrs.clear();

			}

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
