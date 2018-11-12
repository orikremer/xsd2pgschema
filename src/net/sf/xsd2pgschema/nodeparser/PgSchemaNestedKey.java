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

package net.sf.xsd2pgschema.nodeparser;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgTable;

/**
 * Nested key
 *
 * @author yokochi
 */
public class PgSchemaNestedKey {

	/** The nested table. */
	protected PgTable table;

	/** Whether nested key is list holder. */
	protected boolean list_holder;

	/** Whether nested key as attribute. */
	protected boolean as_attr;

	/** The current key. */
	protected String current_key;

	/** The parent key. */
	protected String parent_key;

	/** Whether child node is not nested node (indirect). */
	protected boolean indirect;

	/** The target ordinal number of current node. */
	protected int target_ordinal = 0;

	/**
	 * Instance of nested key.
	 *
	 * @param nested_table nested table
	 * @param field current field as nested key
	 * @param current_key current key
	 */
	public PgSchemaNestedKey(PgTable nested_table, PgField field, String current_key) {

		table = nested_table;

		list_holder = field.list_holder;
		as_attr = field.nested_key_as_attr;

		if (!table.virtual)
			current_key += "/" + (field.nested_key_as_attr ? "@" : "") + field.foreign_table_xname; // XPath child

		this.current_key = current_key;

	}

	/**
	 * Set nested key as is.
	 *
	 * @param node_parser node parser
	 * @return PgSchemaNestedKey nested key
	 */
	protected PgSchemaNestedKey asIs(PgSchemaNodeParser node_parser) {

		parent_key = node_parser.current_key;
		indirect = false;

		return this;
	}

	/**
	 * Set nested key as of child.
	 *
	 * @param node_parser node parser
	 * @return PgSchemaNestedKey nested key
	 */
	protected PgSchemaNestedKey asOfChild(PgSchemaNodeParser node_parser) {

		PgSchemaNodeTester node_test = node_parser.node_test;

		parent_key = node_test.primary_key;

		if (indirect = !node_parser.existsNestedNode(node_test.proc_node, table))
			target_ordinal = node_test.node_ordinal;

		return this;
	}

}
