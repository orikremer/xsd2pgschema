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

import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgTable;

/**
 * Nest tester for JSON builder.
 *
 * @author yokochi
 */
public class JsonBuilderNestTester extends CommonBuilderNestTester {

	/** The current indent level. */
	public int current_indent_level;

	/** The child indent level. */
	public int child_indent_level;

	/**
	 * Instance of nest tester from root node.
	 *
	 * @param table current table
	 * @param jsonb JSON builder
	 * @throws PgSchemaException the pg schema exception
	 */
	public JsonBuilderNestTester(PgTable table, JsonBuilder jsonb) throws PgSchemaException {

		super(table);

		current_indent_level = 1;
		child_indent_level = table.virtual ? current_indent_level : current_indent_level + 1;

	}

	/**
	 * Instance of nest tester from parent node.
	 *
	 * @param table current table
	 * @param parent_test nest test of parent node
	 * @throws PgSchemaException the pg schema exception
	 */
	public JsonBuilderNestTester(PgTable table, JsonBuilderNestTester parent_test) throws PgSchemaException {

		super(table, parent_test);

		current_indent_level = parent_test.child_indent_level;
		child_indent_level = table.virtual ? current_indent_level: current_indent_level + 1;

	}

}
