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
 * Nest tester to compose JSON document.
 *
 * @author yokochi
 */
public class JsonBuilderNestTester extends AbstractNestTester {

	/** The current indent level. */
	protected int current_indent_level = 1;

	/** The child indent level. */
	protected int child_indent_level = 2;

	/**
	 * Instance of nest tester from root node.
	 *
	 * @param table current table
	 * @param jsonb JSON builder
	 */
	public JsonBuilderNestTester(PgTable table, JsonBuilder jsonb) {

		super(table);

		child_indent_level = table.virtual ? current_indent_level : current_indent_level + 1;

	}

	/**
	 * Instance of nest tester from parent node.
	 *
	 * @param table current table
	 * @param parent_test nest test of parent node
	 */
	public JsonBuilderNestTester(PgTable table, JsonBuilderNestTester parent_test) {

		super(table, parent_test);

		current_indent_level = parent_test.child_indent_level;
		child_indent_level = table.virtual ? current_indent_level: current_indent_level + 1;

	}

}
