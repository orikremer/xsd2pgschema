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
 * Common nest tester.
 *
 * @author yokochi
 */
public abstract class CommonBuilderNestTester {

	/** The ancestor node name. */
	protected String ancestor_node;

	/** The parent node name. */
	protected String parent_node;

	/** Whether this node has content. */
	public boolean has_content = false;

	/** Whether this node has simple content. */
	public boolean has_simple_content = false;

	/**
	 * Instance of nest tester from root node.
	 *
	 * @param table current table
	 * @throws PgSchemaException the pg schema exception
	 */
	public CommonBuilderNestTester(PgTable table) throws PgSchemaException {

		ancestor_node = "";
		parent_node = table.xname;

		if (!table.virtual) {

			ancestor_node = parent_node;
			parent_node = table.xname;

		}

	}

	/**
	 * Instance of nest tester from parent node.
	 *
	 * @param table current table
	 * @param parent_test nest test of parent node
	 * @throws PgSchemaException the pg schema exception
	 */
	public CommonBuilderNestTester(PgTable table, CommonBuilderNestTester parent_test) throws PgSchemaException {

		ancestor_node = parent_test.ancestor_node;
		parent_node = parent_test.parent_node;

		if (!table.virtual) {

			ancestor_node = parent_node;
			parent_node = table.xname;

		}

	}

}
