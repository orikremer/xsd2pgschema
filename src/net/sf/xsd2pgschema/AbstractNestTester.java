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
 * Abstract nest tester.
 *
 * @author yokochi
 */
public abstract class AbstractNestTester {

	/** The ancestor node name. */
	protected String ancestor_node;

	/** The parent node name. */
	protected String parent_node;

	/** Whether this node has child element. */
	protected boolean has_child_elem = false;

	/** Whether this node has content. */
	protected boolean has_content = false;

	/** Whether this node has simple content. */
	protected boolean has_simple_content = false;

	/** Whether this node has opened simple content. */
	protected boolean has_open_simple_content = false;

	/** Whether this node has inserted document key. */
	protected boolean has_insert_doc_key = false;

	/**
	 * Instance of nest tester from root node.
	 *
	 * @param table current table
	 */
	public AbstractNestTester(PgTable table) {

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
	 */
	public AbstractNestTester(PgTable table, AbstractNestTester parent_test) {

		ancestor_node = parent_test.ancestor_node;
		parent_node = parent_test.parent_node;

		if (!table.virtual) {

			ancestor_node = parent_node;
			parent_node = table.xname;

		}

		has_insert_doc_key = parent_test.has_insert_doc_key;

	}

	/**
	 * Merge test result.
	 *
	 * @param test nest test of child node
	 */
	public void merge(AbstractNestTester test) {

		has_child_elem |= test.has_child_elem;
		has_content |= test.has_content;
		has_simple_content |= test.has_simple_content;
		has_open_simple_content |= test.has_open_simple_content;

	}

}
