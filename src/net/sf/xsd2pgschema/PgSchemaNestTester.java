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
 * Nest tester.
 *
 * @author yokochi
 */
public class PgSchemaNestTester {

	/** The ancestor node name. */
	protected String ancestor_node = null;

	/** The parent node name. */
	protected String parent_node = null;

	/** The current indent space. */
	protected String current_indent_space = "";

	/** The child indent space. */
	protected String child_indent_space = "";

	/** The unit of indent space. */
	protected String indent_space = null;

	/** The indent offset. */
	protected int indent_offset = PgSchemaUtil.indent_offset;

	/** Whether this node has child element. */
	boolean has_child_elem = false;

	/** Whether this node has content. */
	boolean has_content = false;

	/** Whether this node has simple content. */
	boolean has_simple_content = false;

	/** Whether this node has opened simple content. */
	boolean has_open_simple_content = false;

	/**
	 * Instance of nest tester from root node.
	 *
	 * @param table current table
	 * @param xmlb XML builder
	 */
	public PgSchemaNestTester(PgTable table, XmlBuilder xmlb) {

		ancestor_node = "";
		parent_node = table.xname;

		if (!table.virtual) {

			ancestor_node = parent_node;
			parent_node = table.xname;

		}

		StringBuilder sb = new StringBuilder();

		for (int l = 0; l < xmlb.indent_offset; l++)
			sb.append(" ");

		indent_offset = xmlb.indent_offset;
		indent_space = sb.toString();

		sb.setLength(0);

		current_indent_space = xmlb.init_indent_space;
		child_indent_space = table.virtual ? current_indent_space: current_indent_space + indent_space;

	}

	/**
	 * Instance of nest tester from parent node.
	 *
	 * @param table current table
	 * @param parent_test nest test of parent node
	 */
	public PgSchemaNestTester(PgTable table, PgSchemaNestTester parent_test) {

		ancestor_node = parent_test.ancestor_node;
		parent_node = parent_test.parent_node;

		if (!table.virtual) {

			ancestor_node = parent_node;
			parent_node = table.xname;

		}

		indent_offset = parent_test.indent_offset;
		indent_space = parent_test.indent_space;

		current_indent_space = parent_test.child_indent_space;
		child_indent_space = table.virtual ? current_indent_space: current_indent_space + indent_space;

	}

	/**
	 * Merge test result.
	 *
	 * @param test nest test of child node
	 */
	public void mergeTest(PgSchemaNestTester test) {

		has_child_elem |= test.has_child_elem;
		has_content |= test.has_content;
		has_simple_content |= test.has_simple_content;
		has_open_simple_content |= test.has_open_simple_content;

	}

	/**
	 * Return indent space of parent node.
	 *
	 * @return String indent space of parent node
	 */
	public String getParentIndentSpace() {
		return current_indent_space.length() > indent_offset ? current_indent_space.substring(indent_offset) : "";
	}

}
