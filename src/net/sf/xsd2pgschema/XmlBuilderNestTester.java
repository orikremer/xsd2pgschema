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
 * Nest tester to compose XML document.
 *
 * @author yokochi
 */
public class XmlBuilderNestTester extends AbstractNestTester {

	/** The current indent space. */
	protected String current_indent_space = "";

	/** The child indent space. */
	protected String child_indent_space = "";

	/** The unit of indent space. */
	protected String indent_space = null;

	/** The indent offset. */
	protected int indent_offset = PgSchemaUtil.indent_offset;

	/**
	 * Instance of nest tester from root node.
	 *
	 * @param table current table
	 * @param xmlb XML builder
	 */
	public XmlBuilderNestTester(PgTable table, XmlBuilder xmlb) {

		super(table);

		StringBuilder sb = new StringBuilder();

		for (int l = 0; l < xmlb.indent_offset; l++)
			sb.append(" ");

		indent_offset = xmlb.indent_offset;
		indent_space = sb.toString();

		sb.setLength(0);

		current_indent_space = "";
		child_indent_space = table.virtual ? current_indent_space : current_indent_space + indent_space;

	}

	/**
	 * Instance of nest tester from parent node.
	 *
	 * @param table current table
	 * @param parent_test nest test of parent node
	 */
	public XmlBuilderNestTester(PgTable table, XmlBuilderNestTester parent_test) {

		super(table, parent_test);

		indent_offset = parent_test.indent_offset;
		indent_space = parent_test.indent_space;

		current_indent_space = parent_test.child_indent_space;
		child_indent_space = table.virtual ? current_indent_space : current_indent_space + indent_space;

	}

}
