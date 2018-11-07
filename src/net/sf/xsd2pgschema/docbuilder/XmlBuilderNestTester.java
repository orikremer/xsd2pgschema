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
import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.PgTable;

/**
 * Nest tester for XML builder.
 *
 * @author yokochi
 */
public class XmlBuilderNestTester extends CommonBuilderNestTester {

	/** The current indent space. */
	public String current_indent_space;

	/** The child indent space. */
	public String child_indent_space;

	/** The current indent space as byte array. */
	public byte[] current_indent_bytes;

	/** The child indent space as byte array. */
	public byte[] child_indent_bytes;

	/** The unit of indent space. */
	protected String indent_space;

	/** The indent offset. */
	protected int indent_offset;

	/**
	 * Instance of nest tester from root node.
	 *
	 * @param table current table
	 * @param xmlb XML builder
	 * @throws PgSchemaException the pg schema exception
	 */
	public XmlBuilderNestTester(PgTable table, XmlBuilder xmlb) throws PgSchemaException {

		super(table);

		StringBuilder sb = new StringBuilder();

		for (int l = 0; l < xmlb.indent_offset; l++)
			sb.append(" ");

		indent_offset = xmlb.indent_offset;
		indent_space = sb.toString();

		sb.setLength(0);

		current_indent_space = "";
		child_indent_space = table.virtual ? current_indent_space : current_indent_space + indent_space;

		current_indent_bytes = xmlb.getSimpleBytes(current_indent_space); // .getBytes(PgSchemaUtil.latin_1_charset);
		child_indent_bytes = xmlb.getSimpleBytes(child_indent_space); // .getBytes(PgSchemaUtil.latin_1_charset);

	}

	/**
	 * Instance of nest tester from parent node.
	 *
	 * @param table current table
	 * @param parent_test nest test of parent node
	 * @throws PgSchemaException the pg schema exception
	 */
	public XmlBuilderNestTester(PgTable table, XmlBuilderNestTester parent_test) throws PgSchemaException {

		super(table, parent_test);

		indent_offset = parent_test.indent_offset;
		indent_space = parent_test.indent_space;

		current_indent_space = parent_test.child_indent_space;
		child_indent_space = table.virtual ? current_indent_space : current_indent_space + indent_space;

		current_indent_bytes = parent_test.child_indent_bytes;
		child_indent_bytes = child_indent_space.getBytes(PgSchemaUtil.latin_1_charset);

	}

}
