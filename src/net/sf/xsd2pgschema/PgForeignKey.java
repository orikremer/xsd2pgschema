/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2018 Masashi Yokochi

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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.github.antlr.grammars_v4.xpath.xpathBaseListener;
import com.github.antlr.grammars_v4.xpath.xpathLexer;
import com.github.antlr.grammars_v4.xpath.xpathParser;
import com.github.antlr.grammars_v4.xpath.xpathParser.MainContext;

/**
 * PostgreSQL foreign key declaration.
 *
 * @author yokochi
 */
public class PgForeignKey {

	/** The foreign key name in PostgreSQL. */
	protected String name;

	/** The PostgreSQL schema name. */
	protected String pg_schema_name;

	/** The child table name (canonical). */
	protected String child_table_xname;

	/** The child table name (in PostgreSQL). */
	protected String child_table_pname;

	/** The child field names (canonical), separated by comma character. */
	protected String child_field_xnames;

	/** The child field names (in PostgreSQL), separated by comma character. */
	protected String child_field_pnames;

	/** The parent table name (canonical). */
	protected String parent_table_xname = null;

	/** The parent table name (in PostgreSQL). */
	protected String parent_table_pname = null;

	/** The parent field names (canonical), separated by comma character. */
	protected String parent_field_xnames = null;

	/** The parent field names (in PostgreSQL), separated by comma character. */
	protected String parent_field_pnames = null;

	/**
	 * Instance of PgForeignKey.
	 *
	 * @param option PostgreSQL data model option
	 * @param pg_schema_name PostgreSQL schema name
	 * @param node current node
	 * @param name foreign key name
	 * @param key_name key name
	 * @throws PgSchemaException the pg schema exception
	 */
	public PgForeignKey(PgSchemaOption option, String pg_schema_name, Node node, String name, String key_name) throws PgSchemaException {

		String xs_prefix_ = option.xs_prefix_;

		this.name = name;

		this.pg_schema_name = pg_schema_name;

		child_table_xname = extractTableName(option, node);
		child_table_pname = option.case_sense ? child_table_xname : child_table_xname.toLowerCase();

		child_field_xnames = extractFieldNames(option, node);
		child_field_pnames = option.case_sense ? child_field_xnames : child_field_xnames.toLowerCase();

		NodeList key_nodes = node.getOwnerDocument().getElementsByTagName(xs_prefix_ + "key");

		for (int i = 0; i < key_nodes.getLength(); i++) {

			Node key_node = key_nodes.item(i);

			if (!key_name.equals(((Element) key_node).getAttribute("name")))
				continue;

			parent_table_xname = extractTableName(option, key_node);
			parent_table_pname = option.case_sense ? parent_table_xname : parent_table_xname.toLowerCase();

			parent_field_xnames = extractFieldNames(option, key_node);
			parent_field_pnames = option.case_sense ? parent_field_xnames : parent_field_xnames.toLowerCase();

			break;
		}

	}

	/**
	 * Extract child table name from xs:selector/@xpath.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 * @return String child table name
	 * @throws PgSchemaException the pg schema exception
	 */
	private String extractTableName(PgSchemaOption option, Node node) throws PgSchemaException {

		String xs_prefix_ = option.xs_prefix_;

		StringBuilder sb = new StringBuilder();

		try {

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (!child.getNodeName().equals(xs_prefix_ + "selector"))
					continue;

				String xpath_expr = ((Element) child).getAttribute("xpath");

				if (xpath_expr == null || xpath_expr.isEmpty())
					return null;

				xpathLexer lexer = new xpathLexer(CharStreams.fromString(xpath_expr));

				CommonTokenStream tokens = new CommonTokenStream(lexer);

				xpathParser parser = new xpathParser(tokens);
				parser.addParseListener(new xpathBaseListener());

				MainContext main = parser.main();

				ParseTree tree = main.children.get(0);
				String main_text = main.getText();

				if (parser.getNumberOfSyntaxErrors() > 0 || tree.getSourceInterval().length() == 0)
					throw new PgSchemaException("Invalid XPath expression. (" + main_text + ")");

				XPathCompList xpath_comp_list = new XPathCompList(tree);

				if (xpath_comp_list.comps.size() == 0)
					throw new PgSchemaException("Insufficient XPath expression. (" + main_text + ")");

				XPathComp[] last_qname_comp = xpath_comp_list.getLastQNameComp();

				for (XPathComp comp : last_qname_comp) {

					if (comp != null)
						sb.append(PgSchemaUtil.getUnqualifiedName(comp.tree.getText()) + ", ");

				}

				xpath_comp_list.clear();

				int len = sb.length();

				if (len > 0)
					sb.setLength(len - 2);

				break;
			}

			return sb.toString();

		} finally {
			sb.setLength(0);
		}

	}

	/**
	 * Extract child field names from xs:field/@xpath.
	 *
	 * @param option PostgreSQL data model option
	 * @param node current node
	 * @return String child field names separated by comma
	 * @throws PgSchemaException the pg schema exception
	 */
	private String extractFieldNames(PgSchemaOption option, Node node) throws PgSchemaException {

		String xs_prefix_ = option.xs_prefix_;

		StringBuilder sb = new StringBuilder();

		try {

			for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {

				if (child.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if (!child.getNodeName().equals(xs_prefix_ + "field"))
					continue;

				String xpath_expr = ((Element) child).getAttribute("xpath");

				if (xpath_expr == null || xpath_expr.isEmpty())
					return null;

				xpathLexer lexer = new xpathLexer(CharStreams.fromString(xpath_expr));

				CommonTokenStream tokens = new CommonTokenStream(lexer);

				xpathParser parser = new xpathParser(tokens);
				parser.addParseListener(new xpathBaseListener());

				MainContext main = parser.main();

				ParseTree tree = main.children.get(0);
				String main_text = main.getText();

				if (parser.getNumberOfSyntaxErrors() > 0 || tree.getSourceInterval().length() == 0)
					throw new PgSchemaException("Invalid XPath expression. (" + main_text + ")");

				XPathCompList xpath_comp_list = new XPathCompList(tree);

				if (xpath_comp_list.comps.size() == 0)
					throw new PgSchemaException("Insufficient XPath expression. (" + main_text + ")");

				XPathComp[] last_qname_comp = xpath_comp_list.getLastQNameComp();

				for (XPathComp comp : last_qname_comp) {

					if (comp != null)
						sb.append(PgSchemaUtil.getUnqualifiedName(comp.tree.getText()) + ", ");

				}

				xpath_comp_list.clear();

			}

			int len = sb.length();

			if (len > 0)
				sb.setLength(len - 2);

			return sb.toString();

		} finally {
			sb.setLength(0);
		}

	}

	/**
	 * Return whether foreign key is empty.
	 *
	 * @return boolean whether foreign key is empty
	 */
	public boolean isEmpty() {

		if (child_table_xname == null || child_table_xname.isEmpty())
			return true;

		if (parent_table_xname == null || parent_table_xname.isEmpty())
			return true;

		if (child_field_xnames == null || child_field_xnames.isEmpty())
			return true;

		if (parent_field_xnames == null || parent_field_xnames.isEmpty())
			return true;

		return false;
	}

	/**
	 * Return equality of foreign key.
	 *
	 * @param foreign_key compared foreign key
	 * @return boolean whether foreign key matches
	 */
	public boolean equals(PgForeignKey foreign_key) {
		return pg_schema_name.equals(foreign_key.pg_schema_name) &&
				child_table_xname.equals(foreign_key.child_table_xname) && parent_table_xname.equals(foreign_key.parent_table_xname) &&
				child_field_xnames.equals(foreign_key.child_field_xnames) && parent_field_xnames.equals(foreign_key.parent_field_xnames);
	}

}
