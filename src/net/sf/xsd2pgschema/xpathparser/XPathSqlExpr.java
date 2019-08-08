/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017-2019 Masashi Yokochi

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

package net.sf.xsd2pgschema.xpathparser;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.antlr.v4.runtime.tree.ParseTree;

import net.sf.xsd2pgschema.PgField;
import net.sf.xsd2pgschema.PgSchema;
import net.sf.xsd2pgschema.PgSchemaException;
import net.sf.xsd2pgschema.PgTable;

/**
 * XPath SQL expression.
 *
 * @author yokochi
 */
public class XPathSqlExpr {

	/** The node path. */
	protected String path;

	/** The canonical name in XML Schema. */
	protected String xname;

	/** The column name in PostgreSQL. */
	protected String pname;

	/** The PostgreSQL XPath code. */
	protected String pg_xpath_code = null;

	/** The predicate of XPath. */
	protected String predicate;

	/** The predicate value of concerned SQL. */
	protected String value;

	/** The terminus type. */
	protected XPathCompType terminus;

	/** The parent tree. */
	protected ParseTree parent_tree = null;

	/** The current tree. */
	protected ParseTree current_tree = null;

	/** The unary operator. */
	protected String unary_operator = null;

	/** The binary operator. */
	protected String binary_operator = null;

	/** The PostgreSQL table. */
	public PgTable table;

	/** The PostgreSQL field. */
	public PgField field = null;

	/**
	 * Instance of XPathSqlExpr.
	 *
	 * @param schema PostgreSQL data model
	 * @param path current path
	 * @param table PostgreSQL table
	 * @param xname canonical name of column
	 * @param pg_xpath_code PostgreSQL XPath code
	 * @param predicate predicate
	 * @param terminus terminus type
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathSqlExpr(PgSchema schema, String path, PgTable table, String xname, String pg_xpath_code, String predicate, XPathCompType terminus) throws PgSchemaException {

		this.path = path;
		this.table = table;
		this.xname = pname = xname;
		this.predicate = this.value = predicate;
		this.terminus = terminus;

		switch (terminus) {
		case any_element:
		case any_attribute:
			this.pg_xpath_code = pg_xpath_code;
		case element:
		case simple_content:
		case attribute:
			decideField(schema, null);
			break;
		case table:
			if (table == null)
				throw new PgSchemaException();
			break;
		default:
		}

	}

	/**
	 * Instance of XPathSqlExpr.
	 *
	 * @param schema PostgreSQL data model
	 * @param path current path
	 * @param table PostgreSQL table
	 * @param xname canonical name of column
	 * @param pg_xpath_code PostgreSQL XPath code
	 * @param predicate predicate
	 * @param terminus terminus type
	 * @param parent_tree parent parse tree
	 * @param current_tree current parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathSqlExpr(PgSchema schema, String path, PgTable table, String xname, String pg_xpath_code, String predicate, XPathCompType terminus, ParseTree parent_tree, ParseTree current_tree) throws PgSchemaException {

		this.path = path;
		this.table = table;
		this.xname = pname = xname;
		this.predicate = this.value = predicate;
		this.terminus = terminus;
		this.parent_tree = parent_tree;
		this.current_tree = current_tree;

		switch (terminus) {
		case any_element:
		case any_attribute:
			this.pg_xpath_code = pg_xpath_code;
		case element:
		case simple_content:
		case attribute:
			decideField(schema, current_tree);
			break;
		case table:
			if (table == null)
				throw new PgSchemaException(current_tree);
			break;
		default:
		}

		validatePredicate();

	}

	/**
	 * Instance of XPathSqlExpr.
	 *
	 * @param schema PostgreSQL data model
	 * @param path current path
	 * @param table PostgreSQL table
	 * @param xname canonical name of column
	 * @param pg_xpath_code PostgreSQL XPath code
	 * @param predicate predicate
	 * @param terminus terminus type
	 * @param parent_tree parent parse tree
	 * @param current_tree current parse tree
	 * @param unary_operator unary operator code
	 * @param binary_operator binary operator code
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathSqlExpr(PgSchema schema, String path, PgTable table, String xname, String pg_xpath_code, String predicate, XPathCompType terminus, ParseTree parent_tree, ParseTree current_tree, String unary_operator, String binary_operator) throws PgSchemaException {

		this.path = path;
		this.table = table;
		this.xname = pname = xname;
		this.predicate = this.value = predicate;
		this.terminus = terminus;
		this.parent_tree = parent_tree;
		this.current_tree = current_tree;
		this.unary_operator = unary_operator;
		this.binary_operator = binary_operator;

		switch (terminus) {
		case any_element:
		case any_attribute:
			this.pg_xpath_code = pg_xpath_code;
		case element:
		case simple_content:
		case attribute:
			decideField(schema, current_tree);
			break;
		case table:
			if (table == null)
				throw new PgSchemaException(current_tree);
			break;
		default:
		}

		validatePredicate();

	}

	/**
	 * Decide PostgreSQL field.
	 *
	 * @param schema PostgreSQL data model
	 * @param current_tree current parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	private void decideField(PgSchema schema, ParseTree current_tree) throws PgSchemaException {

		if (table == null) {
			if (current_tree != null)
				throw new PgSchemaException(current_tree);
			else throw new PgSchemaException();
		}

		if (xname == null || xname.equals("*"))
			return;

		Optional<PgField> opt;

		switch (terminus) {
		case element:
			if (table.has_element) {
				opt = table.elem_fields.stream().filter(field -> field.element && field.xname.equals(xname)).findFirst();
				if (opt.isPresent()) {
					field = opt.get();
					break;
				}
			}
			if (current_tree != null)
				throw new PgSchemaException(current_tree);
			else throw new PgSchemaException();
		case simple_content:
			if (table.has_simple_content) {
				opt = table.elem_fields.stream().filter(field -> field.simple_content && !field.simple_attribute && field.xname.equals(xname)).findFirst();
				if (opt.isPresent()) {
					field = opt.get();
					break;
				}
			}
			if (current_tree != null)
				throw new PgSchemaException(current_tree);
			else throw new PgSchemaException();
		case attribute:
			if (table.has_attribute || table.has_simple_attribute) {
				opt = table.attr_fields.stream().filter(field -> (field.attribute || field.simple_attribute || field.simple_attr_cond) && (field.attribute ? field.xname.equals(xname) : field.containsParentNodeName(xname))).findFirst();
				if (opt.isPresent()) {
					field = opt.get();
					break;
				}
			}
			if (current_tree != null)
				throw new PgSchemaException(current_tree);
			else throw new PgSchemaException();
		case any_element:
			if (table.has_any) {
				opt = table.elem_fields.stream().filter(field -> field.any && field.xname.equals(xname)).findFirst();
				if (opt.isPresent()) {
					field = opt.get();
					break;
				}
			}
			if (current_tree != null)
				throw new PgSchemaException(current_tree);
			else throw new PgSchemaException();
		case any_attribute:
			if (table.has_any_attribute) {
				opt = table.attr_fields.stream().filter(field -> field.any_attribute && field.xname.equals(xname)).findFirst();
				if (opt.isPresent()) {
					field = opt.get();
					break;
				}
			}
			if (current_tree != null)
				throw new PgSchemaException(current_tree);
			else throw new PgSchemaException();
		default:
			throw new PgSchemaException();
		}

		pname = field.pname;

	}

	/**
	 * Validate predicate expression.
	 *
	 * @throws PgSchemaException the pg schema exception
	 */
	private void validatePredicate() throws PgSchemaException {

		if (predicate == null)
			return;

		if (field == null)
			return;

		if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith("\"") && value.endsWith("\"")))
			value = predicate.substring(1, value.length() - 1);

		if (!field.validate(value))
			throw new PgSchemaException();

		value = field.getSqlPredicate(value);

	}

	/**
	 * Return whether relational expression is empty.
	 *
	 * @return boolean whether relational expression is empty
	 */
	private boolean isEmptyRelation() {
		return table == null || xname == null;
	}

	/**
	 * Return equality of relational expression.
	 *
	 * @param sql_expr compared SQL expression
	 * @return boolean whether SQL expression matches
	 */
	public boolean equalsRelationally(XPathSqlExpr sql_expr) {

		if (isEmptyRelation() || sql_expr.isEmptyRelation())
			return false;

		if (!terminus.equals(sql_expr.terminus))
			return false;

		if (!table.equals(sql_expr.table) || !xname.equals(sql_expr.xname))
			return false;

		return pg_xpath_code == null ? true : pg_xpath_code.equals(sql_expr.pg_xpath_code);
	}

	/**
	 * Return parent path.
	 *
	 * @return String parent path
	 */
	public String getParentPath() {

		String _path_ = path;

		switch (terminus) {
		case any_element:
			if (path.split(" ").length > 1)
				_path_ = path.split(" ")[0];
		default:
			String[] _path = _path_.split("/");

			StringBuilder sb = new StringBuilder();

			try {

				for (int i = 1; i < _path.length - 1; i++)
					sb.append("/" + _path[i]);

				return sb.toString();

			} finally {
				sb.setLength(0);
			}
		}

	}

	/**
	 * Return fragment of XPath expression
	 *
	 * @return String fragment of XPath expression
	 */
	public String getXPathFragment() {

		if (pg_xpath_code == null)
			return null;

		Pattern pattern = Pattern.compile("^xpath\\(\\'\\/" + table.pname + "\\/(.*)\\', .*\\)$");

		Matcher matcher = pattern.matcher(pg_xpath_code);

		return matcher.find() ? matcher.group(1) : null;
	}

}
