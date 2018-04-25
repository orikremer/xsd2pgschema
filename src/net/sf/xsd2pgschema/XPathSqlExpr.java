/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017-2018 Masashi Yokochi

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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.antlr.v4.runtime.tree.ParseTree;

/**
 * XPath SQL expression.
 *
 * @author yokochi
 */
public class XPathSqlExpr {

	/** The node path. */
	public String path = null;

	/** The column name. */
	public String column_name = null;

	/** The PostgreSQL XPath code. */
	public String pg_xpath_code = null;

	/** The predicate of XPath. */
	public String predicate = null;

	/** The predicate value of concerned SQL. */
	public String value = null;

	/** The terminus type. */
	public XPathCompType terminus = null;

	/** The parent tree. */
	public ParseTree parent_tree = null;

	/** The current tree. */
	public ParseTree current_tree = null;

	/** The unary operator. */
	public String unary_oprator = null;

	/** The binary operator. */
	public String binary_operator = null;

	/** The PostgreSQL table. */
	public PgTable table = null;

	/** The PostgreSQL field. */
	private PgField field = null;

	/**
	 * Instance of XPathSqlExpr.
	 *
	 * @param schema PostgreSQL data model
	 * @param path current path
	 * @param table PostgreSQL table
	 * @param column_name column name
	 * @param pg_xpath_code PostgreSQL XPath code
	 * @param predicate predicate
	 * @param terminus terminus type
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathSqlExpr(PgSchema schema, String path, PgTable table, String column_name, String pg_xpath_code, String predicate, XPathCompType terminus) throws PgSchemaException {

		this.path = path;
		this.table = table;
		this.column_name = column_name;
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
	 * @param column_name column name
	 * @param pg_xpath_code PostgreSQL XPath code
	 * @param predicate predicate
	 * @param terminus terminus type
	 * @param parent_tree parent parse tree
	 * @param current_tree current parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathSqlExpr(PgSchema schema, String path, PgTable table, String column_name, String pg_xpath_code, String predicate, XPathCompType terminus, ParseTree parent_tree, ParseTree current_tree) throws PgSchemaException {

		this.path = path;
		this.table = table;
		this.column_name = column_name;
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
	 * @param column_name column name
	 * @param pg_xpath_code PostgreSQL XPath code
	 * @param predicate predicate
	 * @param terminus terminus type
	 * @param parent_tree parent parse tree
	 * @param current_tree current parse tree
	 * @param unary_operator unary operator code
	 * @param binary_operator binary operator code
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathSqlExpr(PgSchema schema, String path, PgTable table, String column_name, String pg_xpath_code, String predicate, XPathCompType terminus, ParseTree parent_tree, ParseTree current_tree, String unary_operator, String binary_operator) throws PgSchemaException {

		this.path = path;
		this.table = table;
		this.column_name = column_name;
		this.predicate = this.value = predicate;
		this.terminus = terminus;
		this.parent_tree = parent_tree;
		this.current_tree = current_tree;
		this.unary_oprator = unary_operator;
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

		if (column_name == null || column_name.equals("*"))
			return;

		switch (terminus) {
		case element:
			if (!table.fields.stream().anyMatch(field -> field.element && field.xname.equals(column_name))) {
				if (current_tree != null)
					throw new PgSchemaException(current_tree);
				else throw new PgSchemaException();
			}
			break;
		case simple_content:
			if (!table.fields.stream().anyMatch(field -> field.simple_content && field.xname.equals(column_name))) {
				if (current_tree != null)
					throw new PgSchemaException(current_tree);
				else throw new PgSchemaException();
			}
			break;
		case attribute:
			if (!table.fields.stream().anyMatch(field -> field.attribute && field.xname.equals(column_name))) {
				if (current_tree != null)
					throw new PgSchemaException(current_tree);
				else throw new PgSchemaException();
			}
			break;
		case any_element:
			if (!table.fields.stream().anyMatch(field -> field.any && field.xname.equals(column_name))) {
				if (current_tree != null)
					throw new PgSchemaException(current_tree);
				else throw new PgSchemaException();
			}
			break;
		case any_attribute:
			if (!table.fields.stream().anyMatch(field -> field.any_attribute && field.xname.equals(column_name))) {
				if (current_tree != null)
					throw new PgSchemaException(current_tree);
				else throw new PgSchemaException();
			}
			break;
		default:
			throw new PgSchemaException();
		}

		field = table.getField(column_name);

		if (field == null)
			throw new PgSchemaException();

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
	 * Return whether relational expression is empty or not.
	 *
	 * @return boolean whether the relational expression is empty or not
	 */
	private boolean isEmptyRelation() {
		return table == null || column_name == null;
	}

	/**
	 * Return equality of relational expression.
	 *
	 * @param sql_expr compared SQL expression
	 * @return boolean whether the SQL expression matches or not
	 */
	public boolean equalsRelationally(XPathSqlExpr sql_expr) {

		if (isEmptyRelation() || sql_expr.isEmptyRelation())
			return false;

		if (!terminus.equals(sql_expr.terminus))
			return false;

		if (!table.equals(sql_expr.table) || !column_name.equals(sql_expr.column_name))
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

		Pattern pattern = Pattern.compile("^xpath\\(\\'\\/" + table.name + "\\/(.*)\\', .*\\)$");

		Matcher matcher = pattern.matcher(pg_xpath_code);

		return matcher.find() ? matcher.group(1) : null;
	}

}
