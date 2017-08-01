/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017 Masashi Yokochi

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

import org.antlr.v4.runtime.tree.ParseTree;

/**
 * XPath SQL expression.
 *
 * @author yokochi
 */
public class XPathSqlExpr {

	/** The path. */
	public String path = null;

	/** The table name. */
	public String table_name = null;

	/** The column name. */
	public String column_name = null;

	/** The predicate of XPath. */
	public String predicate = null;

	/** The predicate value of concerned SQL. */
	public String value = null;

	/** The terminus type. */
	public XPathCompType terminus = null;

	/** The parent tree. */
	public ParseTree parent_tree = null;

	/** The current tree. */
	public ParseTree current_tree= null;

	/** The unary operator. */
	public String unary_oprator = null;

	/** The binary operator. */
	public String binary_operator = null;

	/** The PostgreSQL table. */
	private PgTable table = null;

	/** The PostgreSQL field. */
	private PgField field = null;

	/**
	 * Instance of XPathSqlExpr.
	 *
	 * @param schema PostgreSQL data model
	 * @param path current path
	 * @param table_name table name
	 * @param column_name column name
	 * @param predicate predicate
	 * @param terminus terminus type
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathSqlExpr(PgSchema schema, String path, String table_name, String column_name, String predicate, XPathCompType terminus) throws PgSchemaException {

		this.path = path;
		this.table_name = table_name;
		this.column_name = column_name;
		this.predicate = this.value = predicate;
		this.terminus = terminus;

		switch (terminus) {
		case element:
		case simple_content:
		case attribute:
			decideField(schema);
			break;
		case table:
			decideTable(schema);
			break;
		default:
		}

	}

	/**
	 * Instance of XPathSqlExpr.
	 *
	 * @param schema PostgreSQL data model
	 * @param path current path
	 * @param table_name table name
	 * @param column_name column name
	 * @param predicate predicate
	 * @param terminus terminus type
	 * @param parent_tree parent parse tree
	 * @param current_tree current parse tree
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathSqlExpr(PgSchema schema, String path, String table_name, String column_name, String predicate, XPathCompType terminus, ParseTree parent_tree, ParseTree current_tree) throws PgSchemaException {

		this.path = path;
		this.table_name = table_name;
		this.column_name = column_name;
		this.predicate = this.value = predicate;
		this.terminus = terminus;
		this.parent_tree = parent_tree;
		this.current_tree = current_tree;

		switch (terminus) {
		case element:
		case simple_content:
		case attribute:
			decideField(schema);
			break;
		case table:
			decideTable(schema);
			break;
		default:
		}

		validatePredicate(schema);

	}

	/**
	 * Instance of XPathSqlExpr.
	 *
	 * @param schema PostgreSQL data model
	 * @param path current path
	 * @param table_name table name
	 * @param column_name column name
	 * @param predicate predicate
	 * @param terminus terminus type
	 * @param parent_tree parent parse tree
	 * @param current_tree current parse tree
	 * @param unary_operator unary operator code
	 * @param binary_operator binary operator code
	 * @throws PgSchemaException the pg schema exception
	 */
	public XPathSqlExpr(PgSchema schema, String path, String table_name, String column_name, String predicate, XPathCompType terminus, ParseTree parent_tree, ParseTree current_tree, String unary_operator, String binary_operator) throws PgSchemaException {

		this.path = path;
		this.table_name = table_name;
		this.column_name = column_name;
		this.predicate = this.value = predicate;
		this.terminus = terminus;
		this.parent_tree = parent_tree;
		this.current_tree = current_tree;
		this.unary_oprator = unary_operator;
		this.binary_operator = binary_operator;

		switch (terminus) {
		case element:
		case simple_content:
		case attribute:
			decideField(schema);
			break;
		case table:
			decideTable(schema);
			break;
		default:
		}

		validatePredicate(schema);

	}

	/**
	 * Decide PostgreSQL table.
	 *
	 * @param schema PostgreSQL data model
	 * @throws PgSchemaException the pg schema exception
	 */
	private void decideTable(PgSchema schema) throws PgSchemaException {

		if (table_name == null)
			return;

		int table_id = schema.getTableId(table_name);

		if (table_id < 0)
			throw new PgSchemaException(current_tree);

		table = schema.getTable(table_id);

	}

	/**
	 * Decide PostgreSQL field.
	 *
	 * @param schema PostgreSQL data model
	 * @throws PgSchemaException the pg schema exception
	 */
	private void decideField(PgSchema schema) throws PgSchemaException {

		decideTable(schema);

		if (table == null)
			return;

		if (column_name == null || column_name.equals("*"))
			return;

		switch (terminus) {
		case element:
			if (!table.fields.stream().anyMatch(field -> field.element && field.xname.equals(column_name)))
				throw new PgSchemaException(current_tree);
			break;
		case simple_content:
			if (!table.fields.stream().anyMatch(field -> field.simple_cont && field.xname.equals(column_name)))
				throw new PgSchemaException(current_tree);
			break;
		case attribute:
			if (!table.fields.stream().anyMatch(field -> field.attribute && field.xname.equals(column_name)))
				throw new PgSchemaException(current_tree);
			break;
		default:
			throw new PgSchemaException(current_tree);
		}

		int field_id = table.getFieldId(column_name);

		if (field_id < 0)
			throw new PgSchemaException(current_tree);

		field = table.fields.get(field_id);

	}

	/**
	 * Validate predicate expression.
	 *
	 * @param schema PostgreSQL data model
	 * @throws PgSchemaException the pg schema exception
	 */
	private void validatePredicate(PgSchema schema) throws PgSchemaException {

		if (predicate == null)
			return;

		decideField(schema);

		if (field == null)
			return;

		if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith("\"") && value.endsWith("\"")))
			value = predicate.substring(1, value.length() - 1);

		if (!XsDataType.isValid(field, value))
			throw new PgSchemaException(current_tree);

		value = XsDataType.getSqlPredicate(field, value);

	}

	/**
	 * Return whether relational expression is empty or not.
	 *
	 * @return boolean whether the relational expression is empty or not
	 */
	public boolean isEmptyRelation() {
		return table_name == null || column_name == null;
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

		return table_name.equals(sql_expr.table_name) && column_name.equals(sql_expr.column_name);
	}

}
