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

import java.util.ArrayList;
import java.util.List;

/**
 * XPath expression.
 *
 * @author yokochi
 */
public class XPathExpr {

	/** The path. */
	public String path = null;

	/** The terminus type. */
	public XPathCompType terminus = null;

	/** The previous terminus type. */
	public XPathCompType prev_term = null;

	/** The realized SQL sentence. */
	public String sql = null;

	/** The subject SQL expression. */
	public XPathSqlExpr sql_subject = null;

	/** The predicate SQL expression. */
	public List<XPathSqlExpr> sql_predicates = null;

	/**
	 * Instance of XPathExpr.
	 *
	 * @param path_expr current path expression
	 */
	public XPathExpr(XPathExpr path_expr) {

		this.path = path_expr.path;
		this.terminus = path_expr.terminus;

	}

	/**
	 * Instance of XPathExpr.
	 *
	 * @param path current path
	 * @param terminus current terminus
	 */
	public XPathExpr(String path, XPathCompType terminus) {

		this.path = path;
		this.terminus = terminus;

	}

	/**
	 * Instance of XPathExpr prepared for appending text node.
	 *
	 * @param path current path
	 * @param terminus current terminus
	 * @param prev_term previous terminus
	 */
	public XPathExpr(String path, XPathCompType terminus, XPathCompType prev_term) {

		this.path = path;
		this.terminus = terminus;
		this.prev_term = prev_term;

	}

	/**
	 * Return equality of path expression.
	 *
	 * @param path_expr compared path expression
	 * @return boolean whether the path expression matches or not
	 */
	public boolean equals(XPathExpr path_expr) {
		return path.equals(path_expr.path) && terminus.equals(path_expr.terminus);
	}

	/**
	 * Set primary SQL expression.
	 *
	 * @param sql_expr subject SQL expression
	 */
	public void setSubjectSql(XPathSqlExpr sql_expr) {

		sql_subject = sql_expr;

	}

	/**
	 * Append a SQL expression of predicate.
	 *
	 * @param sql_expr predicate SQL expression
	 */
	public void appendPredicateSql(XPathSqlExpr sql_expr) {

		if (sql_predicates == null)
			sql_predicates = new ArrayList<XPathSqlExpr>();

		sql_predicates.add(sql_expr);

	}

}
