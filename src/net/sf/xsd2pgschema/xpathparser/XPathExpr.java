/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2017-2020 Masashi Yokochi

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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * XPath expression.
 *
 * @author yokochi
 */
public class XPathExpr implements Serializable {

	/** The default serial version ID. */
	private static final long serialVersionUID = 1L;

	/** The node path. */
	protected String path;

	/** The terminus type. */
	public XPathCompType terminus;

	/** The previous terminus type. */
	protected XPathCompType prev_term = null;

	/** The realized SQL statement. */
	public String sql = null;

	/** The subject SQL expression. */
	public XPathSqlExpr sql_subject = null;

	/** The predicate SQL expression. */
	protected List<XPathSqlExpr> sql_predicates = null;

	/** The adverb SQL expression. */
	protected XPathSqlExpr sql_adverb = null;

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
	 * Instance of XPathExpr for appending text node.
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
	 * @return boolean whether the path expression matches
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

	/**
	 * Set a SQL expression as adverb.
	 *
	 * @param sql_expr adverb SQL expression
	 */
	public void setAdverbSql(XPathSqlExpr sql_expr) {

		if (sql_adverb == null)
			sql_adverb = sql_expr;

	}

	/**
	 * Return parent path.
	 *
	 * @return String parent path
	 */
	public String getParentPath() {

		StringBuilder sb = new StringBuilder();

		String[] _path;

		switch (terminus) {
		case any_element:
			if (path.split(" ").length > 1) {

				_path = path.split(" ");

				try {

					for (int i = 0; i < _path.length - 1; i++)
						sb.append(_path[i] + " ");

					return sb.toString().trim();

				} finally {
					sb.setLength(0);
				}
			}
		default:
			_path = path.split("/");

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
	 * Return the last node name.
	 *
	 * @return String the last node name
	 */
	public String getLastNodeName() {

		String[] _path = path.split(" ").length < 2 ? path.replaceFirst("//$", "").split("/") : path.replaceFirst("//$", "").split(" ");

		int position = _path.length - 1;

		if (position < 0)
			return null;

		return _path[position];
	}

	/**
	 * Return the parent node name.
	 *
	 * @return String the parent node name
	 */
	public String getParentNodeName() {

		String[] _path = path.split(" ").length < 2 ? path.replaceFirst("//$", "").split("/") : path.replaceFirst("//$", "").split(" ");

		int position = _path.length - 2;

		if (position < 0)
			return null;

		return _path[position];
	}

	/**
	 * Return readable path.
	 *
	 * @return String readable XPath
	 */
	public String getReadablePath() {
		return path.replace(' ', '/');
	}

	/**
	 * Return parent terminus.
	 *
	 * @return XPathCompType parent terminus type
	 */
	public XPathCompType getParentTerminus() {

		switch (terminus) {
		case table:
		case element:
		case simple_content:
		case attribute:
		case any_attribute:
			return XPathCompType.table;
		case any_element:
			return path.split(" ").length < 2 ? XPathCompType.table : XPathCompType.any_element;
		default:
			return prev_term;
		}

	}

}
