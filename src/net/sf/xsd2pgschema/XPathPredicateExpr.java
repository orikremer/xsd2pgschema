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

import java.util.ArrayList;
import java.util.List;

/**
 * XPath predicate expression.
 *
 * @author yokochi
 */
public class XPathPredicateExpr {

	/** The XPath component of predicate. */
	protected XPathComp src_comp = null;

	/** The source path expression of predicate. */
	protected XPathExpr src_path_expr = null;

	/** The destination path expression of predicate. */
	protected List<XPathExpr> dst_path_exprs = null;

	/**
	 * Instance of XPathPredicateExpr.
	 *
	 * @param src_comp XPath component of predicate
	 * @param src_path_expr source path expression of predicate
	 * @param union_id union id
	 */
	public XPathPredicateExpr(XPathComp src_comp, XPathExpr src_path_expr, int union_id) {

		this.src_comp = src_comp;
		this.src_path_expr = new XPathExpr(src_path_expr); // new instance that prevents shallow copy

		if (union_id >= 0) {

			dst_path_exprs = new ArrayList<XPathExpr>();
			dst_path_exprs.add(new XPathExpr(src_path_expr)); // new instance that prevents shallow copy

		}

	}

	/**
	 * Replace destination path expressions.
	 *
	 * @param path_exprs new path expressions
	 */
	public void replaceDstPathExprs(List<XPathExpr> path_exprs) {

		dst_path_exprs = new ArrayList<XPathExpr>(path_exprs);

	}

	/**
	 * Return equality of predicate.
	 *
	 * @param predicate compared predicate expression
	 * @return boolean whether the predicate expression matches or not
	 */
	public boolean equals(XPathPredicateExpr predicate) {

		if (!src_path_expr.equals(predicate.src_path_expr))
			return false;

		if (dst_path_exprs.size() != predicate.dst_path_exprs.size())
			return false;

		boolean matched = true;

		for (XPathExpr path_expr : dst_path_exprs) {

			if (!(matched = predicate.dst_path_exprs.stream().anyMatch(_path_expr -> _path_expr.equals(path_expr))))
				break;

		}

		return matched;
	}

}
