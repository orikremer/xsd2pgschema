/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2019 Masashi Yokochi

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
import java.util.HashMap;
import java.util.List;

/**
 * XPath query.
 *
 * @author yokochi
 */
public class XPathQuery implements Serializable {

	/** The default serial version ID. */
	private static final long serialVersionUID = 1L;

	/** The XPath query. */
	private String xpath_query = "";

	/** The XPath variables (concatenated). */
	private String variables = "";

	/** The path string. */
	public String path_string = null;

	/** The SQL string. */
	public String sql_string = null;

	/** Instance of XPath expressions. */
	public List<XPathExpr> path_exprs = null;

	/**
	 * Instance of XPathQuery.
	 *
	 * @param xpath_query XPath query
	 * @param variables XPath variable reference
	 */
	public XPathQuery(String xpath_query, HashMap<String, String> variables) {

		this.xpath_query = xpath_query;
		this.variables = concatVars(variables);

	}

	/**
	 * Concatenate variables.
	 *
	 * @param variables XPath variable reference
	 * @return String concatenated XPath variables
	 */
	private String concatVars(HashMap<String, String> variables) {

		if (variables.size() == 0)
			return "";

		StringBuilder sb = new StringBuilder();

		try {

			variables.entrySet().forEach(arg -> sb.append(arg.getKey() + arg.getValue()));

			return sb.toString();

		} finally {
			sb.setLength(0);
		}

	}

	/**
	 * Return equality of XPath query.
	 *
	 * @param xpq XPath query
	 * @return boolean whether the XPath query matches
	 */
	public boolean equals(XPathQuery xpq) {
		return this.xpath_query.equals(xpq.xpath_query) && this.variables.equals(xpq.variables);
	}

	/**
	 * Complete XPath expressions.
	 *
	 * @param xpath_comp_list serialized XPath parse tree
	 * @return XPathQuery result of the XPath query
	 */
	public XPathQuery completeXPathExprs(XPathCompList xpath_comp_list) {

		StringBuilder sb = new StringBuilder();

		xpath_comp_list.showPathExprs(sb);

		path_string = sb.toString();

		sb.setLength(0);

		xpath_comp_list.showSqlExpr(sb);

		sql_string = sb.toString();

		sb.setLength(0);

		path_exprs = xpath_comp_list.path_exprs;

		return this;
	}

}
