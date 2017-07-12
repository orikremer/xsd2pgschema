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
 * XPath component.
 *
 * @author yokochi
 */
public class XPathComp {

	/** The expression id. */
	public int expr_id = -1;

	/** The step id. */
	public int step_id = -1;

	/** The XPath parse tree. */
	public ParseTree tree = null;

	/**
	 * Instance of XPathComp.
	 *
	 * @param expr_id expression id
	 * @param step_id step id
	 * @param tree XPath parse tree
	 */
	public XPathComp(int expr_id, int step_id, ParseTree tree) {

		this.expr_id = expr_id;
		this.step_id = step_id;
		this.tree = tree;

	}

}
